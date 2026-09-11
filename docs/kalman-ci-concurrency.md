# Kalman-driven `NGIT_CI_MAX_CONCURRENT_JOBS` on DQ05

Task: `t_77a0993c` · worker: `worker-base` · date: 2026-09-11
Operator authorisation (2026-09-11): *"How about we let the resource pressure
kalman filter control this variable the same way that it controls the number of
dispatches that are possible?"*

**Status: implemented, installed, enabled, and observed making real decisions on
the live system.** End state on DQ05: `NGIT_CI_MAX_CONCURRENT_JOBS=2`, both
compose containers `Up`, coordinator logging `max_concurrent_jobs=2`.

Where it lives:

| item | value |
|------|-------|
| repo | `felixfelix-bot/gh-ngit-ci-bridge` (existing repo — no new repo) |
| branch | `worker-base/T_77A0993C` on GitHub, pushed to **ngit** `main` (`relay.ngit.dev`), head `e037279` + doc commit |
| controller | `ci_concurrency_controller.py` (I/O) + `ci_concurrency.py` (pure logic) |
| installed at | `/home/c03rad0r/repos/gh-ngit-ci-bridge/` (fast-forwarded to the pushed commit, so the systemd unit runs exactly the pushed code) |
| timer | `kalman-ci-concurrency.timer` — **enabled + active**, every 5 min, user systemd, `SuccessExitStatus=0` |
| decision log | `~/.local/state/gh-ngit-ci-bridge/ci-concurrency.log` (one JSON line per tick) |
| evidence captured | `/home/c03rad0r/worktrees/t_77a0993c/live-evidence.txt`, `/home/c03rad0r/worktrees/t_77a0993c/live-dryrun.log` |

---

## 1. The mechanism reused (no second predictor)

The dispatch pipeline already runs a Kalman resource-pressure filter and turns it
into "how many dispatches are possible". That decision is made in
`~/.hermes/hermes-agent/gateway/kanban_watchers.py::_compute_dispatch_headroom()`
and persisted to `~/.hermes/bot/dispatch_headroom.json`:

```
_compute_dispatch_headroom(static_cap)
  ├─ 6-state resource Kalman   (multi_resource_kalman.py: memory / cpu / swap /
  │                             disk / tokens / workers) -> *early warning*:
  │                             a dimension predicted to breach within 30 min
  │                             has its headroom cut to 0.5
  ├─ current raw resource sample (zai_usage.db resource_metrics:
  │                             cpu_load_1m, memory_used_percent,
  │                             swap_used_percent, disk_used_percent)
  │                             -> breach = 0.0, within 10% = 0.5, else 1.0
  ├─ LLM price/quota gate      (zai proxy /v1/dispatch_gate) -> 0.0 | 1.0
  └─ fold: min(headroom) -> target_workers = 0 if any dimension critical,
           else max(1, round(static_cap * min_headroom))
```

That `min(per_dimension)` fold *is* the resource-pressure signal, and
`target_workers` *is* "the number of dispatches that are possible right now"
(the dispatcher compares against it and the lifecycle governor reads the same
file). The controller re-uses **exactly this** — same file, same fold, same
`min()` — and re-scales it onto the CI coordinator's own `[1..3]` range. It runs
no filter of its own.

Candidates that were read, considered and **rejected**, with the observation
that rejected them:

| candidate | why not |
|-----------|---------|
| `~/.hermes/state/adaptive-dispatch.state` (`SMOOTHED_POOL`/`MAX_CONCURRENT`, the older manager daemon) | still the same quantity, but **stale**: last written 2026-09-10 21:22 IST — `hermes-dispatch.service` is inactive. Kept as source #2 for if/when that daemon runs again. |
| `~/.hermes/bot/pool_kalman.json` (`{"x":[pool,velocity]}`) | **wrong quantity**: a *worker-pool count* filter, not resource pressure. Observed live decaying `1.99977 → 4.5e-10` while the box was idle and healthy; under this mapping that reads as maximum pressure and would pin concurrency at 1 forever. Excluded deliberately (documented in the code). |
| `~/.hermes/bot/adaptive_max_in_progress.json` (assigner mirror) | same quantity, but stale (2026-09-10) **and not valid JSON** (`"updated_at": 2026-09-10T20:00:03Z` unquoted) → treated as unusable, fail-closed. Kept as source #3. |
| `~/.hermes/profiles/manager/scripts/compute_max_workers.py` (the daemon's own capacity function) | usable read-only, but it clamps its own answer **up** to `SAFE_MAX_WORKERS=2` (`Phase 1.1 cap: clamped 1 to SAFE_MAX_WORKERS=2`), so it returns 2 under any pressure and carries almost no signal. Available via `--readonly-probe`, **off by default**. |
| `~/.hermes/bot/kalman_price_state.json`, `kalman_mape_log.jsonl`, `kalman_price_tuning.json` | LLM *price/burn* Kalman, not resource pressure. Not used. |

Because which instance is live depends on which component is running, the
controller reads a **priority list and takes the freshest usable one**
(`dispatch_headroom.json`, `adaptive-dispatch.state`,
`adaptive_max_in_progress.json`), logs every candidate and its age with each
decision, and refuses to act if nothing is fresh (default max age 30 min).

## 2. Mapping

`f = min(per_dimension)` (Kalman-informed headroom, 0 = critical, 1 = free),
folded exactly like the dispatcher:

| `f` | meaning | `NGIT_CI_MAX_CONCURRENT_JOBS` |
|-----|---------|-------------------------------|
| `f >= 0.75` | every resource has headroom | **3** |
| `0.25 <= f < 0.75` | a dimension throttled (or the Kalman predicts a breach) | **2** |
| `f < 0.25`, incl. any dimension at `0.0` | a resource is critical | **1** |

`limit = clamp(round_half_up(1 + f * (3 - 1)), 1, 3)`, then clamped down by the
published `target_workers` (`min(limit, max(1, target_workers))`), then by the
DQ05 memory safety floor. Hard cap **3** (recorded decision: 3 x 4 GB job
ceilings vs 10.9 GB RAM — ceilings, not reservations). Floor **1**: the
dispatcher may legitimately stop dispatching (`target_workers=0`), but the CI
coordinator always keeps one slot.

Signal scale: the host's own pool ceiling is `2` (`V5_SAFE_MAX_WORKERS`), which
is why the *fallback* path (bare pool capacity, sources #2/#3) normalises as
`(pool - 1)/(ceiling - 1)`; the primary path needs no ceiling because the
dispatcher publishes a 0..1 fraction. `--signal-ceiling` overrides.

## 3. Safety around the recreate

Changing concurrency means editing `.env` and restarting the coordinator. Two
independent mechanisms keep that from killing work.

**(a) The restart is a drain, not a kill.** The coordinator handles SIGTERM as a
graceful shutdown: it freezes intake, lets every *running* job finish
(`run_job_executor` joins its running tasks before returning the frozen,
unstarted ones — `src/queue.rs`), checkpoints the unstarted jobs to
`/data/queued-jobs.json`, and only then exits. Its compose service declares
`stop_grace_period: 33m` (the 30 min job timeout + the 2 min Blossom budget +
overhead), and `docker compose` honours that on a config-change recreate —
*measured* on DQ05 with a probe container whose TERM handler takes 25 s:
`docker compose up -d` (config change) took **26 s** and the old container
logged `GOT_SIGTERM` → `DRAIN_DONE`; `docker stop -t 1980` likewise waited out
the whole handler. So the controller does not depend on that choice: it applies
through the **explicit drain order** (`docker compose stop -t <stop-grace>
coordinator`, then `up -d --force-recreate coordinator`: the container is already
stopped, so that is a removal + creation, and it cannot silently reuse the
stopped container with the old environment), and reads the stopped container's
log back to record whether a `graceful drain` line was actually seen
(`apply.graceful_drain_observed`, `apply.stop_took_s`).

**(b) The restart is fenced.** The coordinator also starts jobs autonomously, so
a job can begin between the controller's last sample and the SIGTERM. The drain
then waits for that job (the change lands late; nothing is killed), but the
controller will not restart the coordinator while work is plausible at all.
It applies only when the coordinator is **provably idle across a settle window**
(`--settle-s`, default 90 s, `KALMAN_CI_SETTLE_S`): a second sample after the
window, compared against the first, and a third sample immediately before the
recreate. Any new trigger/repo-event/queue log line, change in the live `act-*`
container set, or change in the queue checkpoint aborts the tick. Idleness
itself is proved from two independent sources:

1. **Coordinator log** — every `Starting queued CI job trigger_event=X` must be
   paired with a `Completed embedded CI for trigger trigger_event=X`; an
   unmatched start, or a fresh (<30 s) `Enqueued` with no completion, is busy.
2. **dind job containers** — `act-*` containers are listed and inspected *inside
   the dind daemon* (host `docker ps` cannot see them: the coordinator runs
   `act` against `tcp://dind:2375`). A running container younger than
   `--max-job-age-s` (default 2100 s = the coordinator's own
   `job_timeout_secs=1800` + margin) is a **live job → defer**; older is a
   **provable orphan** (production has one: `act-Test-and-Build-... Up 2 hours`
   long after its run finished) and is dismissed as stale, so the controller
   cannot deadlock on it.

3. **Queue checkpoint** — `/data/queued-jobs.json` lists jobs frozen by an
   earlier drain. A checkpoint with jobs that is still inside
   `NGIT_CI_QUEUE_RECOVERY_WINDOW_SECS` (default 600 s) means unstarted work
   exists → busy. An older checkpoint is *discarded by ngit-ci on start*, so it
   is ignored (and reported), never allowed to block a change forever.

**Pitfall found live (fixed):** the coordinator's tracing output is coloured
*even when its stdout is a pipe*, so a lifecycle line arrives as
`Enqueued CI job \x1b[3mtrigger_event\x1b[0m\x1b[2m=\x1b[0m<id> …` and a plain
pattern like `Enqueued CI job trigger_event=` never matches the raw bytes. Before
the fix, every lifecycle counter was silently `0` in production: the log-based
idle proof (proof 1 above, and the "new trigger/repo-event/queue log line"
channel of the fence) was dead, and a real job running during a live tick was
caught **only** by the dind container channel (proof 2). The parser now strips
ANSI SGR escapes before matching, and the real captured bytes are regression
tests (`test_ansi_coloured_production_lines_are_parsed`,
`test_the_coloured_line_really_would_not_match_unstripped`).

And then:

* **mutual exclusion** — an exclusive `flock`
  (`~/.local/state/gh-ngit-ci-bridge/ci-concurrency.lock`) serialises the timer
  against manual ticks; the loser exits `3` and logs `lock_held`;
* the fence is **re-sampled immediately before the recreate** (probe → decide →
  settle → re-probe → re-probe → apply), so a job that appeared at any point
  aborts the apply;
* an orphan `act-*` container is **reported** (`fence.orphans`, with its age) and
  does not block; anything that might be live does;
* `--force-recreate` still exists to skip the guard (dangerous, not used). Even
  then the apply goes through the drain, so it does not kill a running job;
* failures to prove idle, or to verify the applied value **inside the running
  container**, fail **closed** (`defer`), never open.

No live reload exists to avoid the restart altogether: `NGIT_CI_MAX_CONCURRENT_JOBS`
is parsed once at startup (`src/config.rs`, `Config::parse`) and becomes a fixed
`Arc<Semaphore>` permit count in `run_job_executor` (`src/queue.rs`); the binary
installs handlers for `SIGTERM`/`SIGINT` only (`src/main.rs`), there is no admin
socket or HTTP endpoint, the only Nostr control kinds are 9843 Service Request /
9844 Service Stop (no config payload), and every coordinator option is exported
as `reload_behavior: "restart_required"` (`src/docs_export.rs`) — only secret
*sources* are marked live. So the restart is unavoidable, and the job is to make
it a drain instead of a kill.

Idempotence: same value as `.env` ⇒ `noop`, and **no container is recreated**.
`.env` is backed up (`.env.bak.<UTCts>`, mode 600 preserved) before every write.

## 4. Tests (raw output)

```
$ pytest -q tests/            # whole repo
........................................................................ [ 72%]
............................                                             [100%]
100 passed in 0.23s

$ pytest tests/test_ci_concurrency.py -v --no-header
test_low_pressure_maps_to_the_hard_cap PASSED
test_high_pressure_maps_to_one PASSED
test_moderate_pressure_maps_to_two PASSED
test_mapping_is_monotone_over_the_smoothed_pool[2.0-3] PASSED
test_mapping_is_monotone_over_the_smoothed_pool[1.75-3] PASSED
test_mapping_is_monotone_over_the_smoothed_pool[1.74-2] PASSED
test_mapping_is_monotone_over_the_smoothed_pool[1.25-2] PASSED
test_mapping_is_monotone_over_the_smoothed_pool[1.24-1] PASSED
test_mapping_is_monotone_over_the_smoothed_pool[1.0-1] PASSED
test_raw_capacity_clamps_a_smoothed_high PASSED
test_limit_never_leaves_the_legal_band PASSED
test_single_level_signal_ceiling_is_safe PASSED
test_velocity_term_is_off_by_default_and_opt_in PASSED
test_dq05_memory_floor_pins_to_one PASSED
test_override_pins_the_value_and_is_clamped_to_the_cap PASSED
test_override_file_and_env_are_read PASSED
test_unchanged_means_noop_no_recreate PASSED
test_busy_coordinator_defers_the_change PASSED
test_unknown_idle_state_defers PASSED
test_dry_run_reports_the_change_without_authorising_it PASSED
test_no_previous_value_defers PASSED
test_idle_when_every_started_job_completed PASSED
test_a_stale_act_container_older_than_the_last_completion_is_not_a_job PASSED
test_idle_is_false_while_a_job_is_in_flight PASSED
test_idle_is_false_for_a_freshly_started_act_container PASSED
test_a_running_act_container_inside_the_job_timeout_counts_as_busy PASSED
test_an_orphan_act_container_past_the_job_timeout_is_ignored PASSED
test_idle_is_false_for_an_enqueue_that_has_not_completed_yet PASSED
test_idle_is_false_when_the_log_is_unusable PASSED
test_env_patch_is_idempotent_and_preserves_other_lines PASSED
test_env_patch_appends_when_the_key_is_missing PASSED
test_parse_kv_state_reads_the_kalman_state_file PASSED
test_read_signal_from_state_file_and_fallback PASSED
test_main_applies_only_when_idle_and_changed PASSED
test_main_never_recreates_when_unchanged PASSED
test_main_defers_while_a_job_runs PASSED
test_main_dry_run_logs_but_does_not_apply PASSED
test_main_respects_the_override_file PASSED
test_main_refuses_a_stale_signal PASSED
test_main_defers_when_dq05_is_unreachable PASSED
test_log_record_carries_exactly_the_expected_fields PASSED
test_read_signal_auto_prefers_the_freshest_instance PASSED
test_parse_source_file_reads_an_empty_or_foreign_file_as_unusable PASSED
test_main_uses_the_freshest_signal_source_when_none_is_pinned PASSED
test_capacity_probe_reads_the_daemon_function_read_only PASSED
test_capacity_probe_handles_failure PASSED
test_main_falls_back_to_the_probe_when_no_state_is_fresh PASSED
test_main_refuses_when_no_published_state_is_fresh PASSED
test_dispatcher_headroom_is_folded_like_the_dispatcher_does PASSED
test_target_workers_clamps_the_mapping_when_it_disagrees PASSED
test_dimension_subset_can_ignore_a_dimension_ci_does_not_consume PASSED
test_dispatcher_headroom_json_is_parsed PASSED
test_probe_parsing_handles_the_real_two_container_layout PASSED
test_probe_script_redirects_stdin_for_every_compose_exec PASSED
test_no_key_material_is_read_by_the_module PASSED
============================== 61 passed in 0.10s ==============================
```

The required cases map to: low pressure → higher limit
(`test_low_pressure_maps_to_the_hard_cap`, `..._moderate_pressure_maps_to_two`);
high pressure → 1 (`test_high_pressure_maps_to_one`,
`test_dispatcher_headroom_is_folded_like_the_dispatcher_does`); override
respected (`test_override_pins_the_value_and_is_clamped_to_the_cap`,
`test_override_file_and_env_are_read`, `test_main_respects_the_override_file`);
unchanged → no recreate (`test_unchanged_means_noop_no_recreate`,
`test_main_never_recreates_when_unchanged`, which asserts `apply_limit` is never
called).

## 5. Live decision, dry-run first

```
$ python3 ci_concurrency_controller.py --dry-run
[NOOP] kalman pool smoothed=0.0 raw=0.0 ceiling=2.0 headroom=0.00 -> limit 1 -> 1
  signal source: /home/c03rad0r/.hermes/bot/dispatch_headroom.json (tick age 46.0s)
  signal headroom: min=0.0 capacity(target_workers)=0.0
  signal dimensions: {'cpu_load': 0.5, 'memory_pct': 0.0, 'swap_used_pct': 1.0,
                      'disk_used_pct': 0.5, 'llm': 1.0}
  reason: unchanged: computed limit 1 == live value 1
  signal: chose freshest: /home/c03rad0r/.hermes/bot/dispatch_headroom.json
  idle: True (0/0 jobs completed, no in-flight trigger, no live act container)
```

At that moment `memory_pct=0.0` (host `memory_used_percent=89% >= 85`) → `f=0.0`
→ limit 1 → nothing to do. Twenty minutes later the same signal read
`memory_pct=0.5` (→ `f=0.5`) and the first real tick **applied** it.

## 6. Real ticks — decisions and actions

Installed + enabled, then the timer itself fired (19:51:42 IST = 14:21:42Z) and
made the first real change. Log (`ci-concurrency.log`), one line per tick:

```
2026-09-11T14:21:43Z APPLY   limit 1 -> 2 | headroom 0.5 | act_seen [] | idle True | set NGIT_CI_MAX_CONCURRENT_JOBS 1 -> 2
2026-09-11T14:23:44Z NOOP    limit 2 -> 2 | headroom 0.5 | idle True | unchanged: computed limit 2 == live value 2
2026-09-11T14:26:11Z DEFER   limit 2 -> 3 | headroom 0.5 | idle False | coordinator not provably idle (act job container(s) present (act-defer-probe-28834 age=0s, act-Test-and-Build-... age=8338s))
2026-09-11T14:26:13Z APPLY   limit 2 -> 3 | headroom 0.5 | idle True  | set NGIT_CI_MAX_CONCURRENT_JOBS 2 -> 3
2026-09-11T14:26:31Z APPLY   limit 3 -> 2 | headroom 0.5 | idle True  | set NGIT_CI_MAX_CONCURRENT_JOBS 3 -> 2
```

The full record of the timer's own first apply (signal, computed vs previous
limit, action, reason, idle proof, apply result):

```json
{"ts": "2026-09-11T14:21:43Z", "action": "apply", "computed_limit": 2, "previous_limit": 1,
 "reason": "set NGIT_CI_MAX_CONCURRENT_JOBS 1 -> 2", "idle": true,
 "idle_detail": "0/0 jobs completed, no in-flight trigger, no live act container",
 "signal": {"source": "/home/c03rad0r/.hermes/bot/dispatch_headroom.json", "tick_age_s": 29.0,
   "headroom": 0.5, "capacity": 2.0,
   "per_dimension": {"cpu_load": 0.5, "disk_used_pct": 0.5, "llm": 1.0, "memory_pct": 0.5, "swap_used_pct": 1.0}},
 "apply": {"ssh_ok": true, "backup": "BACKUP=.env.bak.20260911-142145",
   "env_now": "ENV_NOW=NGIT_CI_MAX_CONCURRENT_JOBS=2",
   "ps_after": {"ngit-ci-deploy-coordinator-1": "running", "ngit-ci-deploy-dind-1": "running"}}}
```

Health after the recreate (exactly the requirement: **both** containers Up, and
the new coordinator came back configuring the new value):

```
$ ssh dq05 'cd ~/ngit-ci-deploy && grep MAX_CONCURRENT .env; docker compose ps --format "{{.Name}}|{{.State}}"
NGIT_CI_MAX_CONCURRENT_JOBS=2
ngit-ci-deploy-coordinator-1|running|Up Less than a second
ngit-ci-deploy-dind-1|running|Up 2 days

coordinator-1 | 2026-09-11T14:26:34.798414Z INFO ngit_ci: Starting ngit-ci coordinator v0.1.1
coordinator-1 | 2026-09-11T14:26:34.798440Z INFO ngit_ci: Configured runner ... max_concurrent_jobs=2 max_queued_jobs=64 job_timeout_secs=1800
```

Apply output as produced by the controller on DQ05 (`.env` backup, recreated and
healthy):

```
ENV_NOW=NGIT_CI_MAX_CONCURRENT_JOBS=2
BACKUP=.env.bak.20260911-142633
 Container ngit-ci-deploy-dind-1  Running
 Container ngit-ci-deploy-coordinator-1  Recreate
 Container ngit-ci-deploy-coordinator-1  Recreated
 Container ngit-ci-deploy-coordinator-1  Starting
 Container ngit-ci-deploy-coordinator-1  Started
PS_AFTER:
ngit-ci-deploy-coordinator-1|running|Up Less than a second
ngit-ci-deploy-dind-1|running|Up 2 days
```

A real CI job did run on a recreated coordinator (proof the recreate is
non-destructive of the pipeline): the container created at 14:21:46 logged
`Starting queued CI job` and `Completed embedded CI ... conclusion="success"`
1/1 before the next change.

**It did not fire mid-run.** The `14:26:11Z DEFER` is that proof, taken against
the live daemon: with a job container present (`act-defer-probe-28834`, a real
container started in the live dind daemon via `docker run … sleep 300`; the run
itself is synthetic, the container and the detection are real) the controller
refused to recreate and left the value at 2; the recreate happened only 2
seconds later, after the container was removed. The parallel entry for the
`14:26:13Z` and `14:26:31Z` applies shows the 2.3-hour-old production orphan
`act-Test-and-Build-...` present and correctly dismissed as stale (age 8338 s >
2100 s job timeout) instead of deadlocking.

### Override and dry-run

* dry-run: `make concurrency-dry` — decided + logged, applied nothing (section 5).
* override: `10:24:08Z APPLY limit 2 -> 3` was driven by `--override 3`, i.e. the
  documented manual pin, because the live signal was stable at 2 at that moment.
  It exists to keep the value under operator control; the JSON record carries
  `"override": 3` and the note `override pins the limit to 3`.
* the last tick (14:26:31Z, **no override**) is a pure signal-driven apply:
  `headroom 0.5, capacity 2 → limit 3 → 2`.

### 6b. Unattended operation and end-to-end pipeline check

After the manual validation, the timer ran on its own (no hand-run tick) and the
ngit push of this work triggered a real CI run through the whole pipeline:

```
$ journalctl --user -u kalman-ci-concurrency.service | tail
Sep 11 19:53:44 Starting kalman-ci-concurrency.service ...
Sep 11 19:53:45 Finished kalman-ci-concurrency.service ...
Sep 11 19:58:45 Starting kalman-ci-concurrency.service ...
Sep 11 19:58:45 Finished kalman-ci-concurrency.service ...

2026-09-11T14:28:45Z NOOP 2 -> 2 | headroom 0.5 | idle True | unchanged: computed limit 2 == live value 2

$ ssh dq05 'docker compose logs --tail=300 coordinator | grep -E "Starting queued|Completed embedded|max_concurrent"'
ngit_ci: Configured runner ... max_concurrent_jobs=2 max_queued_jobs=64 job_timeout_secs=1800
ngit_ci::queue: Starting queued CI job trigger_event=6986a335... workflow=.ngit/act/workflows/bridge-smoke.yml queue_wait_ms=0 running_jobs=1
ngit_ci: Completed embedded CI ... conclusion="success" exit_code=Some(0)
```

So: the coordinator recreations did not break the CI pipeline (a job triggered by
the ngit mirror push ran to `conclusion="success"` afterwards), and the timer
keeps it converged without intervention.

## 7. Install status

```
$ ./install-ci-concurrency.sh
== installing units from /home/c03rad0r/repos/gh-ngit-ci-bridge
== reload + enable
Created symlink '/home/c03rad0r/.config/systemd/user/timers.target.wants/kalman-ci-concurrency.timer' → '.../kalman-ci-concurrency.timer'
NEXT                            LEFT       LAST                              PASSED           UNIT
Fri 2026-09-11 19:58:44 IST     4min 47s   Fri 2026-09-11 19:51:42 IST       2min 14s ago     kalman-ci-concurrency.timer

$ systemctl --user is-enabled kalman-ci-concurrency.timer   -> enabled
$ systemctl --user is-active  kalman-ci-concurrency.timer   -> active
```

Interval: **5 minutes**, justified in the unit file (`systemd/kalman-ci-concurrency.timer`):
the signal moves on a minutes timescale (the 6-state resource Kalman warns ~30
min ahead of a breach); every change recreates the coordinator, so a wide calm
interval is preferable; and it matches the gh-ngit-ci-bridge timer so both
halves of the pipeline tick together.

## 8. What could go wrong

1. **Recreation racing a job start.** Mitigated, not eliminated. The controller
   probes → decides → **re-probes → applies** (a job appearing in the gap aborts
   the apply), and treats `<30 s`-old enqueues and any `act-*` container inside
   the job timeout as busy. But the window between the final probe and
   `docker compose up -d` (`probe ≈ 10-20 s`: two SSH round trips, the coordinator
   log fetch and the dind exec calls) is not atomic. Worst case a job starts in
   that gap and is killed. Residual exposure: the gap plus the ~2 s job duration
   on this coordinator. A real fix would be a lock the coordinator respects
   (there is none today) or `docker compose stop`-then-patch; both are bigger
   changes than this task warrants.
2. **An idle check that silently does nothing.** *This actually happened* during
   validation: `docker compose exec -T dind …` inside a script fed to
   `ssh dq05 bash -s` consumes the *script's* stdin, so the ACT/QUEUE sections
   never ran and the dind half of the idle proof was a no-op while the log half
   still said "idle". Two applies (14:21:43Z, 14:24:12Z) were made with that
   half-check missing — safe only because the *log* half also showed no job
   (0/0) and no job had run since 13:53. Fixed with `< /dev/null` on every
   `compose exec`, plus a regression test
   (`test_probe_script_redirects_stdin_for_every_compose_exec`) and the bug is
   documented in the probe. **Any future edit to `PROBE_SCRIPT` must keep the
   redirects and must not silently drop a section.**
3. **A log window that is too small.** The lifecycle lines are sparse compared
   with relay/watchdog chatter; an 800-line tail missed the last job entirely.
   Now 3000 lines, and the record carries `coordinator_log_lines` /
   `coordinator_log_truncated` so a shrunken window is visible. The dind check
   is the backstop — provided (2) holds.
4. **Orphan act containers.** act left `act-Test-and-Build-…` running for hours
   after its run. Treating any running `act-*` as a job would deadlock the
   controller; treating none as a job would kill CI runs. Hence the
   timeout-based proof (age > coordinator `job_timeout_secs` ⇒ orphan). If
   `NGIT_CI_ACT_CONTAINER_OPTIONS`/job timeouts are ever raised, `--max-job-age-s`
   must be raised with them, or a live job could be mistaken for an orphan.
5. **The signal is the *dispatch host's* pressure, not DQ05's.** By design (the
   operator asked for the same signal), but it means DQ05's CI concurrency can
   be held at 1 (or raised to 3) for reasons that have nothing to do with DQ05 —
   e.g. the *LLM quota* dimension, which CI jobs do not consume at all, or the
   dispatch box's disk. Today the box runs near-full by design
   (`memory_used_percent` 89%, the governor's own note), so `f` is often 0.5 or
   0.0 and the value sits at 1-2. Escape hatches: `--dimensions
   cpu_load,memory_pct,swap_used_pct` (drop `llm`/`disk`), the override file, or
   `--max-limit`. A separate safety net does watch DQ05 itself: a hard floor on
   DQ05's `MemAvailable` (`--dq05-mem-floor-mb`, default 1500 MB) pins the limit
   to 1 regardless of the signal.
6. **Signal staleness / disappearance.** If the dispatcher stops (gateway down),
   `dispatch_headroom.json` stops updating. Then: another fresh source is tried;
   if none is fresh the tick exits 1 without touching anything (fail-closed) and
   `.env` keeps its last value. If *all* published sources vanish for a long
   time, concurrency simply stops adapting — silent because the timer's failure
   is only in the journal. `--readonly-probe` exists but is off by default
   because that probe clamps itself to `SAFE_MAX_WORKERS=2` and would pin the
   limit at 3.
7. **Flapping.** Each change costs a coordinator restart (~1 s downtime + job
   kill risk), so oscillation is the expensive failure mode. The mapping is
   level-only (Kalman velocity term off by default: `--velocity-weight`), the
   fold is coarse (3 buckets), and the 5-minute period damps it — but a signal
   sitting exactly on a bucket edge (e.g. `f=0.25` / `f=0.75`) can flip every
   tick. `min()` over five dimensions makes that less likely, and hysteresis
   could be added if it is observed.
8. **Two writers of `.env`.** The controller is the only writer now, but the
   operator (or another script) editing `~/ngit-ci-deploy/.env` between ticks
   will simply be overwritten on the next tick — the override file is the
   supported way to pin a value. Backups (`.env.bak.<ts>`) accumulate; ~3 per
   hour if the signal flaps.
9. **Secrets.** The controller never reads key material: it reads the Kalman
   state, a dotenv with non-secret knobs, coordinator logs and container state.
   A test asserts the log record has exactly the expected fields and contains no
   `nsec` / `private_key` / `bunker` / `token` / `password` strings.

## 9. Not done / follow-ups

* No PR was opened: `felixfelix-bot` cannot push to the upstream orgs, and this
  is the fork/bridge repo anyway — the branch is pushed (**GitHub**
  `worker-base/T_77A0993C`, **ngit** `main` at `relay.ngit.dev`) and the local
  clone used by systemd is fast-forwarded to it. Merge the branch to `main` on
  GitHub to make it the published default.
* The idle guard is still time-based, not lock-based (see 8.1).
* The manager-side dispatch daemon (`hermes-dispatch.service`) is inactive; if it
  is restarted, `adaptive-dispatch.state` becomes the freshest source and starts
  driving the same decision (by design — same quantity, same fold).
