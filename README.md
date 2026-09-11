# gh-ngit-ci-bridge

Makes **new commits from a set of watched GitHub identities** trigger [ngit-ci]
(the self-hosted Nostr CI coordinator), which by design can only see Nostr.

```
GitHub push by a watched identity
        │  (gh api: commits + push events)
        ▼
   bridge.py  ── decide(commit, repo, state) ──┐
        │                                      │  skip + reason
        │                                      └──────────────► log only
        ▼  trigger
   push ref to the repo's ngit mirror  ──► git-remote-nostr publishes kind 30618
        │                                    (repo state — what the coordinator consumes)
        └─ fallback: kind-9840 Manual Trigger with `w` = workflow path + content sha256
```

## Why

ngit-ci reacts to NIP-34 repository-state events (30618), Service Requests
(9843) and Manual Triggers (9840) on relays. A GitHub push emits none of those,
so commits authored or pushed by `felixfelix-bot`, `c03rad0r`, `Amperstrand`,
`Origami74`, `Franchovy`, `maximotodev` or `hkarani` are invisible to the
coordinator. The bridge publishes the ref to the repo's ngit mirror, which emits
the 30618 the coordinator already listens for.

## Public-only gate (security invariant)

Nostr relays are public and permanent: a mirror push (kind-30618) and a repo
announcement (kind-30617) cannot be revoked. **Only GitHub repositories whose
visibility is exactly `public` may be mirrored or announced.** The gate is
implemented once in `public_only.py` and is **fail-closed**: `private`,
`internal`, an unknown/empty value, NOT-FOUND, a timeout and any API error all
deny. See [`SECURITY.md`](SECURITY.md).

It is enforced at config load (a `repo -> visibility -> verdict` table is
printed and non-public entries are reported and skipped, never silently removed
from `config.json`) and again immediately before every clone, ngit push and
kind-9840 publish. A repo that fails the gate is a *permanent skip for that
tick* — logged with its observed visibility, never recorded as a retryable
failure, and re-evaluated on the next tick.

## Kalman-driven coordinator concurrency (`NGIT_CI_MAX_CONCURRENT_JOBS`)

`ci_concurrency_controller.py` (unit-tested logic in `ci_concurrency.py`) sets
the ngit-ci coordinator's concurrency from **the same resource-pressure Kalman
signal the dispatcher already uses to decide how many dispatches are possible**.
It is not a second predictor: it reads the folded headroom the dispatcher
publishes and re-scales it onto the coordinator's `[1..3]` range.

```
dispatcher (gateway/kanban_watchers.py)
  _compute_dispatch_headroom()
    6-state resource Kalman (multi_resource_kalman) early-warning  ─┐
    raw resource pressure (RAM / load / swap / disk)               ├─> min() -> target_workers
    LLM quota gate (/v1/dispatch_gate)                            ─┘
  persisted to ~/.hermes/bot/dispatch_headroom.json
        │
        ▼
ci_concurrency_controller.py  ── f = min(per_dimension) ──> limit = clamp(1 + f*(3-1), 1, 3)
        │                       (capacity clamp: min(limit, target_workers); never < 1)
        ▼  only when the coordinator is provably idle
   ~/ngit-ci-deploy/.env  NGIT_CI_MAX_CONCURRENT_JOBS=N  +  docker compose up -d coordinator
```

| signal | meaning | limit |
|--------|---------|-------|
| `min(per_dimension) >= 0.75` | every resource has headroom | 3 |
| `0.25 <= min < 0.75` | a dimension is throttled (or the Kalman predicts a breach) | 2 |
| `min < 0.25`, incl. any dimension at `0.0` | a resource is critical | 1 |

The floor is 1: the dispatcher may stop dispatching entirely (`target_workers=0`),
but the CI coordinator always keeps one slot. The hard cap is 3 (a recorded
decision: 3 x 4 GB job ceilings against 10.9 GB of RAM).

Behaviour:

* **idempotent** — same limit as the live `.env` value means *no recreate* (the
  coordinator container is only touched when the number actually changes);
* **single-flight** — an exclusive `flock` on
  `~/.local/state/gh-ngit-ci-bridge/ci-concurrency.lock` means the 5-minute
  timer and a manual tick can never overlap; the loser exits `3` (logged as
  `lock_held`) instead of racing a second recreate;
* **quiet-window fenced** — a single idle sample is not a fence. A change is
  applied only when the coordinator is provably idle at *both* ends of a settle
  window (`--settle-s`, default 90 s): no new trigger/repo-event/queue log line,
  no change in the live `act-*` container set, and no queued job in the queue
  checkpoint — then sampled once more immediately before the recreate. Any
  change aborts the tick (`defer`, with the reason logged). An `act-*`
  container older than the coordinator's own 30-minute job timeout is an
  **orphan**: reported under `fence.orphans`, never blocking and never mistaken
  for a live job;
* **drain-safe apply** — the value is applied with
  `docker compose stop -t <stop-grace> coordinator` (default 1980 s = the
  coordinator's own `stop_grace_period`) *before*
  `docker compose up -d --force-recreate`. The coordinator treats SIGTERM as a
  graceful drain (it finishes in-flight jobs and checkpoints the unstarted
  ones), so a job that starts inside the fence's last gap is drained rather than
  killed; the forced recreate then guarantees the new value lands in the new
  process. The tick records `stop_took_s` and whether a `graceful drain` line
  was observed;
* **verified** — after the recreate the controller reads
  `NGIT_CI_MAX_CONCURRENT_JOBS` *from inside the running container*
  (`docker compose exec … printenv`), not just from `.env`; a mismatch is
  logged as `apply verification FAILED` and the tick defers;
* **colour-proof parsing** — the coordinator's tracing output is coloured even
  into a pipe (`Enqueued CI job \x1b[3mtrigger_event\x1b[0m\x1b[2m=\x1b[0m…`), so
  the parser strips ANSI escapes before matching. Without that every lifecycle
  counter silently reads `0` in production and the log-based idle proof is dead
  — observed live: a real running job was caught only by the dind container
  channel;
* **override** — `echo 2 > ~/.local/state/gh-ngit-ci-bridge/ci-concurrency.override`
  (or `KALMAN_CI_CONCURRENCY_PIN=2`, or `--override 2`) pins the value; the pin
  is respected and logged;
* **dry-run** — `--dry-run` decides and logs, applies nothing;
* **auditable** — every tick appends one JSON line to
  `~/.local/state/gh-ngit-ci-bridge/ci-concurrency.log` with the signal value,
  the computed limit, the previous limit, the action and the reason. No key
  material is read or written by this program.

```bash
make concurrency-test      # unit tests for the mapping / idle detection
make concurrency-dry       # decide against the live signal, apply nothing
make concurrency-once      # one real tick
make concurrency-install   # install + enable the 5-minute systemd user timer
```

Useful flags: `--max-limit`, `--signal-ceiling`, `--dimensions cpu_load,memory_pct`
(CI jobs consume neither LLM quota nor the dispatch host's disk; folding those
dimensions in is faithful to the dispatcher but can hold CI concurrency down for
reasons CI does not cause), `--dq05-mem-floor-mb`, `--force-recreate` (skip the
idle check — dangerous), `--json`.

## Files

| file | purpose |
|------|---------|
| `bridge.py` | the poller: discovery, decision, trigger, lock, logging |
| `decision.py` | pure decision logic (unit-tested, no I/O) |
| `public_only.py` | fail-closed public-only gate (`gh api ... --jq .visibility`) |
| `nostr_event.py` | signs an event with the key in `key_file` — key never enters argv |
| `config.json` | committed config: identities, orgs/repos, relays, intervals. No secrets |
| `audit.py` | reports, per watched repo, its GitHub visibility and whether a CI run is possible |
| `SECURITY.md` | the invariant: public GitHub repos only, fail-closed |
| `tests/test_decision.py` | unit tests for the decision matrix |
| `tests/test_public_only.py` | unit tests for the public-only gate |
| `ci_concurrency.py` | pure mapping: Kalman headroom -> concurrency limit, idle classification, dotenv patching |
| `ci_concurrency_controller.py` | the controller: read signal, compare, apply only when idle, log every decision |
| `systemd/gh-ngit-ci-bridge.*` | bridge user timer + service (every 5 minutes) |
| `systemd/kalman-ci-concurrency.*` | concurrency user timer + service (every 5 minutes) |
| `install.sh` | installs and enables the bridge timer |
| `install-ci-concurrency.sh` | installs and enables the concurrency timer |
| `.ngit/act/workflows/bridge-smoke.yml` | the repo's own ngit-CI workflow |

## Hard-won lessons about git-remote-nostr (read before touching the push path)

1. **It reads `nostr.nsec`/`nostr.npub` from the repository's own `.git/config`** and ignores
   `GIT_CONFIG_*` environment injection. Injecting via the environment makes it sign as the
   machine-global ngit account, which is rejected as a non-maintainer.
2. **It exits 0 when it rejects the push** (`your nostr account … isn't listed as a
   maintainer`) and prints `Error: could not update remote_ref locally` on pushes that
   succeeded. Never trust its exit status — verify with
   `git ls-remote https://relay.ngit.dev/<npub>/<repo>.git refs/heads/<branch>`.
3. The source side of a push refspec must be a **ref name**; pushing a bare SHA fails with
   `cannot find ref <sha>`. The bridge stages `refs/bridge-publish/<branch>` first.
4. `ngit init` **replaces `origin` with a `nostr://` remote**. Keep a separate `origin` for
   GitHub and a `ngit` remote for Nostr.

## Install

```bash
./install.sh                       # copies units, enables gh-ngit-ci-bridge.timer
systemctl --user start gh-ngit-ci-bridge.service        # run one tick now
journalctl --user -u gh-ngit-ci-bridge.service -n 50    # tick output
tail -f ~/.local/state/gh-ngit-ci-bridge/bridge.log     # bridge log
```

The timer runs the service every 5 minutes. The service type is `oneshot`; exit
code `3` means another tick held the lock (normal, not an error).

## Usage

```bash
python3 bridge.py --dry-run                      # classify only, publish nothing, keep state
python3 bridge.py --dry-run --repo OpenTollGate/tollgate-module-basic-go \
                  --sha <sha>                    # classify one specific commit
python3 bridge.py --refresh-mirrors              # re-read kind-30617 announcements now
python3 bridge.py --repo owner/name              # real run, one repo only
python3 audit.py                                 # visibility + triggerability, per repo
make test                                        # unit tests (decision + public-only gate)
```

Exit codes: `0` ok, `2` runtime error, `3` another tick holds the lock,
`4` config error.

## State (outside the repo)

`~/.local/state/gh-ngit-ci-bridge/state.json` holds last-seen head SHAs, the set
of handled commit SHAs (dedupe: a SHA never triggers twice), the kind-30617
mirror map, the org repo cache and counters. It is never committed.

Clones used to push are created lazily on first trigger under
`~/.local/share/gh-ngit-ci-bridge/cache/` (mode 700).

## Trigger paths

1. **mirror_push** — `git push nostr://<npub>/relay.ngit.dev/<repo-id> <sha>:refs/heads/<branch>`.
   The nostr signing key is handed to git through `GIT_CONFIG_*` environment
   variables, never through argv. After the push the bridge re-reads the newest
   kind-30618 for the repo and logs its event id plus whether the new commit is
   actually in the published state.
2. **manual_9840** — NIP-C1 Manual Trigger (`p` = coordinator only, `a` = 30617
   coordinate, `c` = commit, `w` = workflow path + sha256 of the file at that
   commit, `r` = ref). Used when a repo-state change would be a no-op (mirror
   already carries the sha) or when `trigger_mode` is `manual`.

Both paths log the resulting Nostr event id.

## Config notes

* `orgs` are expanded at runtime through `gh api orgs/<org>/repos`, so repos
  migrated to ngit later are picked up automatically — the repo list is not
  hard-coded.
* A GitHub repo maps to an ngit repo-id by the `d` tag of its kind-30617
  announcement, normally the repository name; `ngit_repo_id` overrides it.
* `baseline_on_first_run: true` records the current head without triggering, so
  enabling the bridge does not retro-fire CI for old commits.
* `trigger_mode`: `mirror` (default) or `manual`.

## Authorisation

The coordinator runs with `request-required` policy: a push-driven run needs a
standing Service Request (kind 9843) for that repo. Manual Triggers bypass the
gate by design. If a repo has no standing SR the coordinator logs
`Skipping push trigger until an authorized Service Request is observed` — the
bridge still publishes the state event, so the run happens once an SR exists.

[ngit-ci]: https://relay.ngit.dev/npub15qydau2hjma6ngxkl2cyar74wzyjshvl65za5k5rl69264ar2exs5cyejr/relay.ngit.dev/ngit-ci.git
