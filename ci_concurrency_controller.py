#!/usr/bin/env python3
"""ci_concurrency_controller.py — drive NGIT_CI_MAX_CONCURRENT_JOBS on DQ05
from the dispatch daemon's Kalman resource-pressure signal.

One tick:
  1. read the Kalman signal the existing dispatcher already publishes
     (``~/.hermes/state/adaptive-dispatch.state``; ``compute_max_workers.py``
     -> pool Kalman -> ``MAX_CONCURRENT``) — no second predictor;
  2. map it onto an integer concurrency in [1..3] (see ``ci_concurrency.py``);
  3. read the live ``NGIT_CI_MAX_CONCURRENT_JOBS`` from DQ05's
     ``~/ngit-ci-deploy/.env``;
  4. if it differs, apply it **only when the coordinator is provably idle**
     (changing concurrency recreates the coordinator container, which kills
     any in-flight job), otherwise defer to the next tick;
  5. log the decision (signal, computed limit, previous limit, action, reason).

Dry-run: ``--dry-run`` prints the decision and writes it to the log, applying
nothing. Override: pin the limit in ``ci-concurrency.override`` (one integer)
or pass ``--override N``; a pinned value is respected and still logged.

Never logs key material: nothing in this program reads a key file.
"""

from __future__ import annotations

import argparse
import dataclasses
import fcntl
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ci_concurrency import (  # noqa: E402
    ACTION_APPLY,
    ACTION_DEFER,
    ACTION_DRY_RUN,
    ACTION_NOOP,
    DEFAULT_DQ05_MEM_FLOOR_MB,
    DEFAULT_MAX_LIMIT,
    DEFAULT_MAX_SIGNAL_AGE_S,
    DEFAULT_RECOVERY_WINDOW_S,
    DEFAULT_SETTLE_S,
    DEFAULT_SIGNAL_CEILING,
    DEFAULT_STOP_GRACE_S,
    Decision,
    LogRecord,
    Signal,
    classify_idle,
    decide,
    fingerprint_jobs,
    new_activity,
    orphan_policy_text,
    parse_env_value,
    parse_kv_state,
    parse_queued_jobs,
    patch_env_value,
)

DEFAULT_STATE_FILE = "~/.hermes/state/adaptive-dispatch.state"
DEFAULT_FALLBACK_STATE = "~/.hermes/bot/adaptive_max_in_progress.json"
#: Instances of the SAME resource-pressure Kalman capacity decision, freshest
#: wins:
#:   * ``dispatch_headroom.json`` — written by the dispatcher's
#:     ``_compute_dispatch_headroom()`` (gateway/kanban_watchers.py), which folds
#:     the 6-state resource Kalman (multi_resource_kalman: memory/cpu/swap/disk/
#:     tokens/workers) early-warning together with the current raw resource
#:     pressure and the LLM quota gate into ``target_workers`` — literally "how
#:     many dispatches are possible right now";
#:   * ``adaptive-dispatch.state`` — the dispatch daemon's Kalman-smoothed pool
#:     capacity (the same quantity, published while that daemon runs);
#:   * ``adaptive_max_in_progress.json`` — the kanban assigner's mirror of it.
#:
#: Deliberately NOT in this list: ~/.hermes/bot/pool_kalman.json. That file is a
#: *worker-pool count* filter (its measurement is a live worker count), not
#: resource pressure — it was observed decaying to ~0.0 on an idle, healthy
#: host, which this mapping would read as maximum pressure and would pin
#: concurrency at 1 forever.
DEFAULT_SIGNAL_SOURCES = (
    "~/.hermes/bot/dispatch_headroom.json",
    "~/.hermes/state/adaptive-dispatch.state",
    "~/.hermes/bot/adaptive_max_in_progress.json",
)
#: The daemon's own capacity function. Available as an explicit opt-in
#: (``--readonly-probe``) but NOT used by default: ``compute_max_workers.py``
#: clamps its own answer up to ``SAFE_MAX_WORKERS=2``, so it returns 2 under
#: any pressure and therefore carries almost no signal.
DEFAULT_CAPACITY_PROBE = "~/.hermes/profiles/manager/scripts/compute_max_workers.py"
DEFAULT_OVERRIDE_FILE = "~/.local/state/gh-ngit-ci-bridge/ci-concurrency.override"
DEFAULT_LOG_FILE = "~/.local/state/gh-ngit-ci-bridge/ci-concurrency.log"
DEFAULT_LOCK_FILE = "~/.local/state/gh-ngit-ci-bridge/ci-concurrency.lock"
DEFAULT_SSH_TARGET = "dq05"
DEFAULT_DEPLOY_DIR = "~/ngit-ci-deploy"
ENV_KEY = "NGIT_CI_MAX_CONCURRENT_JOBS"

#: Exit code for "another controller tick holds the lock". Same convention (and
#: same meaning) as gh-ngit-ci-bridge's own EXIT_LOCKED, so both units can list
#: it in SuccessExitStatus.
EXIT_LOCKED = 3
ACTION_LOCK_HELD = "lock_held"

MARK = {
    ACTION_APPLY: "APPLY",
    ACTION_NOOP: "NOOP",
    ACTION_DEFER: "DEFER",
    ACTION_DRY_RUN: "DRY-RUN",
    ACTION_LOCK_HELD: "LOCK-HELD",
}

#: Indirection so tests can drive the settle window without sleeping through it.
SETTLE_SLEEP = time.sleep


class TickLock:
    """Exclusive single-flight lock over the controller's state directory.

    The 5-minute timer and a manual ``make concurrency-once`` must never
    overlap: two ticks could each observe "idle", each decide to apply a
    *different* limit, and each recreate the coordinator — the second one
    killing whatever the first had just started. ``flock`` is released by the
    kernel when the process dies, so a crashed tick cannot wedge the controller.
    """

    def __init__(self, path: str | Path):
        self.path = Path(os.path.expanduser(str(path)))
        self.handle = None

    def __enter__(self) -> "TickLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("w")
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            handle.close()
            raise BlockingIOError(f"another tick holds {self.path}")
        self.handle = handle
        return self

    def __exit__(self, *exc) -> None:
        if self.handle is not None:
            try:
                fcntl.flock(self.handle, fcntl.LOCK_UN)
            finally:
                self.handle.close()
                self.handle = None

# ------------------------------------------------------------------ ssh probe

PROBE_SCRIPT = r"""
cd "$HOME/ngit-ci-deploy" 2>/dev/null || { echo "===MISSING==="; exit 0; }
echo "===ENV==="
cat .env 2>/dev/null
echo "===MEM==="
awk '/MemAvailable:/{print $2}' /proc/meminfo 2>/dev/null
echo "===PS==="
docker compose ps --format '{{.Name}}|{{.State}}|{{.Status}}' 2>&1
echo "===LOGS==="
# 3000 lines reaches hours back on this coordinator; the job-lifecycle lines are
# sparse relative to relay/watchdog chatter, so a small window could miss the
# last job entirely.
docker compose logs --tail=3000 coordinator 2>&1
echo "===ACT==="
# NOTE: every `docker compose exec` needs `< /dev/null`. This script is fed to
# `ssh ... bash -s` on STDIN, and without the redirect the exec'd process
# consumes the rest of the script - the ACT/QUEUE sections silently never ran
# and the idle check was left with only the log-based proof (observed live).
names=$(docker compose exec -T dind docker ps -a --format '{{.Names}}' < /dev/null 2>/dev/null | grep '^act-' || true)
for n in $names; do
  docker compose exec -T dind docker inspect "$n" \
    --format '{{.Name}}|{{.State.Status}}|{{.State.StartedAt}}' < /dev/null 2>/dev/null
done
echo "===QUEUE==="
docker compose exec -T coordinator sh -c 'cat /data/queued-jobs.json 2>/dev/null | head -c 4000' < /dev/null 2>/dev/null
echo "===END==="
"""


def _iso_to_epoch(text: str) -> float | None:
    import datetime as _dt

    text = text.strip()
    if not text:
        return None
    try:
        return _dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _split_sections(out: str) -> dict[str, str]:
    sections: dict[str, list[str]] = {}
    current = "__head__"
    sections[current] = []
    for line in out.splitlines():
        if line.startswith("===") and line.endswith("==="):
            current = line.strip("=")
            sections.setdefault(current, [])
            continue
        sections[current].append(line)
    return {k: "\n".join(v) for k, v in sections.items()}


@dataclass
class Dq05State:
    reachable: bool = False
    error: str = ""
    env_text: str = ""
    env_value: int | None = None
    mem_available_mb: int | None = None
    compose_ps: str = ""
    log_text: str = ""
    act_containers: list[dict] = field(default_factory=list)
    queue_text: str = ""
    log_lines: int = 0
    log_truncated: bool = False

    def parse_ps(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for line in self.compose_ps.splitlines():
            parts = line.split("|")
            if len(parts) >= 2:
                out[parts[0].strip()] = parts[1].strip()
        return out


def probe_dq05(ssh_target: str, timeout: int = 90) -> Dq05State:
    state = Dq05State()
    try:
        proc = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", ssh_target, "bash", "-s"],
            input=PROBE_SCRIPT,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        state.error = f"ssh probe failed: {exc}"
        return state
    if proc.returncode != 0:
        state.error = f"ssh probe exit {proc.returncode}: {proc.stderr.strip()[:200]}"
        return state
    sec = _split_sections(proc.stdout)
    if "MISSING" in proc.stdout:
        state.error = "~/ngit-ci-deploy missing on DQ05"
        return state
    state.reachable = True
    state.env_text = sec.get("ENV", "")
    raw_env = parse_env_value(state.env_text, ENV_KEY)
    try:
        state.env_value = int(str(raw_env).strip()) if raw_env is not None else None
    except ValueError:
        state.env_value = None
    mem_raw = (sec.get("MEM", "") or "").strip().splitlines()
    if mem_raw:
        try:
            state.mem_available_mb = int(int(mem_raw[0].strip()) / 1024)
        except ValueError:
            state.mem_available_mb = None
    state.compose_ps = sec.get("PS", "")
    state.log_text = sec.get("LOGS", "")
    state.log_lines = len(state.log_text.splitlines())
    state.log_truncated = state.log_lines >= 3000
    state.queue_text = (sec.get("QUEUE", "") or "").strip()
    for line in (sec.get("ACT", "") or "").splitlines():
        parts = line.split("|")
        if len(parts) >= 3:
            state.act_containers.append(
                {
                    "name": parts[0].lstrip("/").strip(),
                    "running": parts[1].strip().lower() == "running",
                    "started": _iso_to_epoch(parts[2]),
                }
            )
    return state


APPLY_SCRIPT = r"""
set -eu
cd "$HOME/ngit-ci-deploy"
ts=$(date -u +%Y%m%d-%H%M%S)
cp -p .env ".env.bak.$ts"
sed -i "s/^NGIT_CI_MAX_CONCURRENT_JOBS=.*/NGIT_CI_MAX_CONCURRENT_JOBS=__LIMIT__/" .env
chmod 600 .env
echo "ENV_NOW=$(grep '^NGIT_CI_MAX_CONCURRENT_JOBS=' .env)"
echo "BACKUP=.env.bak.$ts"
# Drain-safe restart. `docker compose stop` sends SIGTERM and WAITS, and the
# coordinator treats SIGTERM as a graceful drain: it freezes intake, finishes
# every in-flight job, checkpoints unstarted ones, and only then exits. The
# timeout below matches the coordinator's own `stop_grace_period` (33m), so a
# job that starts between the controller's last sample and this stop is drained
# to completion instead of being killed by the recreate (which is what
# `docker compose up -d` alone does, at whatever stop timeout it picks).
s=$(date +%s)
docker compose stop -t __GRACE__ coordinator 2>&1 | tail -3
echo "STOP_TOOK_S=$(( $(date +%s) - s ))"
# The stopped container still holds the log of the shutdown we just triggered;
# read it back so the decision log can carry proof that the drain happened.
echo "DRAIN_TAIL:"
docker compose logs --tail=8 coordinator 2>&1 | sed 's/^/  /'
# --force-recreate: the container is already stopped (the drain finished), so
# this is a removal + creation, not a stop. It guarantees the new .env value
# lands in the new process instead of risking compose reusing the stopped
# container with the old environment.
docker compose up -d --force-recreate coordinator 2>&1 | tail -6
# The value that matters is the one inside the running process, not the one in
# the file: verify both.
echo "RUNTIME_VALUE=$(docker compose exec -T coordinator printenv NGIT_CI_MAX_CONCURRENT_JOBS < /dev/null 2>/dev/null || echo UNKNOWN)"
echo "PS_AFTER:"
docker compose ps --format '{{.Name}}|{{.State}}|{{.Status}}' 2>&1
"""


def apply_limit(
    ssh_target: str,
    limit: int,
    timeout: int | None = None,
    *,
    stop_grace_s: float = DEFAULT_STOP_GRACE_S,
) -> tuple[bool, str]:
    """Apply ``limit`` and recreate the coordinator without killing a job.

    The ssh timeout is derived from the stop grace period: a drain that waits
    for a full 30-minute job is a success, not a timeout.
    """
    grace = int(max(1.0, float(stop_grace_s)))
    if timeout is None:
        timeout = int(grace + 180)
    script = APPLY_SCRIPT.replace("__LIMIT__", str(int(limit))).replace("__GRACE__", str(grace))
    try:
        proc = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", ssh_target, "bash", "-s"],
            input=script,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return False, f"ssh apply failed: {exc}"
    out = (proc.stdout or "") + (proc.stderr or "")
    return proc.returncode == 0, out.strip()


# ------------------------------------------------------------------- signal


def read_signal(
    state_file: str | Path,
    fallback_file: str | Path,
    ceiling: float,
    *,
    now: float | None = None,
) -> Signal:
    """Read the reuse-the-Kalman signal. Raises RuntimeError when unusable."""
    now = time.time() if now is None else now
    path = Path(os.path.expanduser(str(state_file)))
    if path.exists():
        sig = parse_source_file(path, ceiling, now=now)
        if sig is not None:
            return sig
    fb = Path(os.path.expanduser(str(fallback_file)))
    if fb.exists():
        sig = parse_source_file(fb, ceiling, now=now)
        if sig is not None:
            return sig
    raise RuntimeError(
        f"no Kalman signal available: neither {path} nor {fb} carries a usable pool value"
    )


def parse_source_file(path: Path, ceiling: float, *, now: float | None = None) -> Signal | None:
    """Parse one candidate signal file, whatever shape that instance uses.

    Shapes seen in production (all outputs of the same existing filter):
      * ``adaptive-dispatch.state``  KEY=VALUE, ``SMOOTHED_POOL``/``POOL_RAW``/``TS``
      * ``pool_kalman.json``         ``{"x": [pool, velocity], "ts": epoch}``
      * ``adaptive_max_in_progress.json`` ``{"max_in_progress": N, "raw": N}``
    """
    now = time.time() if now is None else now
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return None
    if not text.strip():
        return None

    ts: float | None = None
    _ts_explicit = False
    smoothed: float
    raw: float | None = None
    velocity = 0.0
    max_concurrent: float | None = None

    stripped = text.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        if "x" in data and isinstance(data.get("x"), (list, tuple)) and data["x"]:
            try:
                smoothed = float(data["x"][0])
                velocity = float(data["x"][1]) if len(data["x"]) > 1 else 0.0
            except (TypeError, ValueError):
                return None
            raw = float(data.get("raw", smoothed))
            max_concurrent = float(data.get("max_concurrent", round(smoothed)))
        elif "max_in_progress" in data:
            try:
                smoothed = float(data["max_in_progress"])
                raw = float(data.get("raw", smoothed))
            except (TypeError, ValueError):
                return None
            max_concurrent = smoothed
        elif "smoothed_pool" in data:
            try:
                smoothed = float(data["smoothed_pool"])
                raw = float(data.get("raw_pool", smoothed))
            except (TypeError, ValueError):
                return None
            velocity = float(data.get("velocity", 0.0) or 0.0)
            max_concurrent = float(data.get("max_concurrent", round(smoothed)))
        elif "per_dimension" in data or "target_workers" in data:
            # dispatch_headroom.json — the dispatcher's own folded headroom.
            dims = data.get("per_dimension") or {}
            try:
                per_dim = {
                    str(k): float(v)
                    for k, v in dims.items()
                    if isinstance(v, (int, float)) and not isinstance(v, bool)
                }
            except (TypeError, ValueError):
                return None
            capacity = data.get("target_workers")
            try:
                cap_f = float(capacity) if capacity is not None else None
            except (TypeError, ValueError):
                cap_f = None
            if not per_dim and cap_f is None:
                return None
            smoothed = float(cap_f) if cap_f is not None else min(per_dim.values())
            raw = smoothed
            headroom = min(per_dim.values()) if per_dim else None
            return Signal(
                pool_smoothed=smoothed,
                pool_raw=raw,
                ceiling=ceiling,
                per_dimension=per_dim,
                headroom=headroom,
                capacity=cap_f,
                max_concurrent=cap_f,
                source=str(path),
                ts=path.stat().st_mtime,
                age_s=now - path.stat().st_mtime,
                ts_explicit=False,
            )
        else:
            return None
        for key in ("ts", "updated_at", "generated_ts"):
            if data.get(key) is not None:
                try:
                    ts = float(data[key])
                    _ts_explicit = True
                    break
                except (TypeError, ValueError):
                    continue
    else:
        kv = parse_kv_state(text)
        if "SMOOTHED_POOL" not in kv:
            return None
        try:
            smoothed = float(kv["SMOOTHED_POOL"])
        except ValueError:
            return None
        try:
            raw = float(kv["POOL_RAW"]) if kv.get("POOL_RAW") is not None else smoothed
        except ValueError:
            raw = smoothed
        try:
            velocity = float(kv.get("VELOCITY", 0.0) or 0.0)
        except ValueError:
            velocity = 0.0
        try:
            max_concurrent = float(kv["MAX_CONCURRENT"]) if kv.get("MAX_CONCURRENT") is not None else None
        except ValueError:
            max_concurrent = None
        try:
            ts = float(kv["TS"]) if kv.get("TS") is not None else None
            _ts_explicit = ts is not None
        except ValueError:
            ts = None

    if ts is None:
        try:
            ts = path.stat().st_mtime
        except OSError:
            ts = None
    ts_explicit = _ts_explicit

    return Signal(
        pool_smoothed=smoothed,
        pool_raw=raw if raw is not None else smoothed,
        ceiling=ceiling,
        velocity=velocity,
        max_concurrent=max_concurrent,
        source=str(path),
        ts=ts,
        age_s=None if ts is None else now - ts,
        ts_explicit=ts_explicit,
    )


def read_signal_auto(
    sources: "list[str] | tuple[str, ...]",
    ceiling: float,
    *,
    now: float | None = None,
    max_age_s: float | None = None,
) -> tuple[Signal, list[str]]:
    """Pick the FRESHEST usable signal file out of every known instance.

    All candidates are outputs of the same existing resource-pressure Kalman
    filter (the dispatch daemon's pool state, its smoothed capacity state, and
    the assigner's max-in-progress mirror). Which one is live depends on which
    component is running, so freshness decides. Returns ``(signal, notes)``.
    """
    now = time.time() if now is None else now
    notes: list[str] = []
    candidates: list[Signal] = []
    for src in sources:
        path = Path(os.path.expanduser(str(src)))
        if not path.exists():
            notes.append(f"absent: {path}")
            continue
        sig = parse_source_file(path, ceiling, now=now)
        if sig is None:
            notes.append(f"unusable (no pool value): {path}")
            continue
        age = "" if sig.age_s is None else f", age {sig.age_s / 60:.1f} min"
        notes.append(f"candidate: {path} pool={sig.pool_smoothed:.3f}{age}")
        if max_age_s is not None and sig.age_s is not None and sig.age_s > max_age_s:
            continue
        candidates.append(sig)
    if not candidates:
        raise RuntimeError(
            "no fresh Kalman signal among: " + "; ".join(notes or ["(none configured)"])
        )
    # Freshest first (age from the file's own tick timestamp, else its mtime);
    # a source that publishes its own timestamp wins a tie, then configured
    # priority. The point is the *signal*, not the layout: whichever component
    # is actually running right now is the one whose reading is actionable.
    def _rank(s: Signal) -> tuple:
        age = s.age_s if s.age_s is not None else float("inf")
        return (
            round(age, 1),
            0 if s.ts_explicit else 1,
            sources.index(s.source) if s.source in sources else len(sources),
        )

    candidates.sort(key=_rank)
    chosen = candidates[0]
    notes.append(f"chose freshest: {chosen.source}")
    return chosen, notes


def probe_capacity_readonly(
    probe_path: str | Path,
    ceiling: float,
    *,
    now: float | None = None,
    runner=None,
    timeout: int = 30,
) -> tuple[Signal | None, str]:
    """Run the daemon's own capacity function read-only and use its answer.

    Same estimator, same inputs, no dispatch side effects: this is the fallback
    for when the dispatch daemon is stopped (nothing publishes its smoothed
    pool). The probe prints one integer; with the daemon's pool smoother absent
    the daemon itself also falls back to this raw capacity, so the two agree.
    """
    now = time.time() if now is None else now
    path = Path(os.path.expanduser(str(probe_path)))
    if not path.exists():
        return None, f"capacity probe not found: {path}"
    runner = runner or subprocess.run
    try:
        proc = runner(
            [sys.executable, str(path), "--verbose"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return None, f"capacity probe failed: {exc}"
    out = (proc.stdout or "").strip().splitlines()
    value: float | None = None
    for line in reversed(out):
        line = line.strip()
        if line.isdigit():
            value = float(line)
            break
    if value is None:
        return None, f"capacity probe produced no integer (stdout={proc.stdout!r})"
    if value > ceiling:
        value = ceiling
    detail = " | ".join((proc.stderr or "").strip().splitlines())
    note = f"read-only capacity probe {path} -> {value:.0f}"
    if detail:
        note += f" [{detail}]"
    return (
        Signal(
            pool_smoothed=value,
            pool_raw=value,
            ceiling=ceiling,
            velocity=0.0,
            max_concurrent=value,
            source=f"probe:{path}",
            ts=now,
            age_s=0.0,
        ),
        note,
    )


def read_override(path: str | Path, env_value: str | None = None) -> tuple[int | None, str]:
    if env_value not in (None, ""):
        try:
            return int(str(env_value).strip()), f"env override ({env_value})"
        except ValueError:
            return None, f"ignored non-integer env override {env_value!r}"
    p = Path(os.path.expanduser(str(path)))
    if p.exists():
        raw = p.read_text().strip()
        if raw:
            try:
                return int(raw.split()[0]), f"override file {p}"
            except ValueError:
                return None, f"ignored malformed override file {p}"
    return None, ""


# --------------------------------------------------------------------- main


def build_argparser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="decide + log, apply nothing")
    ap.add_argument("--force-recreate", action="store_true", help="skip the idle check (dangerous)")
    ap.add_argument(
        "--settle-s",
        type=float,
        default=float(os.environ.get("KALMAN_CI_SETTLE_S", DEFAULT_SETTLE_S)),
        help=(
            "quiet window (seconds) the coordinator must be observably idle across "
            "before a recreate: two idle samples with no new trigger/repo-event/queue "
            "line, no change in the live act-container set and no queued job in "
            "between (default 90s; 0 disables the wait, not the comparison)"
        ),
    )
    ap.add_argument(
        "--stop-grace-s",
        type=float,
        default=float(os.environ.get("KALMAN_CI_STOP_GRACE_S", DEFAULT_STOP_GRACE_S)),
        help=(
            "docker stop timeout for the drain-safe apply; the coordinator drains "
            "in-flight jobs on SIGTERM (default 1980s = its own stop_grace_period)"
        ),
    )
    ap.add_argument(
        "--recovery-window-s",
        type=float,
        default=float(os.environ.get("KALMAN_CI_RECOVERY_WINDOW_S", DEFAULT_RECOVERY_WINDOW_S)),
        help=(
            "ngit-ci's NGIT_CI_QUEUE_RECOVERY_WINDOW_SECS: a queue checkpoint older "
            "than this is discarded on start and no longer counts as queued work"
        ),
    )
    ap.add_argument("--lock-file", default=DEFAULT_LOCK_FILE)
    ap.add_argument("--override", type=int, default=None, help="pin the limit for this tick")
    ap.add_argument("--state-file", default=None, help="pin one signal file (default: freshest of --signal-sources)")
    ap.add_argument("--fallback-state-file", default=DEFAULT_FALLBACK_STATE)
    ap.add_argument(
        "--signal-sources",
        default=os.environ.get("KALMAN_CI_SIGNAL_SOURCES", ",".join(DEFAULT_SIGNAL_SOURCES)),
        help="comma-separated candidate signal files; the freshest usable one wins",
    )
    ap.add_argument("--override-file", default=DEFAULT_OVERRIDE_FILE)
    ap.add_argument(
        "--capacity-probe",
        default=os.environ.get("KALMAN_CI_CAPACITY_PROBE", DEFAULT_CAPACITY_PROBE),
        help="the daemon's own capacity function, used read-only when no fresh Kalman state exists",
    )
    ap.add_argument(
        "--readonly-probe",
        dest="allow_readonly_probe",
        action="store_true",
        help=(
            "if no published Kalman state is fresh, run compute_max_workers.py read-only "
            "(off by default: it clamps itself to SAFE_MAX_WORKERS and carries little signal)"
        ),
    )
    ap.add_argument(
        "--max-job-age-s",
        type=float,
        default=float(os.environ.get("KALMAN_CI_MAX_JOB_AGE_S", 2100.0)),
        help=(
            "an act job container older than this is an orphan, not a live job "
            "(default 2100s = the coordinator's 1800s job timeout + margin)"
        ),
    )
    ap.add_argument(
        "--dimensions",
        default=os.environ.get("KALMAN_CI_DIMENSIONS", ""),
        help=(
            "comma-separated subset of headroom dimensions to fold (default: all published, "
            "exactly like the dispatcher). e.g. cpu_load,memory_pct,swap_used_pct"
        ),
    )
    ap.add_argument("--log-file", default=DEFAULT_LOG_FILE)
    ap.add_argument("--ssh-target", default=os.environ.get("KALMAN_CI_SSH_TARGET", DEFAULT_SSH_TARGET))
    ap.add_argument("--signal-ceiling", type=float, default=float(os.environ.get("KALMAN_CI_SIGNAL_CEILING", DEFAULT_SIGNAL_CEILING)))
    ap.add_argument("--max-limit", type=int, default=int(os.environ.get("KALMAN_CI_MAX_LIMIT", DEFAULT_MAX_LIMIT)))
    ap.add_argument(
        "--velocity-weight",
        type=float,
        default=float(os.environ.get("KALMAN_CI_VELOCITY_WEIGHT", 0.0)),
        help="weight of the Kalman velocity term (default 0: level-only, flap-resistant)",
    )
    ap.add_argument(
        "--dq05-mem-floor-mb",
        type=int,
        default=int(os.environ.get("KALMAN_CI_DQ05_MEM_FLOOR_MB", DEFAULT_DQ05_MEM_FLOOR_MB)),
    )
    ap.add_argument(
        "--max-signal-age-s",
        type=float,
        default=float(os.environ.get("KALMAN_CI_MAX_SIGNAL_AGE_S", DEFAULT_MAX_SIGNAL_AGE_S)),
        help="refuse to act on a stale Kalman state older than this",
    )
    ap.add_argument("--json", action="store_true", help="print the decision as JSON")
    ap.add_argument("--quiet", action="store_true", help="log only")
    return ap


def write_log(log_file: Path, record: dict) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a") as fh:
        fh.write(json.dumps(record, sort_keys=True) + "\n")


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: single-flight lock, then exactly one tick."""
    args = build_argparser().parse_args(argv)
    log_file = Path(os.path.expanduser(args.log_file))
    now = time.time()

    if args.dry_run:
        # A dry run mutates nothing, so it does not need the lock (and must not
        # be blocked by a real tick sitting in its settle window).
        return _run_tick(args, log_file, now)

    # (a) mutual exclusion. The timer and a manual `make concurrency-once` must
    # never overlap: two ticks could each see "idle", each pick a different
    # limit, and the second recreate could kill what the first just started.
    lock = TickLock(args.lock_file)
    try:
        lock.__enter__()
    except BlockingIOError as exc:
        record = LogRecord(
            ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
            signal={},
            computed_limit=0,
            previous_limit=None,
            action=ACTION_LOCK_HELD,
            reason=f"another tick holds the lock ({exc}); not proceeding",
            idle=None,
            idle_detail="",
            extra={"lock_file": str(Path(os.path.expanduser(args.lock_file)))},
        )
        write_log(log_file, record.as_dict())
        if not args.quiet:
            print(f"[LOCK-HELD] another tick holds the lock; exiting (see {log_file})")
        return EXIT_LOCKED

    try:
        return _run_tick(args, log_file, now)
    finally:
        lock.__exit__(None, None, None)


def _run_tick(args, log_file: Path, now: float) -> int:
    signal_notes: list[str] = []
    try:
        if args.state_file:
            signal = read_signal(
                args.state_file, args.fallback_state_file, args.signal_ceiling, now=now
            )
            signal_notes.append(f"explicit signal file: {args.state_file}")
        else:
            sources = [s.strip() for s in str(args.signal_sources).split(",") if s.strip()]
            try:
                signal, signal_notes = read_signal_auto(
                    sources, args.signal_ceiling, now=now, max_age_s=args.max_signal_age_s
                )
            except RuntimeError as exc:
                if not args.allow_readonly_probe:
                    raise
                signal_notes = [str(exc)]
                signal, probe_note = probe_capacity_readonly(
                    args.capacity_probe, args.signal_ceiling, now=now
                )
                signal_notes.append(probe_note)
                if signal is None:
                    raise RuntimeError(probe_note) from exc
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.state_file and signal.age_s is not None and signal.age_s > args.max_signal_age_s:
        print(
            f"ERROR: Kalman state is stale ({signal.age_s / 60:.1f} min old > "
            f"{args.max_signal_age_s / 60:.1f} min) — refusing to act",
            file=sys.stderr,
        )
        return 1

    pinned, override_note = read_override(args.override_file, os.environ.get("KALMAN_CI_CONCURRENCY_PIN"))
    if args.override is not None:
        pinned, override_note = args.override, f"cli override ({args.override})"

    dims = [d.strip() for d in str(args.dimensions).split(",") if d.strip()] or None

    dq = probe_dq05(args.ssh_target)

    # The quiet-window fence's ledger: every field here is written to the
    # decision log, so a deferred tick says exactly what it saw and why it
    # refused to restart the coordinator.
    fence: dict = {
        "settle_s": max(0.0, float(args.settle_s)),
        "stop_grace_s": float(args.stop_grace_s),
        "recovery_window_s": float(args.recovery_window_s),
        "samples": 0,
        "sample_1": None,
        "sample_2": None,
        "sample_3": None,
        "changed": False,
        "change_reason": "",
        "pre_apply_recheck": {"changed": False, "reason": ""},
        "orphan_policy": orphan_policy_text(args.max_job_age_s),
        "orphans": [],
        "defer_reason": "",
        "bypassed": "",
    }
    act1 = None

    def sample(state):
        """One fence sample: job-activity fingerprint + queue facts + idle verdict."""
        sampled_at = time.time()
        queued, queued_detail = parse_queued_jobs(
            state.queue_text, now=sampled_at, recovery_window_s=args.recovery_window_s
        )
        activity = fingerprint_jobs(
            state.log_text,
            state.act_containers,
            now=sampled_at,
            max_job_age_s=args.max_job_age_s,
            queued_jobs=queued,
            log_lines=state.log_lines,
            log_truncated=state.log_truncated,
        )
        verdict = classify_idle(
            state.log_text,
            state.act_containers,
            now=sampled_at,
            max_job_age_s=args.max_job_age_s,
            log_readable=True,
            queued_jobs=queued,
            queued_detail=queued_detail,
        )
        return activity, verdict

    if not dq.reachable:
        decision = decide(
            signal=signal,
            previous=None,
            idle=None,
            idle_detail=dq.error,
            dry_run=args.dry_run,
            override=pinned,
            max_limit=args.max_limit,
            velocity_weight=args.velocity_weight,
            dq05_avail_mb=None,
            dq05_mem_floor_mb=args.dq05_mem_floor_mb,
            dimensions=dims,
        )
        decision = dataclasses.replace(
            decision, reason=f"DQ05 unreachable: {dq.error}"
        )
    else:
        act1, (idle, idle_detail, _idle_facts) = sample(dq)
        fence["samples"] = 1
        fence["sample_1"] = act1.as_dict()
        fence["orphans"] = act1.as_dict()["orphans"]
        decision = decide(
            signal=signal,
            previous=dq.env_value,
            idle=True if args.force_recreate else idle,
            idle_detail=idle_detail,
            dry_run=args.dry_run,
            override=pinned,
            max_limit=args.max_limit,
            velocity_weight=args.velocity_weight,
            dq05_avail_mb=dq.mem_available_mb,
            dq05_mem_floor_mb=args.dq05_mem_floor_mb,
            dimensions=dims,
        )

    extra: dict = {"signal_notes": signal_notes}
    if dq.reachable:
        extra["dq05"] = {
            "compose_ps": dq.compose_ps.strip().replace("\n", "; "),
            "queued_jobs_present": bool(dq.queue_text),
            "coordinator_log_lines": dq.log_lines,
            "coordinator_log_truncated": dq.log_truncated,
            "act_containers_seen": [
                {"name": c["name"][:60], "running": c["running"]} for c in dq.act_containers
            ],
        }
    if override_note:
        extra["override_note"] = override_note
    extra["fence"] = fence

    record = LogRecord(
        ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        signal=signal.as_dict(),
        computed_limit=decision.limit,
        previous_limit=decision.previous,
        action=decision.action,
        reason=decision.reason,
        idle=decision.idle,
        idle_detail=decision.idle_detail,
        override=decision.override,
        dry_run=args.dry_run,
        extra=extra,
    )

    apply_detail = ""
    abort_reason = ""
    if decision.action == ACTION_APPLY:
        if act1 is None:
            # No usable sample (DQ05 was unreachable): cannot fence, must not act.
            abort_reason = "no fence sample available for this tick"
        elif args.force_recreate:
            fence["bypassed"] = (
                "--force-recreate: the idle check and the quiet-window fence were "
                "skipped (the apply still drains on SIGTERM instead of killing)"
            )
        else:
            # (b) A single idle sample is not a fence. Require the coordinator to
            # be observably idle at BOTH ends of a settle window, with no new
            # trigger/repo-event/queue log line, no change in the live act
            # container set and no queued job in between. Any change aborts this
            # tick: the only thing we lose is five minutes.
            settle_s = max(0.0, float(args.settle_s))
            settle_started = time.time()
            if settle_s > 0:
                SETTLE_SLEEP(settle_s)
            fence["settle_elapsed_s"] = round(time.time() - settle_started, 1)
            dq2 = probe_dq05(args.ssh_target)
            if not dq2.reachable:
                abort_reason = f"re-probe after the settle window failed ({dq2.error})"
            else:
                act2, (idle2, idle2_detail, _) = sample(dq2)
                changed2, why2 = new_activity(act1, act2)
                fence["samples"] = 2
                fence["sample_2"] = act2.as_dict()
                fence["changed"] = changed2
                fence["change_reason"] = why2
                fence["orphans"] = act2.as_dict()["orphans"]
                if changed2:
                    abort_reason = (
                        f"job activity during the {settle_s:.0f}s settle window: {why2}"
                    )
                elif not idle2:
                    abort_reason = (
                        f"coordinator not idle at the end of the settle window ({idle2_detail})"
                    )
                else:
                    # (c) The window proved it *was* quiet, but the coordinator
                    # starts jobs autonomously; sample once more immediately
                    # before the recreate and abort on any change since.
                    dq3 = probe_dq05(args.ssh_target)
                    if not dq3.reachable:
                        abort_reason = f"pre-apply re-check failed ({dq3.error})"
                    else:
                        act3, (idle3, idle3_detail, _) = sample(dq3)
                        changed3, why3 = new_activity(act2, act3)
                        fence["samples"] = 3
                        fence["sample_3"] = act3.as_dict()
                        fence["pre_apply_recheck"] = {"changed": changed3, "reason": why3}
                        fence["orphans"] = act3.as_dict()["orphans"]
                        if changed3:
                            abort_reason = (
                                "job activity appeared between the settle sample and the "
                                f"recreate: {why3}"
                            )
                        elif not idle3:
                            abort_reason = (
                                f"coordinator not idle immediately before the recreate "
                                f"({idle3_detail})"
                            )

        if abort_reason:
            # (f) Fail closed: nothing is applied, the next tick tries again, and
            # the reason is in the decision log.
            fence["defer_reason"] = abort_reason
            record.action = ACTION_DEFER
            record.reason = f"{record.reason}; FENCE HELD: {abort_reason}"
            apply_detail = f"fence held: {abort_reason}"
        else:
            ok, out = apply_limit(
                args.ssh_target, decision.limit, stop_grace_s=args.stop_grace_s
            )
            apply_detail = out
            ps_after = {}
            for line in out.splitlines():
                if "|" in line:
                    parts = line.split("|")
                    if len(parts) >= 2:
                        ps_after[parts[0].strip()] = parts[1].strip()

            def _field(name: str) -> str:
                return next(
                    (l.split("=", 1)[1] for l in out.splitlines() if l.startswith(f"{name}=")),
                    "",
                )

            record.extra["apply"] = {
                "ssh_ok": ok,
                "env_now": next(
                    (l for l in out.splitlines() if l.startswith("ENV_NOW=")), ""
                ),
                "backup": next((l for l in out.splitlines() if l.startswith("BACKUP=")), ""),
                # Verification of the value the *process* sees, not just the file.
                "runtime_value": _field("RUNTIME_VALUE"),
                # How long the drain-safe stop waited: >0s means a real drain.
                "stop_took_s": _field("STOP_TOOK_S"),
                "graceful_drain_observed": "graceful drain" in out.lower(),
                "ps_after": ps_after,
            }
            if not ok:
                record.action = ACTION_DEFER
                record.reason = f"{record.reason}; apply verification FAILED (non-zero exit)"
            elif _field("RUNTIME_VALUE") != str(int(decision.limit)):
                record.action = ACTION_DEFER
                record.reason = (
                    f"{record.reason}; apply verification FAILED: the running coordinator "
                    f"reports {ENV_KEY}={_field('RUNTIME_VALUE') or 'UNKNOWN'}, "
                    f"expected {decision.limit}"
                )
            elif ps_after.get("ngit-ci-deploy-coordinator-1") != "running":
                record.action = ACTION_DEFER
                record.reason = f"{record.reason}; apply verification FAILED"
    elif decision.action == ACTION_NOOP:
        fence["note"] = "no change needed; the quiet-window fence was not run"

    if record.action == ACTION_DEFER and not fence["defer_reason"]:
        fence["defer_reason"] = record.reason

    write_log(log_file, record.as_dict())

    if args.json:
        print(json.dumps(record.as_dict(), indent=2, sort_keys=True))
    elif not args.quiet:
        s = record.signal
        print(
            f"[{MARK.get(record.action, record.action)}] kalman pool "
            f"smoothed={s['smoothed_pool']} raw={s['raw_pool']} ceiling={s['ceiling']} "
            f"headroom={decision.headroom:.2f} -> limit {record.previous_limit} -> {record.computed_limit}"
        )
        print(f"  signal source: {s['source']} (tick age {s['tick_age_s']}s)")
        if s.get("per_dimension"):
            print(f"  signal headroom: min={s['headroom']} capacity(target_workers)={s['capacity']}")
            print(f"  signal dimensions: {s['per_dimension']}")
        print(f"  reason: {record.reason}")
        for note in decision.notes:
            print(f"  note: {note}")
        for note in signal_notes:
            print(f"  signal: {note}")
        if record.idle is not None:
            print(f"  idle: {record.idle} ({record.idle_detail})")
        changed_note = f" ({fence['change_reason']})" if fence["change_reason"] else ""
        print(
            f"  fence: samples={fence['samples']} settle={fence['settle_s']:.0f}s"
            f" elapsed={fence.get('settle_elapsed_s', 0)}s"
            f" changed={fence['changed']}{changed_note}"
        )
        for orphan in fence["orphans"]:
            print(
                f"  fence orphan (reported, not blocking): {orphan['name']} "
                f"age={orphan['age_s']}s"
            )
        if fence["bypassed"]:
            print(f"  fence bypassed: {fence['bypassed']}")
        if fence["defer_reason"]:
            print(f"  fence defer reason: {fence['defer_reason']}")
        if apply_detail:
            print("  apply output:")
            for line in apply_detail.splitlines():
                print(f"    {line}")
        print(f"  log: {log_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
