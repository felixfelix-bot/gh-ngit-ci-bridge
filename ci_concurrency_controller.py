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
    DEFAULT_SIGNAL_CEILING,
    Decision,
    LogRecord,
    Signal,
    classify_idle,
    decide,
    parse_env_value,
    parse_kv_state,
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
DEFAULT_SSH_TARGET = "dq05"
DEFAULT_DEPLOY_DIR = "~/ngit-ci-deploy"
ENV_KEY = "NGIT_CI_MAX_CONCURRENT_JOBS"

MARK = {ACTION_APPLY: "APPLY", ACTION_NOOP: "NOOP", ACTION_DEFER: "DEFER", ACTION_DRY_RUN: "DRY-RUN"}

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
docker compose up -d coordinator 2>&1 | tail -6
echo "PS_AFTER:"
docker compose ps --format '{{.Name}}|{{.State}}|{{.Status}}' 2>&1
"""


def apply_limit(ssh_target: str, limit: int, timeout: int = 240) -> tuple[bool, str]:
    script = APPLY_SCRIPT.replace("__LIMIT__", str(int(limit)))
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
    args = build_argparser().parse_args(argv)
    log_file = Path(os.path.expanduser(args.log_file))
    now = time.time()

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
        idle, idle_detail, idle_facts = classify_idle(
            dq.log_text,
            dq.act_containers,
            now=now,
            max_job_age_s=args.max_job_age_s,
            log_readable=True,
        )
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
    if decision.action == ACTION_APPLY:
        # Race guard: the job could have started between probe and apply.
        recheck = probe_dq05(args.ssh_target)
        if not recheck.reachable:
            apply_detail = f"re-check failed ({recheck.error}); not applied"
            record.action = ACTION_DEFER
            record.reason = f"{record.reason}; re-check before apply failed: {recheck.error}"
        else:
            idle2, idle2_detail, _ = classify_idle(
                recheck.log_text,
                recheck.act_containers,
                now=time.time(),
                max_job_age_s=args.max_job_age_s,
            )
            if not idle2 and not args.force_recreate:
                apply_detail = f"aborted at re-check: {idle2_detail}"
                record.action = ACTION_DEFER
                record.reason = f"job appeared between probe and apply ({idle2_detail})"
            else:
                ok, out = apply_limit(args.ssh_target, decision.limit)
                apply_detail = out
                ps_after = {}
                for line in out.splitlines():
                    if "|" in line:
                        parts = line.split("|")
                        if len(parts) >= 2:
                            ps_after[parts[0].strip()] = parts[1].strip()
                record.extra["apply"] = {
                    "ssh_ok": ok,
                    "env_now": next(
                        (l for l in out.splitlines() if l.startswith("ENV_NOW=")), ""
                    ),
                    "backup": next((l for l in out.splitlines() if l.startswith("BACKUP=")), ""),
                    "ps_after": ps_after,
                }
                if not ok or ps_after.get("ngit-ci-deploy-coordinator-1") != "running":
                    record.action = ACTION_DEFER
                    record.reason = f"{record.reason}; apply verification FAILED"

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
        if apply_detail:
            print("  apply output:")
            for line in apply_detail.splitlines():
                print(f"    {line}")
        print(f"  log: {log_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
