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
docker compose logs --tail=800 coordinator 2>&1
echo "===ACT==="
names=$(docker compose exec -T dind docker ps -a --format '{{.Names}}' 2>/dev/null | grep '^act-' || true)
for n in $names; do
  docker compose exec -T dind docker inspect "$n" \
    --format '{{.Name}}|{{.State.Status}}|{{.State.StartedAt}}' 2>/dev/null
done
echo "===QUEUE==="
docker compose exec -T coordinator sh -c 'cat /data/queued-jobs.json 2>/dev/null | head -c 4000' 2>/dev/null
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
        text = path.read_text(errors="replace")
        kv = parse_kv_state(text)
        if "SMOOTHED_POOL" in kv:
            try:
                smoothed = float(kv["SMOOTHED_POOL"])
            except ValueError as exc:
                raise RuntimeError(f"{path}: SMOOTHED_POOL not a number ({exc})") from exc
            raw = kv.get("POOL_RAW")
            try:
                raw_f = float(raw) if raw is not None else smoothed
            except ValueError:
                raw_f = smoothed
            vel = kv.get("VELOCITY")
            try:
                vel_f = float(vel) if vel is not None else 0.0
            except ValueError:
                vel_f = 0.0
            mc = kv.get("MAX_CONCURRENT")
            try:
                mc_f = float(mc) if mc is not None else None
            except ValueError:
                mc_f = None
            ts = kv.get("TS")
            try:
                ts_f = float(ts) if ts is not None else None
            except ValueError:
                ts_f = None
            return Signal(
                pool_smoothed=smoothed,
                pool_raw=raw_f,
                ceiling=ceiling,
                velocity=vel_f,
                max_concurrent=mc_f,
                source=str(path),
                ts=ts_f,
                age_s=None if ts_f is None else now - ts_f,
            )
    fb = Path(os.path.expanduser(str(fallback_file)))
    if fb.exists():
        try:
            data = json.loads(fb.read_text() or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{fb}: not valid JSON ({exc})") from exc
        if "max_in_progress" in data:
            mc = float(data["max_in_progress"])
            raw = float(data.get("raw", mc))
            return Signal(
                pool_smoothed=mc,
                pool_raw=raw,
                ceiling=ceiling,
                max_concurrent=mc,
                source=str(fb),
            )
    raise RuntimeError(
        f"no Kalman signal available: neither {path} nor {fb} carries a usable pool value"
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
    ap.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    ap.add_argument("--fallback-state-file", default=DEFAULT_FALLBACK_STATE)
    ap.add_argument("--override-file", default=DEFAULT_OVERRIDE_FILE)
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

    try:
        signal = read_signal(
            args.state_file, args.fallback_state_file, args.signal_ceiling, now=now
        )
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if signal.age_s is not None and signal.age_s > args.max_signal_age_s:
        print(
            f"ERROR: Kalman state is stale ({signal.age_s / 60:.1f} min old > "
            f"{args.max_signal_age_s / 60:.1f} min) — refusing to act",
            file=sys.stderr,
        )
        return 1

    pinned, override_note = read_override(args.override_file, os.environ.get("KALMAN_CI_CONCURRENCY_PIN"))
    if args.override is not None:
        pinned, override_note = args.override, f"cli override ({args.override})"

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
        )
        decision = dataclasses.replace(
            decision, reason=f"DQ05 unreachable: {dq.error}"
        )
    else:
        idle, idle_detail, idle_facts = classify_idle(
            dq.log_text, dq.act_containers, now=now, log_readable=True
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
        )

    extra: dict = {}
    if dq.reachable:
        extra["dq05"] = {
            "compose_ps": dq.compose_ps.strip().replace("\n", "; "),
            "queued_jobs_present": bool(dq.queue_text),
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
                recheck.log_text, recheck.act_containers, now=time.time()
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
        print(f"  reason: {record.reason}")
        for note in decision.notes:
            print(f"  note: {note}")
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
