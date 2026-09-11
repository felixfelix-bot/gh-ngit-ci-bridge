#!/usr/bin/env python3
"""Pure mapping from the dispatch Kalman resource-pressure signal to an
ngit-ci coordinator concurrency limit.

No I/O, no network, no subprocess: every function here is a deterministic
function of its arguments, so the mapping matrix can be unit-tested in
isolation (same convention as ``decision.py``).

The signal we reuse (do NOT invent a second predictor)
------------------------------------------------------
The dispatch daemon (``~/.hermes/profiles/manager/scripts/adaptive-dispatch-daemon.sh``)
already runs a Kalman filter over this machine's resource-pressure capacity:

  ``compute_max_workers.py``  -> raw capacity integer from real signals
                                 (available RAM, load-per-core, swap%, API quota),
                                 hard-capped at the host's dispatch ceiling.
  pool Kalman smoothing       -> ``SMOOTHED_POOL`` (float) = smoothed capacity.
  persisted every tick to ``~/.hermes/state/adaptive-dispatch.state`` as::

      SMOOTHED_POOL=<float>   # Kalman-smoothed capacity (survives restarts)
      POOL_RAW=<int>          # raw capacity this tick (unsmoothed)
      VELOCITY=<float>        # Kalman velocity of the pool
      MAX_CONCURRENT=<int>    # what the daemon actually gates dispatch count on

``MAX_CONCURRENT`` *is* "how many dispatches are possible right now" — the
variable the daemon compares ``RUNNING`` against before it will dispatch. This
module reads that same smoothed capacity and re-scales it onto the CI
coordinator's own [1..MAX_LIMIT] range. One filter, two consumers.

Mapping
-------
The Kalman capacity lives on the host's own scale: 1 = maximum resource
pressure, ``ceiling`` = no measured pressure. We normalise it to a headroom
fraction and rescale onto the CI range::

    f          = clamp01((pool - 1) / (ceiling - 1))     # 0 = pressure, 1 = free
    limit      = clamp1..max(floor(1 + f*(max - 1) + 0.5))

so for ``ceiling=2`` and ``max_limit=3``::

    f < 0.25   -> 1   (high pressure)
    f  0.25-0.75 -> 2 (moderate pressure)
    f >= 0.75  -> 3   (low pressure / free)

The *unsmoothed* capacity is applied as a fail-safe clamp on top (a raw reading
of 1 means the current tick measured real pressure, whatever the smoothed value
still remembers). Two optional, documented guards sit below the cap:
a transient velocity term (off by default — see ``velocity_weight``) and a
hard floor on the *observed* DQ05 memory (a safety clamp, not a predictor).
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Mapping, Sequence

# ------------------------------------------------------------------ vocabulary

ACTION_APPLY = "apply"
ACTION_NOOP = "noop"
ACTION_DEFER = "defer"
ACTION_DRY_RUN = "dry_run"

MIN_LIMIT = 1

#: Host dispatch ceiling of the Kalman pool signal: the pool cannot exceed this
#: (it comes from ``V5_SAFE_MAX_WORKERS``/``MAX_CAP`` in compute_max_workers.py).
DEFAULT_SIGNAL_CEILING = 2

#: Hard cap for ngit-ci concurrency on DQ05. 3 x 4 GB job caps = 12 GB against
#: 10.9 GB RAM: those are *ceilings*, not reservations, and a recorded earlier
#: decision allows at most 3.
DEFAULT_MAX_LIMIT = 3

#: Observed-memory floor on DQ05 (MB of MemAvailable). Below this the limit is
#: pinned to 1 regardless of the signal. A safety clamp on a live measurement,
#: NOT a second predictor.
DEFAULT_DQ05_MEM_FLOOR_MB = 1500

#: How old the persisted Kalman state may be before we refuse to act on it.
DEFAULT_MAX_SIGNAL_AGE_S = 1800


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _round_half_up(value: float) -> int:
    # Deterministic and monotone (python's round() is banker's rounding).
    return int(math.floor(value + 0.5))


# -------------------------------------------------------------------- signal


@dataclass(frozen=True)
class Signal:
    """The reused Kalman resource-pressure signal, as persisted by the daemon."""

    pool_smoothed: float  # SMOOTHED_POOL — the Kalman-smoothed capacity
    pool_raw: float  # POOL_RAW — raw capacity this tick
    ceiling: float = DEFAULT_SIGNAL_CEILING  # host dispatch ceiling of the pool
    velocity: float = 0.0  # POOL_VELOCITY
    max_concurrent: float | None = None  # MAX_CONCURRENT (what gates dispatch)
    source: str = ""  # where the numbers came from (path or "env:...")
    ts: float | None = None  # daemon tick timestamp (epoch seconds)
    age_s: float | None = None  # now - ts, when known
    #: True when the file carried its own timestamp; False when we fell back to
    #: the file's mtime (a source without a tick timestamp is less trustworthy
    #: than one that publishes when it last ran).
    ts_explicit: bool = True
    #: Per-dimension resource headroom as published by the dispatcher's
    #: ``_compute_dispatch_headroom`` fold (Kalman early-warning + raw pressure
    #: + LLM gate). 0.0 = dimension critical, 1.0 = full headroom.
    per_dimension: Mapping[str, float] = field(default_factory=dict)
    #: ``min(per_dimension)`` as published (all dimensions).
    headroom: float | None = None
    #: ``target_workers``-style capacity count, when the source publishes one.
    capacity: float | None = None

    def as_dict(self) -> dict:
        return {
            "smoothed_pool": round(self.pool_smoothed, 4),
            "raw_pool": round(self.pool_raw, 4),
            "ceiling": self.ceiling,
            "velocity": round(self.velocity, 4),
            "max_concurrent": self.max_concurrent,
            "source": self.source,
            "tick_age_s": None if self.age_s is None else round(self.age_s, 1),
            "tick_ts_explicit": self.ts_explicit,
            "headroom": None if self.headroom is None else round(self.headroom, 4),
            "capacity": self.capacity,
            "per_dimension": {k: round(float(v), 4) for k, v in dict(self.per_dimension).items()},
        }


def headroom_fraction(
    pool: float,
    ceiling: float = DEFAULT_SIGNAL_CEILING,
    velocity: float = 0.0,
    velocity_weight: float = 0.0,
) -> float:
    """Normalise a Kalman capacity reading to 0 (pressure) .. 1 (free)."""
    ceiling = float(ceiling)
    effective = float(pool) + float(velocity_weight) * float(velocity)
    if ceiling <= 1.0:
        return 1.0 if effective >= ceiling else 0.0
    return _clamp01((effective - 1.0) / (ceiling - 1.0))


def capacity_to_limit(
    fraction: float,
    min_limit: int = MIN_LIMIT,
    max_limit: int = DEFAULT_MAX_LIMIT,
) -> int:
    """Rescale a headroom fraction onto [min_limit..max_limit]."""
    min_limit = max(1, int(min_limit))
    max_limit = max(min_limit, int(max_limit))
    span = max_limit - min_limit
    raw = min_limit + _clamp01(fraction) * span
    return int(max(min_limit, min(max_limit, _round_half_up(raw))))


def compute_limit(
    signal: Signal,
    *,
    min_limit: int = MIN_LIMIT,
    max_limit: int = DEFAULT_MAX_LIMIT,
    velocity_weight: float = 0.0,
    dq05_avail_mb: int | None = None,
    dq05_mem_floor_mb: int = DEFAULT_DQ05_MEM_FLOOR_MB,
    dimensions: Sequence[str] | None = None,
) -> tuple[int, float, list[str]]:
    """Map the Kalman resource-pressure signal to an integer limit in [1..max].

    Returns ``(limit, headroom_fraction, notes)``.

    Three source shapes, in order of fidelity to the dispatcher's own fold:

    1. ``per_dimension`` published by ``_compute_dispatch_headroom`` — the same
       Kalman-informed headroom the dispatcher folds into ``target_workers``.
       We fold it the same way: the fleet is limited by the *minimum* dimension.
    2. a bare ``headroom`` fraction.
    3. a smoothed/raw capacity count on the host's pool scale, normalised
       against the host ceiling (the daemon's / the assigner's mirror).
    """
    notes: list[str] = []
    fraction: float | None = None

    per_dim = dict(signal.per_dimension or {})
    if per_dim:
        selected = {
            k: float(v)
            for k, v in per_dim.items()
            if not dimensions or k in dimensions
        }
        if selected:
            fraction = _clamp01(min(selected.values()))
            notes.append(
                "dispatcher headroom: min over "
                f"{sorted(selected)} = {fraction:.2f} "
                f"(all dimensions: { {k: round(v, 2) for k, v in per_dim.items()} })"
            )
        else:
            notes.append(
                f"no requested dimension present in {sorted(per_dim)} — "
                "falling back to the capacity scale"
            )

    if fraction is None and signal.headroom is not None:
        fraction = _clamp01(signal.headroom)
        notes.append(f"published headroom {fraction:.2f}")

    if fraction is None:
        fraction = headroom_fraction(
            signal.pool_smoothed,
            signal.ceiling,
            velocity=signal.velocity,
            velocity_weight=velocity_weight,
        )
        notes.append(
            f"kalman smoothed pool {signal.pool_smoothed:.2f}/{signal.ceiling:g} "
            f"-> headroom {fraction:.2f}"
        )

    limit = capacity_to_limit(fraction, min_limit, max_limit)
    notes.append(f"headroom {fraction:.2f} -> limit {limit}")

    # Explicit capacity count ("how many dispatches are possible") as an upper
    # clamp, when the source publishes one.
    if signal.capacity is not None:
        cap_limit = int(
            max(
                int(min_limit),
                min(int(max_limit), _round_half_up(float(signal.capacity))),
            )
        )
        if cap_limit < limit:
            notes.append(
                f"capacity clamp: published capacity {signal.capacity:g} caps {limit} -> {cap_limit}"
            )
            limit = cap_limit

    # Fail-safe clamp on the *unsmoothed* capacity, only meaningful on the
    # pool scale: smoothing must not paper over a tick that measured pressure.
    if fraction is not None and not per_dim and signal.headroom is None:
        raw_fraction = headroom_fraction(signal.pool_raw, signal.ceiling)
        raw_limit = capacity_to_limit(raw_fraction, min_limit, max_limit)
        if raw_limit < limit:
            notes.append(
                f"raw capacity clamp: raw pool {signal.pool_raw:.2f} "
                f"(headroom {raw_fraction:.2f}) caps {limit} -> {raw_limit}"
            )
            limit = raw_limit

    if dq05_avail_mb is not None and dq05_avail_mb < dq05_mem_floor_mb:
        notes.append(
            f"DQ05 memory safety clamp: MemAvailable {dq05_avail_mb}MB < "
            f"floor {dq05_mem_floor_mb}MB -> limit {min_limit}"
        )
        limit = max(1, int(min_limit))

    return int(limit), fraction, notes


# ----------------------------------------------------------------- decision


@dataclass(frozen=True)
class Decision:
    limit: int
    previous: int | None
    action: str
    reason: str
    signal: Signal
    headroom: float
    notes: tuple[str, ...] = ()
    idle: bool | None = None
    idle_detail: str = ""
    override: int | None = None
    dq05_avail_mb: int | None = None
    dry_run: bool = False

    def as_dict(self) -> dict:
        return {
            "signal": self.signal.as_dict(),
            "headroom_fraction": round(self.headroom, 4),
            "computed_limit": self.limit,
            "previous_limit": self.previous,
            "action": self.action,
            "reason": self.reason,
            "notes": list(self.notes),
            "idle": self.idle,
            "idle_detail": self.idle_detail,
            "override": self.override,
            "dq05_avail_mb": self.dq05_avail_mb,
            "dry_run": self.dry_run,
        }


def resolve_override(
    pinned: int | None,
    *,
    min_limit: int = MIN_LIMIT,
    max_limit: int = DEFAULT_MAX_LIMIT,
) -> tuple[int | None, str]:
    """Clamp an operator pin into the legal range (never below 1, never > cap)."""
    if pinned is None:
        return None, ""
    value = int(pinned)
    clamped = max(min(1, int(min_limit)), min(value, int(max_limit)))
    if clamped != value:
        return clamped, f"override {value} clamped to {clamped} (legal range 1..{max_limit})"
    return clamped, f"override pins the limit to {clamped}"


def decide(
    *,
    signal: Signal,
    previous: int | None,
    idle: bool | None = None,
    idle_detail: str = "",
    dry_run: bool = False,
    override: int | None = None,
    min_limit: int = MIN_LIMIT,
    max_limit: int = DEFAULT_MAX_LIMIT,
    velocity_weight: float = 0.0,
    dq05_avail_mb: int | None = None,
    dq05_mem_floor_mb: int = DEFAULT_DQ05_MEM_FLOOR_MB,
    dimensions: Sequence[str] | None = None,
) -> Decision:
    """Decide what to do this tick. Pure: no side effects."""
    limit, fraction, notes = compute_limit(
        signal,
        min_limit=min_limit,
        max_limit=max_limit,
        velocity_weight=velocity_weight,
        dq05_avail_mb=dq05_avail_mb,
        dq05_mem_floor_mb=dq05_mem_floor_mb,
        dimensions=dimensions,
    )

    pinned, override_note = resolve_override(override, min_limit=min_limit, max_limit=max_limit)
    if pinned is not None:
        limit = pinned
        notes.append(override_note)

    def make(action: str, reason: str) -> Decision:
        return Decision(
            limit=limit,
            previous=previous,
            action=action,
            reason=reason,
            signal=signal,
            headroom=fraction,
            notes=tuple(notes),
            idle=idle,
            idle_detail=idle_detail,
            override=pinned,
            dq05_avail_mb=dq05_avail_mb,
            dry_run=dry_run,
        )

    if previous is None:
        return make(ACTION_DEFER, "cannot read the live NGIT_CI_MAX_CONCURRENT_JOBS value; not acting")

    if limit == previous:
        return make(ACTION_NOOP, f"unchanged: computed limit {limit} == live value {previous}")

    if idle is not True:
        detail = idle_detail or "idle state unknown"
        return make(ACTION_DEFER, f"coordinator not provably idle ({detail}); recreate deferred")

    if dry_run:
        return make(
            ACTION_DRY_RUN,
            f"would set NGIT_CI_MAX_CONCURRENT_JOBS {previous} -> {limit} and recreate coordinator",
        )

    return make(ACTION_APPLY, f"set NGIT_CI_MAX_CONCURRENT_JOBS {previous} -> {limit}")


# ------------------------------------------------------------ idle detection

_START_RE = re.compile(r"Starting queued CI job trigger_event=([0-9a-fA-F]+)")
_DONE_RE = re.compile(r"Completed embedded CI for trigger trigger_event=([0-9a-fA-F]+)")
_ENQUEUE_RE = re.compile(r"Enqueued CI job trigger_event=([0-9a-fA-F]+)")
# The timestamp is the FIRST RFC3339 field in the line: `docker compose logs`
# prefixes each line with "<service>-1  | " before the message's own timestamp.
_TS_RE = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)")
_ACT_NAME_RE = re.compile(r"^act-")

#: A job that was just enqueued has not started yet; treat a fresh enqueue as
#: busy for this long so we never recreate the coordinator into the gap between
#: "Enqueued" and the act container appearing.
ENQUEUE_GRACE_S = 30.0

#: An act-* container that has been running longer than this cannot be an
#: active job: the coordinator logs ``job_timeout_secs=1800`` (30 min) and kills
#: jobs past it, so anything older is an orphan (act left one behind in
#: production: ``act-Test-and-Build-... Up 2 hours`` long after its run
#: completed). This is a *timeout-based* proof of staleness, so it does not
#: depend on how far back the fetched log window reaches.
MAX_JOB_AGE_S = 2100.0  # 35 min = coordinator job timeout (1800s) + margin


def _parse_ts(text: str) -> float | None:
    import datetime as _dt

    m = _TS_RE.search(text)
    if not m:
        return None
    raw = m.group(1)
    try:
        return _dt.datetime.fromisoformat(raw).replace(tzinfo=_dt.timezone.utc).timestamp()
    except ValueError:
        return None


def parse_coordinator_log(text: str) -> dict:
    """Extract job lifecycle facts from raw ``docker compose logs`` output."""
    started: dict[str, float | None] = {}
    completed: set[str] = set()
    enqueued: list[tuple[str, float | None]] = []
    last_completed_ts: float | None = None

    for line in text.splitlines():
        ts = _parse_ts(line)
        m = _START_RE.search(line)
        if m:
            started[m.group(1)] = ts
            continue
        m = _DONE_RE.search(line)
        if m:
            completed.add(m.group(1))
            if ts is not None and (last_completed_ts is None or ts > last_completed_ts):
                last_completed_ts = ts
            continue
        m = _ENQUEUE_RE.search(line)
        if m:
            enqueued.append((m.group(1), ts))

    in_flight = sorted(t for t in started if t not in completed)
    return {
        "started": len(started),
        "completed": len(completed),
        "in_flight": in_flight,
        "last_completed_ts": last_completed_ts,
        "last_enqueue_ts": max((ts for _, ts in enqueued if ts is not None), default=None),
    }


def classify_idle(
    log_text: str,
    act_containers: Sequence[Mapping] = (),
    *,
    now: float | None = None,
    enqueue_grace_s: float = ENQUEUE_GRACE_S,
    max_job_age_s: float = MAX_JOB_AGE_S,
    log_readable: bool = True,
) -> tuple[bool, str, dict]:
    """Decide whether the coordinator is provably idle.

    ``act_containers`` items: ``{"name": str, "running": bool, "started": float|None}``.

    Two independent proofs, because either source can be incomplete:

    * the coordinator log gives started/completed trigger pairs (an unmatched
      start is an in-flight job) and fresh enqueues;
    * the dind daemon gives live ``act-*`` job containers. A container is only
      dismissed as an orphan when it is older than the coordinator's own job
      timeout (``max_job_age_s``) — so a truncated log window can never hide a
      live job.

    Fail-closed: anything we cannot prove is "not idle". Returns
    ``(idle, reason, facts)``.
    """
    import time as _time

    now = _time.time() if now is None else now
    facts: dict = {"log_available": bool(log_readable and log_text.strip())}

    if not log_readable:
        return False, "coordinator log unavailable — cannot prove idle", facts

    if not log_text.strip():
        return False, "coordinator log empty — cannot prove idle", facts

    parsed = parse_coordinator_log(log_text)
    facts.update({k: v for k, v in parsed.items() if k != "last_enqueue_ts"})
    last_completed = parsed["last_completed_ts"]
    if last_completed is not None:
        facts["last_completed_age_s"] = round(now - last_completed, 1)

    if parsed["in_flight"]:
        ids = ",".join(t[:12] for t in parsed["in_flight"][:3])
        return False, f"{len(parsed['in_flight'])} job(s) started but not completed ({ids})", facts

    last_enqueue = parsed["last_enqueue_ts"]
    if last_enqueue is not None and (now - last_enqueue) < enqueue_grace_s:
        after_last_done = last_completed is None or last_enqueue > last_completed
        if after_last_done:
            return (
                False,
                f"job enqueued {now - last_enqueue:.0f}s ago and not yet completed",
                facts,
            )

    live: list[tuple[str, float | None]] = []
    for c in act_containers:
        name = str(c.get("name", ""))
        if not _ACT_NAME_RE.match(name):
            continue
        started = c.get("started")
        running = bool(c.get("running"))
        age = None if started is None else now - started
        # LIVE while it is younger than the coordinator's job timeout; only an
        # older container is provably an orphan act never cleaned up.
        if age is None or age < max_job_age_s:
            live.append((name, started))
        elif running:
            facts.setdefault("stale_act_containers", []).append(
                {"name": name, "age_s": None if age is None else round(age, 1)}
            )

    if live:
        ages = ", ".join(
            f"{n[:40]} age={'' if s is None else f'{now - s:.0f}s'}" for n, s in live[:3]
        )
        return False, f"act job container(s) present ({ages})", facts

    if parsed["started"] == 0 and parsed["completed"] == 0:
        facts["no_job_history"] = True

    detail = (
        f"{parsed['completed']}/{parsed['started']} jobs completed, "
        f"no in-flight trigger, no live act container"
    )
    return True, detail, facts


# --------------------------------------------------------------- env patching

def patch_env_value(text: str, key: str, value) -> tuple[str, bool]:
    """Set ``key=value`` in a dotenv-style file, preserving everything else.

    Returns ``(new_text, changed)``. Idempotent: unchanged input is returned
    verbatim with ``changed=False``.
    """
    pattern = re.compile(rf"^{re.escape(key)}=.*$", re.MULTILINE)
    new_line = f"{key}={value}"
    existing = pattern.search(text)
    if existing is not None:
        if existing.group(0) == new_line:
            return text, False
        return pattern.sub(new_line, text, count=1), True
    sep = "" if text.endswith("\n") or text == "" else "\n"
    return f"{text}{sep}{new_line}\n", True


def parse_env_value(text: str, key: str) -> str | None:
    pattern = re.compile(rf"^{re.escape(key)}=(.*)$", re.MULTILINE)
    m = pattern.search(text)
    return m.group(1).strip() if m else None


def parse_kv_state(text: str) -> dict[str, str]:
    """Parse the ``KEY=VALUE`` state file written by the dispatch daemon."""
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip()
    return out


@dataclass
class LogRecord:
    """One decision record. Never contains key material — by construction."""

    ts: str
    signal: dict
    computed_limit: int
    previous_limit: int | None
    action: str
    reason: str
    idle: bool | None = None
    idle_detail: str = ""
    override: int | None = None
    dry_run: bool = False
    extra: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        d = {
            "ts": self.ts,
            "signal": self.signal,
            "computed_limit": self.computed_limit,
            "previous_limit": self.previous_limit,
            "action": self.action,
            "reason": self.reason,
            "idle": self.idle,
            "idle_detail": self.idle_detail,
            "override": self.override,
            "dry_run": self.dry_run,
        }
        d.update(self.extra)
        return d
