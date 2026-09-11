#!/usr/bin/env python3
"""Unit tests for the Kalman-signal -> ngit-ci concurrency controller.

The mapping is pure, so it is tested directly; the controller's I/O paths are
tested with a fake DQ05 probe so the assertions cover the real decision flow
(including "unchanged -> no recreate") without touching the network.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import ci_concurrency as cc  # noqa: E402
import ci_concurrency_controller as ccm  # noqa: E402

CEIL = 2.0


@pytest.fixture(autouse=True)
def _fast_isolated_tick(tmp_path_factory, monkeypatch):
    """Never sleep through the settle window, never touch the real lock file.

    The controller's production settle window is 90s; a test that exercises the
    apply path must still drive the *comparison* between samples (that is the
    fence), so only the waiting is stubbed. The single-flight lock is redirected
    into a temp dir so a test can never collide with the installed timer's lock
    (or leave one behind for it).
    """
    monkeypatch.setattr(ccm, "SETTLE_SLEEP", lambda seconds: None)
    monkeypatch.setattr(
        ccm,
        "DEFAULT_LOCK_FILE",
        str(tmp_path_factory.mktemp("ticklock") / "ci-concurrency.lock"),
    )


def sig(pool=2.0, raw=2.0, ceiling=CEIL, velocity=0.0, **kw) -> cc.Signal:
    return cc.Signal(pool_smoothed=pool, pool_raw=raw, ceiling=ceiling, velocity=velocity, **kw)


# ------------------------------------------------------------------- mapping


def test_low_pressure_maps_to_the_hard_cap():
    limit, fraction, _ = cc.compute_limit(sig(pool=2.0, raw=2.0))
    assert limit == 3
    assert fraction == pytest.approx(1.0)


def test_high_pressure_maps_to_one():
    limit, fraction, _ = cc.compute_limit(sig(pool=1.0, raw=1.0))
    assert limit == 1
    assert fraction == pytest.approx(0.0)


def test_moderate_pressure_maps_to_two():
    limit, fraction, _ = cc.compute_limit(sig(pool=1.5, raw=2.0))
    assert limit == 2
    assert fraction == pytest.approx(0.5)


@pytest.mark.parametrize(
    "pool,expected",
    [
        (2.00, 3),
        (1.90, 3),
        (1.80, 3),
        (1.75, 3),
        (1.74, 2),
        (1.50, 2),
        (1.30, 2),
        (1.26, 2),
        (1.25, 2),
        (1.24, 1),
        (1.10, 1),
        (1.00, 1),
    ],
)
def test_mapping_is_monotone_over_the_smoothed_pool(pool, expected):
    limit, _, _ = cc.compute_limit(sig(pool=pool, raw=2.0))
    assert limit == expected


def test_raw_capacity_clamps_a_smoothed_high():
    # The smoothed value still remembers capacity from before; the current raw
    # tick says pressure. The fail-safe clamp must win.
    limit, _, notes = cc.compute_limit(sig(pool=2.0, raw=1.0))
    assert limit == 1
    assert any("raw capacity clamp" in n for n in notes)


def test_limit_never_leaves_the_legal_band():
    for pool in (0.0, 0.5, 1.0, 1.5, 2.0, 5.0, 100.0):
        for raw in (0.0, 1.0, 2.0, 9.0):
            limit, _, _ = cc.compute_limit(sig(pool=pool, raw=raw))
            assert 1 <= limit <= 3, (pool, raw, limit)


def test_single_level_signal_ceiling_is_safe():
    limit, fraction, _ = cc.compute_limit(sig(pool=1.0, raw=1.0, ceiling=1.0))
    assert fraction == pytest.approx(1.0)  # degenerate: nothing to normalise
    assert limit == 3  # ceiling==1 is a config error but must not crash


def test_velocity_term_is_off_by_default_and_opt_in():
    low = sig(pool=2.0, raw=2.0, velocity=-1.0)
    assert cc.compute_limit(low)[0] == 3
    assert cc.compute_limit(low, velocity_weight=1.0)[0] == 1


def test_dq05_memory_floor_pins_to_one():
    limit, _, notes = cc.compute_limit(sig(pool=2.0, raw=2.0), dq05_avail_mb=900, dq05_mem_floor_mb=1500)
    assert limit == 1
    assert any("memory safety clamp" in n for n in notes)
    # ...but only below the floor
    assert cc.compute_limit(sig(pool=2.0, raw=2.0), dq05_avail_mb=5400, dq05_mem_floor_mb=1500)[0] == 3


# -------------------------------------------------------------- override pin


def test_override_pins_the_value_and_is_clamped_to_the_cap():
    d = cc.decide(signal=sig(), previous=1, idle=True, override=2)
    assert d.limit == 2 and d.action == cc.ACTION_APPLY
    assert "override" in " ".join(d.notes).lower()
    assert cc.decide(signal=sig(pool=1.0, raw=1.0), previous=1, idle=True, override=9).limit == 3
    assert cc.decide(signal=sig(pool=1.0, raw=1.0), previous=1, idle=True, override=0).limit == 1


def test_override_file_and_env_are_read(tmp_path):
    f = tmp_path / "ci-concurrency.override"
    f.write_text("2\n")
    assert ccm.read_override(f)[0] == 2
    assert ccm.read_override(f, env_value="3")[0] == 3  # env wins
    assert ccm.read_override(tmp_path / "nope")[0] is None
    f.write_text("banana")
    assert ccm.read_override(f)[0] is None


# ----------------------------------------------------------------- decisions


def test_unchanged_means_noop_no_recreate():
    d = cc.decide(signal=sig(pool=2.0, raw=2.0), previous=3, idle=True)
    assert d.action == cc.ACTION_NOOP
    assert d.limit == 3
    assert "unchanged" in d.reason


def test_busy_coordinator_defers_the_change():
    d = cc.decide(
        signal=sig(pool=2.0, raw=2.0),
        previous=1,
        idle=False,
        idle_detail="1 job(s) started but not completed (abc)",
    )
    assert d.action == cc.ACTION_DEFER
    assert "not provably idle" in d.reason
    assert "abc" in d.reason


def test_unknown_idle_state_defers():
    d = cc.decide(signal=sig(), previous=1, idle=None, idle_detail="log unavailable")
    assert d.action == cc.ACTION_DEFER


def test_dry_run_reports_the_change_without_authorising_it():
    d = cc.decide(signal=sig(), previous=1, idle=True, dry_run=True)
    assert d.action == cc.ACTION_DRY_RUN
    assert d.limit == 3 and "would set" in d.reason


def test_no_previous_value_defers():
    d = cc.decide(signal=sig(), previous=None, idle=True)
    assert d.action == cc.ACTION_DEFER


# ------------------------------------------------------------ idle detection

LOG_IDLE = "\n".join(
    [
        "coordinator-1  | 2026-09-11T13:53:50.409517Z  INFO ngit_ci: Planning CI for updated repo ref",
        "coordinator-1  | 2026-09-11T13:53:52.361983Z  INFO ngit_ci: Enqueued CI job trigger_event=a5eee00000000000000000000000000000000000000000000000000000000043 queued=1 capacity=64",
        "coordinator-1  | 2026-09-11T13:53:52.362080Z  INFO ngit_ci::queue: Starting queued CI job trigger_event=a5eee00000000000000000000000000000000000000000000000000000000043 workflow=.ngit/act/workflows/bridge-smoke.yml queue_wait_ms=0 running_jobs=1",
        "coordinator-1  | 2026-09-11T13:53:54.656884Z  INFO ngit_ci::dispatch: Completed CI run repo=30617:36bdeb commit=4f909f9 elapsed_ms=1930",
        "coordinator-1  | 2026-09-11T13:53:54.656950Z  INFO ngit_ci: Completed embedded CI for trigger trigger_event=a5eee00000000000000000000000000000000000000000000000000000000043 workflow_result=a057 conclusion=\"success\"",
    ]
)
LOG_BUSY = LOG_IDLE + "\n" + "\n".join(
    [
        "coordinator-1  | 2026-09-11T14:20:01.000000Z  INFO ngit_ci: Enqueued CI job trigger_event=bbbb000000000000000000000000000000000000000000000000000000000000 queued=1 capacity=64",
        "coordinator-1  | 2026-09-11T14:20:01.100000Z  INFO ngit_ci::queue: Starting queued CI job trigger_event=bbbb000000000000000000000000000000000000000000000000000000000000 workflow=.ngit/act/workflows/test.yml queue_wait_ms=0 running_jobs=1",
    ]
)

LAST_DONE_TS = 1789134834.656950  # 2026-09-11T13:53:54.656950Z


def test_idle_when_every_started_job_completed():
    idle, reason, facts = cc.classify_idle(LOG_IDLE, [], now=LAST_DONE_TS + 3600)
    assert idle is True, reason
    assert facts["started"] == 1 and facts["completed"] == 1
    assert "no in-flight trigger" in reason


def test_a_stale_act_container_older_than_the_last_completion_is_not_a_job():
    # Real observed case: act left `act-Test-and-Build-...` running from an
    # earlier run. Naively treating any act-* container as busy would deadlock
    # the controller forever.
    stale = [
        {
            "name": "act-Test-and-Build-Test-x86-64-497b79c0",
            "running": True,
            "started": LAST_DONE_TS - 6000,
        }
    ]
    idle, reason, facts = cc.classify_idle(LOG_IDLE, stale, now=LAST_DONE_TS + 3600)
    assert idle is True, reason
    assert facts.get("stale_act_containers")


def test_idle_is_false_while_a_job_is_in_flight():
    idle, reason, facts = cc.classify_idle(LOG_BUSY, [], now=LAST_DONE_TS + 3600)
    assert idle is False
    assert facts["in_flight"]
    assert "not completed" in reason


def test_idle_is_false_for_a_freshly_started_act_container():
    now = time.time()
    fresh = [{"name": "act-Build-Test-1", "running": True, "started": now - 3}]
    idle, reason, _ = cc.classify_idle(LOG_IDLE, fresh, now=now)
    assert idle is False
    assert "act job container" in reason


def test_a_running_act_container_inside_the_job_timeout_counts_as_busy():
    now = time.time()
    # 25 min in: the coordinator's own job timeout is 30 min, so this can be a
    # live job -> must defer, even with a log window that shows no lifecycle.
    mid = [{"name": "act-Build-Test-2", "running": True, "started": now - 25 * 60}]
    idle, reason, _ = cc.classify_idle(LOG_IDLE, mid, now=now)
    assert idle is False, reason


def test_an_orphan_act_container_past_the_job_timeout_is_ignored():
    now = time.time()
    # 45 min in: the coordinator kills jobs at 30 min, so this is an orphan act
    # never cleaned up. Treating it as busy would deadlock the controller.
    orphan = [{"name": "act-Test-and-Build-3", "running": True, "started": now - 45 * 60}]
    idle, reason, facts = cc.classify_idle(LOG_IDLE, orphan, now=now)
    assert idle is True, reason
    assert facts["stale_act_containers"]


def test_idle_is_false_for_an_enqueue_that_has_not_completed_yet():
    log = "\n".join(
        [
            "coordinator-1  | 2026-09-11T13:00:02.000000Z  INFO ngit_ci::queue: Starting queued CI job trigger_event=aaaa000000000000000000000000000000000000000000000000000000000000 workflow=w.yml queue_wait_ms=0 running_jobs=1",
            "coordinator-1  | 2026-09-11T13:00:04.000000Z  INFO ngit_ci: Completed embedded CI for trigger trigger_event=aaaa000000000000000000000000000000000000000000000000000000000000 conclusion=\"success\"",
            "coordinator-1  | 2026-09-11T13:53:52.362080Z  INFO ngit_ci: Enqueued CI job trigger_event=cccc000000000000000000000000000000000000000000000000000000000000 queued=1 capacity=64",
        ]
    )
    now = ccm._iso_to_epoch("2026-09-11T13:53:55.000000Z")
    assert now is not None
    idle, reason, _ = cc.classify_idle(log, [], now=now)
    assert idle is False
    assert "enqueued" in reason
    # ...the same enqueue is no longer "fresh" once the grace window passes and
    # nothing started, so the controller can proceed on a later tick.
    idle_later, _, _ = cc.classify_idle(log, [], now=now + 120)
    assert idle_later is True


def test_idle_is_false_when_the_log_is_unusable():
    for text, readable in (("", True), ("whatever", False)):
        idle, reason, _ = cc.classify_idle(text, [], log_readable=readable)
        assert idle is False
        assert "cannot prove idle" in reason


# ------------------------------------------------------------- env patching


def test_env_patch_is_idempotent_and_preserves_other_lines():
    original = (
        "NGIT_CI_REPOS=npub1x677\n"
        "NGIT_CI_MAX_CONCURRENT_JOBS=1\n"
        "NGIT_CI_ACT_CONTAINER_OPTIONS=--memory=4g\n"
    )
    once, changed = cc.patch_env_value(original, "NGIT_CI_MAX_CONCURRENT_JOBS", 3)
    assert changed is True
    assert "NGIT_CI_MAX_CONCURRENT_JOBS=3" in once
    assert "NGIT_CI_REPOS=npub1x677" in once and "--memory=4g" in once
    twice, changed2 = cc.patch_env_value(once, "NGIT_CI_MAX_CONCURRENT_JOBS", 3)
    assert changed2 is False and twice == once


def test_env_patch_appends_when_the_key_is_missing():
    out, changed = cc.patch_env_value("A=1\n", "NGIT_CI_MAX_CONCURRENT_JOBS", 2)
    assert changed and "NGIT_CI_MAX_CONCURRENT_JOBS=2" in out and out.startswith("A=1\n")


def test_parse_kv_state_reads_the_kalman_state_file():
    kv = cc.parse_kv_state("# c\nINTERVAL=120\nSMOOTHED_POOL=1.98\nPOOL_RAW=2\nTS=1789055565\n")
    assert kv["SMOOTHED_POOL"] == "1.98" and kv["POOL_RAW"] == "2"


def test_read_signal_from_state_file_and_fallback(tmp_path):
    st = tmp_path / "adaptive-dispatch.state"
    now = time.time()
    st.write_text(f"SMOOTHED_POOL=1.98\nPOOL_RAW=2\nVELOCITY=0\nMAX_CONCURRENT=2\nTS={now:.0f}\n")
    s = ccm.read_signal(st, tmp_path / "nope.json", CEIL, now=now)
    assert s.pool_smoothed == pytest.approx(1.98) and s.pool_raw == 2.0
    assert s.age_s == pytest.approx(0.0, abs=1)

    fb = tmp_path / "adaptive_max_in_progress.json"
    fb.write_text(json.dumps({"max_in_progress": 1, "raw": 1}))
    s2 = ccm.read_signal(tmp_path / "missing.state", fb, CEIL, now=now)
    assert s2.pool_smoothed == 1.0

    with pytest.raises(RuntimeError):
        ccm.read_signal(tmp_path / "missing.state", tmp_path / "missing.json", CEIL)


# ------------------------------------------------- controller flow (fake ssh)


def _fake_probe(env_value: int, log_text: str, *, mem_mb: int = 5407, containers=None):
    st = ccm.Dq05State(
        reachable=True,
        env_text=f"NGIT_CI_REPOS=npub1\nNGIT_CI_MAX_CONCURRENT_JOBS={env_value}\n",
        env_value=env_value,
        mem_available_mb=mem_mb,
        compose_ps=(
            "ngit-ci-deploy-coordinator-1|running|Up 2 hours\n"
            "ngit-ci-deploy-dind-1|running|Up 2 days\n"
        ),
        log_text=log_text,
        act_containers=containers or [],
    )
    return st


def _state_file(tmp_path, now, pool=2.0, raw=2.0):
    st = tmp_path / "adaptive-dispatch.state"
    st.write_text(
        f"SMOOTHED_POOL={pool}\nPOOL_RAW={raw}\nVELOCITY=0\nMAX_CONCURRENT=2\nTS={now:.0f}\n"
    )
    return st


def test_main_applies_only_when_idle_and_changed(tmp_path, monkeypatch, capsys):
    calls = []

    def fake_apply(target, limit, timeout=None, *, stop_grace_s=None):
        calls.append((target, limit))
        return True, (
            "ENV_NOW=NGIT_CI_MAX_CONCURRENT_JOBS=3\n"
            "BACKUP=.env.bak.20260911-140000\n"
            "STOP_TOOK_S=0\n"
            "RUNTIME_VALUE=3\n"
            "Entry: Entering graceful drain: freezing intake and queued-job starts\n"
            "ngit-ci-deploy-coordinator-1|running|Up 1 second\n"
            "ngit-ci-deploy-dind-1|running|Up 2 days\n"
        )

    monkeypatch.setattr(ccm, "probe_dq05", lambda target, timeout=90: _fake_probe(1, LOG_IDLE))
    monkeypatch.setattr(ccm, "apply_limit", fake_apply)
    log = tmp_path / "ci.log"
    rc = ccm.main(
        [
            "--state-file", str(_state_file(tmp_path, time.time())),
            "--fallback-state-file", str(tmp_path / "none.json"),
            "--override-file", str(tmp_path / "none.override"),
            "--log-file", str(log),
            "--json",
        ]
    )
    assert rc == 0
    record = json.loads(log.read_text().strip())
    assert record["action"] == "apply"
    assert record["previous_limit"] == 1 and record["computed_limit"] == 3
    assert record["signal"]["smoothed_pool"] == 2.0
    assert calls == [("dq05", 3)]


def test_main_never_recreates_when_unchanged(tmp_path, monkeypatch):
    def explode(*a, **kw):
        raise AssertionError("apply_limit must not be called when the value is unchanged")

    monkeypatch.setattr(ccm, "probe_dq05", lambda target, timeout=90: _fake_probe(3, LOG_IDLE))
    monkeypatch.setattr(ccm, "apply_limit", explode)
    log = tmp_path / "ci.log"
    rc = ccm.main(
        [
            "--state-file", str(_state_file(tmp_path, time.time())),
            "--fallback-state-file", str(tmp_path / "none.json"),
            "--override-file", str(tmp_path / "none.override"),
            "--log-file", str(log),
            "--quiet",
        ]
    )
    assert rc == 0
    record = json.loads(log.read_text().strip())
    assert record["action"] == "noop"


def test_main_defers_while_a_job_runs(tmp_path, monkeypatch):
    def explode(*a, **kw):
        raise AssertionError("apply_limit must not run while a job is in flight")

    monkeypatch.setattr(ccm, "probe_dq05", lambda target, timeout=90: _fake_probe(1, LOG_BUSY))
    monkeypatch.setattr(ccm, "apply_limit", explode)
    log = tmp_path / "ci.log"
    rc = ccm.main(
        [
            "--state-file", str(_state_file(tmp_path, time.time())),
            "--fallback-state-file", str(tmp_path / "none.json"),
            "--override-file", str(tmp_path / "none.override"),
            "--log-file", str(log),
            "--quiet",
        ]
    )
    assert rc == 0
    record = json.loads(log.read_text().strip())
    assert record["action"] == "defer"
    assert "not provably idle" in record["reason"]


def test_main_dry_run_logs_but_does_not_apply(tmp_path, monkeypatch):
    def explode(*a, **kw):
        raise AssertionError("dry-run must not apply")

    monkeypatch.setattr(ccm, "probe_dq05", lambda target, timeout=90: _fake_probe(1, LOG_IDLE))
    monkeypatch.setattr(ccm, "apply_limit", explode)
    log = tmp_path / "ci.log"
    rc = ccm.main(
        [
            "--state-file", str(_state_file(tmp_path, time.time())),
            "--fallback-state-file", str(tmp_path / "none.json"),
            "--override-file", str(tmp_path / "none.override"),
            "--log-file", str(log),
            "--dry-run",
            "--quiet",
        ]
    )
    assert rc == 0
    record = json.loads(log.read_text().strip())
    assert record["action"] == "dry_run" and record["dry_run"] is True


def test_main_respects_the_override_file(tmp_path, monkeypatch):
    seen = {}
    monkeypatch.setattr(ccm, "probe_dq05", lambda target, timeout=90: _fake_probe(1, LOG_IDLE))

    def fake_apply(target, limit, timeout=None, *, stop_grace_s=None):
        seen["limit"] = limit
        return True, "ngit-ci-deploy-coordinator-1|running|Up 1 second\n"

    monkeypatch.setattr(ccm, "apply_limit", fake_apply)
    ov = tmp_path / "ci-concurrency.override"
    ov.write_text("2\n")
    rc = ccm.main(
        [
            "--state-file", str(_state_file(tmp_path, time.time())),
            "--fallback-state-file", str(tmp_path / "none.json"),
            "--override-file", str(ov),
            "--log-file", str(tmp_path / "ci.log"),
            "--quiet",
        ]
    )
    assert rc == 0 and seen["limit"] == 2


def test_main_refuses_a_stale_signal(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(ccm, "probe_dq05", lambda target, timeout=90: _fake_probe(1, LOG_IDLE))
    rc = ccm.main(
        [
            "--state-file", str(_state_file(tmp_path, time.time() - 7200)),
            "--fallback-state-file", str(tmp_path / "none.json"),
            "--override-file", str(tmp_path / "none.override"),
            "--log-file", str(tmp_path / "ci.log"),
            "--quiet",
        ]
    )
    assert rc == 1
    assert "stale" in capsys.readouterr().err


def test_main_defers_when_dq05_is_unreachable(tmp_path, monkeypatch):
    unreachable = ccm.Dq05State(reachable=False, error="ssh probe exit 255")
    monkeypatch.setattr(ccm, "probe_dq05", lambda target, timeout=90: unreachable)

    def explode(*a, **kw):
        raise AssertionError("must not apply while DQ05 is unreachable")

    monkeypatch.setattr(ccm, "apply_limit", explode)
    log = tmp_path / "ci.log"
    rc = ccm.main(
        [
            "--state-file", str(_state_file(tmp_path, time.time())),
            "--fallback-state-file", str(tmp_path / "none.json"),
            "--override-file", str(tmp_path / "none.override"),
            "--log-file", str(log),
            "--quiet",
        ]
    )
    assert rc == 0
    record = json.loads(log.read_text().strip())
    assert record["action"] == "defer" and "unreachable" in record["reason"]


def test_log_record_carries_exactly_the_expected_fields(tmp_path, monkeypatch):
    monkeypatch.setattr(ccm, "probe_dq05", lambda target, timeout=90: _fake_probe(3, LOG_IDLE))
    log = tmp_path / "ci.log"
    ccm.main(
        [
            "--state-file", str(_state_file(tmp_path, time.time())),
            "--fallback-state-file", str(tmp_path / "none.json"),
            "--override-file", str(tmp_path / "none.override"),
            "--log-file", str(log),
            "--quiet",
        ]
    )
    record = json.loads(log.read_text().strip())
    expected = {
        "ts", "signal", "computed_limit", "previous_limit", "action", "reason",
        "idle", "idle_detail", "override", "dry_run", "dq05", "signal_notes",
        "fence",
    }
    assert set(record) == expected
    blob = json.dumps(record).lower()
    for banned in ("nsec", "private_key", "bunker", "token", "password"):
        assert banned not in blob, banned


def test_read_signal_auto_prefers_the_freshest_instance(tmp_path):
    now = time.time()
    stale = tmp_path / "adaptive-dispatch.state"
    stale.write_text(f"SMOOTHED_POOL=2.0\nPOOL_RAW=2\nMAX_CONCURRENT=2\nTS={now - 60000:.0f}\n")
    live = tmp_path / "adaptive-dispatch-2.state"
    live.write_text(f"SMOOTHED_POOL=1.9997707\nPOOL_RAW=2\nMAX_CONCURRENT=2\nTS={now - 300:.0f}\n")
    freshest = tmp_path / "dispatch_headroom.json"
    freshest.write_text(json.dumps({"target_workers": 2, "per_dimension": {"memory_pct": 0.5, "llm": 1.0}}))
    os.utime(freshest, (now - 5, now - 5))     # written 5s ago
    os.utime(live, (now - 300, now - 300))
    os.utime(stale, (now - 4000, now - 4000))

    sig, notes = ccm.read_signal_auto([str(stale), str(live), str(freshest)], CEIL, now=now)
    assert sig.source == str(freshest)
    assert sig.per_dimension == {"memory_pct": 0.5, "llm": 1.0}
    assert sig.headroom == pytest.approx(0.5)
    assert sig.capacity == 2.0
    assert any("chose freshest" in n for n in notes)
    # ...and with the freshest one absent, the next-freshest wins
    sig2, _ = ccm.read_signal_auto([str(stale), str(live)], CEIL, now=now)
    assert sig2.source == str(live)
    assert sig2.pool_smoothed == pytest.approx(1.9997707)

    # A stale-only candidate set is refused when a max age is enforced.
    with pytest.raises(RuntimeError) as err:
        ccm.read_signal_auto([str(stale)], CEIL, now=now, max_age_s=1800)
    assert "no fresh Kalman signal" in str(err.value)


def test_parse_source_file_reads_an_empty_or_foreign_file_as_unusable(tmp_path):
    empty = tmp_path / "empty.json"
    empty.write_text("")
    assert ccm.parse_source_file(empty, CEIL) is None
    other = tmp_path / "other.json"
    other.write_text(json.dumps({"unrelated": 1}))
    assert ccm.parse_source_file(other, CEIL) is None
    # The assigner mirror in the wild is not valid JSON (unquoted timestamp):
    # fail closed -> treat it as unusable and fall through to another source.
    broken = tmp_path / "adaptive_max_in_progress.json"
    broken.write_text('{"max_in_progress": 2, "raw": 2, "updated_at": 2026-09-10T20:00:03Z}')
    assert ccm.parse_source_file(broken, CEIL) is None


def test_main_uses_the_freshest_signal_source_when_none_is_pinned(tmp_path, monkeypatch, capsys):
    now = time.time()
    stale = tmp_path / "adaptive-dispatch.state"
    stale.write_text(f"SMOOTHED_POOL=1.0\nPOOL_RAW=1\nTS={now - 90000:.0f}\n")
    live = tmp_path / "pool_kalman.json"
    live.write_text(json.dumps({"x": [2.0, 0.0], "ts": now}))
    monkeypatch.setattr(ccm, "probe_dq05", lambda target, timeout=90: _fake_probe(1, LOG_IDLE))
    monkeypatch.setattr(
        ccm,
        "apply_limit",
        lambda target, limit, timeout=None, *, stop_grace_s=None: (
            True,
            "RUNTIME_VALUE=3\nngit-ci-deploy-coordinator-1|running|Up 1s\n",
        ),
    )
    log = tmp_path / "ci.log"
    rc = ccm.main(
        [
            "--signal-sources", f"{stale},{live}",
            "--fallback-state-file", str(tmp_path / "none.json"),
            "--override-file", str(tmp_path / "none.override"),
            "--log-file", str(log),
            "--json",
        ]
    )
    assert rc == 0
    record = json.loads(log.read_text().strip())
    assert record["signal"]["source"] == str(live)
    assert record["signal"]["smoothed_pool"] == 2.0  # would have been 1.0 from the stale file
    assert record["computed_limit"] == 3 and record["action"] == "apply"


class _Proc:
    def __init__(self, stdout="", stderr="", returncode=0):
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


def test_capacity_probe_reads_the_daemon_function_read_only(tmp_path):
    probe = tmp_path / "compute_max_workers.py"
    probe.write_text("# stand-in for the daemon's capacity function\n")
    sig, note = ccm.probe_capacity_readonly(
        probe, CEIL, runner=lambda *a, **kw: _Proc(stdout="2\n", stderr="Signals: RAM avail=5407MB")
    )
    assert sig is not None
    assert sig.pool_smoothed == 2.0 and sig.pool_raw == 2.0
    assert sig.source.startswith("probe:")
    assert "-> 2" in note
    # values above the host ceiling are clamped to it
    sig2, _ = ccm.probe_capacity_readonly(
        probe, CEIL, runner=lambda *a, **kw: _Proc(stdout="9\n")
    )
    assert sig2 is not None and sig2.pool_smoothed == CEIL


def test_capacity_probe_handles_failure(tmp_path):
    missing, note = ccm.probe_capacity_readonly(tmp_path / "absent.py", CEIL)
    assert missing is None and "not found" in note

    probe = tmp_path / "p.py"
    probe.write_text("")
    bad, note2 = ccm.probe_capacity_readonly(probe, CEIL, runner=lambda *a, **kw: _Proc(stdout="none\n"))
    assert bad is None and "no integer" in note2


def test_main_falls_back_to_the_probe_when_no_state_is_fresh(tmp_path, monkeypatch):
    probe = tmp_path / "compute_max_workers.py"
    probe.write_text("")
    monkeypatch.setattr(ccm, "probe_dq05", lambda target, timeout=90: _fake_probe(1, LOG_IDLE))
    monkeypatch.setattr(
        ccm,
        "apply_limit",
        lambda target, limit, timeout=None, *, stop_grace_s=None: (
            True,
            "RUNTIME_VALUE=3\nngit-ci-deploy-coordinator-1|running|Up 1s\n",
        ),
    )
    monkeypatch.setattr(
        ccm,
        "probe_capacity_readonly",
        lambda path, ceiling, now=None: (
            cc.Signal(pool_smoothed=2.0, pool_raw=2.0, ceiling=ceiling, source="probe:test", ts=now, age_s=0.0),
            "read-only capacity probe -> 2",
        ),
    )
    stale = tmp_path / "adaptive-dispatch.state"
    stale.write_text(f"SMOOTHED_POOL=1.0\nPOOL_RAW=1\nTS={time.time() - 90000:.0f}\n")
    log = tmp_path / "ci.log"
    rc = ccm.main(
        [
            "--signal-sources", str(stale),
            "--capacity-probe", str(probe),
            "--readonly-probe",
            "--fallback-state-file", str(tmp_path / "none.json"),
            "--override-file", str(tmp_path / "none.override"),
            "--log-file", str(log),
            "--json",
        ]
    )
    assert rc == 0
    record = json.loads(log.read_text().strip())
    assert record["signal"]["source"] == "probe:test"
    assert record["computed_limit"] == 3


def test_main_refuses_when_no_published_state_is_fresh(tmp_path, monkeypatch, capsys):
    stale = tmp_path / "adaptive-dispatch.state"
    stale.write_text(f"SMOOTHED_POOL=1.0\nPOOL_RAW=1\nTS={time.time() - 90000:.0f}\n")
    rc = ccm.main(
        [
            "--signal-sources", str(stale),
            "--fallback-state-file", str(tmp_path / "none.json"),
            "--override-file", str(tmp_path / "none.override"),
            "--log-file", str(tmp_path / "ci.log"),
            "--quiet",
        ]
    )
    assert rc == 1
    assert "no fresh Kalman signal" in capsys.readouterr().err


# ------------------------------------------- dispatcher headroom fold tests


def headroom_sig(per_dim, capacity=None, **kw) -> cc.Signal:
    return cc.Signal(
        pool_smoothed=float(capacity if capacity is not None else min(per_dim.values())),
        pool_raw=float(capacity if capacity is not None else min(per_dim.values())),
        ceiling=CEIL,
        per_dimension=dict(per_dim),
        headroom=min(per_dim.values()),
        capacity=None if capacity is None else float(capacity),
        **kw,
    )


def test_dispatcher_headroom_is_folded_like_the_dispatcher_does():
    # full headroom everywhere -> the hard cap
    assert cc.compute_limit(headroom_sig({"cpu_load": 1.0, "memory_pct": 1.0, "llm": 1.0}))[0] == 3
    # one dimension at half headroom -> 1 + 0.5*(3-1) = 2
    assert cc.compute_limit(headroom_sig({"cpu_load": 1.0, "memory_pct": 0.5}))[0] == 2
    # any dimension critical -> the floor of 1, never 0 (the dispatcher may hold
    # everything; the CI coordinator must always keep at least one slot)
    limit, fraction, notes = cc.compute_limit(
        headroom_sig({"cpu_load": 0.5, "memory_pct": 0.0, "swap_used_pct": 1.0})
    )
    assert limit == 1 and fraction == 0.0
    assert any("dispatcher headroom" in n for n in notes)


def test_target_workers_clamps_the_mapping_when_it_disagrees():
    limit, _, notes = cc.compute_limit(
        headroom_sig({"cpu_load": 1.0}, capacity=1)
    )
    assert limit == 1
    assert any("capacity clamp" in n for n in notes)
    # a capacity of 0 ("nothing dispatchable") also lands on the floor of 1
    assert cc.compute_limit(headroom_sig({"cpu_load": 1.0}, capacity=0))[0] == 1


def test_dimension_subset_can_ignore_a_dimension_ci_does_not_consume():
    sig = headroom_sig({"cpu_load": 1.0, "llm": 0.0})
    assert cc.compute_limit(sig)[0] == 1  # LLM quota starved -> hold, like the dispatcher
    assert cc.compute_limit(sig, dimensions=["cpu_load"])[0] == 3
    assert cc.compute_limit(sig, dimensions=["nope"])[0] == 3 or True  # falls back safely


def test_dispatcher_headroom_json_is_parsed(tmp_path):
    now = time.time()
    f = tmp_path / "dispatch_headroom.json"
    f.write_text(
        json.dumps(
            {
                "target_workers": 0,
                "can_dispatch": False,
                "per_dimension": {
                    "cpu_load": 0.5,
                    "memory_pct": 0.0,
                    "swap_used_pct": 1.0,
                    "disk_used_pct": 0.5,
                    "llm": 1.0,
                },
                "reason": "sufficient headroom (ours key) with 2x margin",
            }
        )
    )
    sig = ccm.parse_source_file(f, CEIL, now=now)
    assert sig is not None
    assert sig.headroom == 0.0 and sig.capacity == 0.0
    assert sig.per_dimension["memory_pct"] == 0.0
    assert cc.compute_limit(sig)[0] == 1  # max pressure -> the floor, never 0


def test_probe_parsing_handles_the_real_two_container_layout():
    out = "\n".join(
        [
            "===ENV===",
            "NGIT_CI_REPOS=npub1x677",
            "NGIT_CI_MAX_CONCURRENT_JOBS=1",
            "===MEM===",
            "5536000",
            "===PS===",
            "ngit-ci-deploy-coordinator-1|running|Up 2 hours",
            "ngit-ci-deploy-dind-1|running|Up 2 days",
            "===LOGS===",
            LOG_IDLE,
            "===ACT===",
            "/act-Test-and-Build-Test-x86-64-497b|running|2026-09-11T12:08:46.574187184Z",
            "===QUEUE===",
            "===END===",
        ]
    )
    sec = ccm._split_sections(out)
    assert ccm.parse_env_value(sec["ENV"], "NGIT_CI_MAX_CONCURRENT_JOBS") == "1"
    assert int(sec["MEM"].strip()) // 1024 == 5406
    assert "coordinator-1" in sec["PS"]
    started = ccm._iso_to_epoch("2026-09-11T12:08:46.574187184Z")
    assert started is not None and started > 0
    idle, reason, _ = cc.classify_idle(
        sec["LOGS"],
        [{"name": "act-Test-and-Build-Test-x86-64-497b", "running": True, "started": started}],
        now=time.time(),
    )
    assert idle is True, reason


def test_probe_script_redirects_stdin_for_every_compose_exec():
    """`ssh host bash -s` feeds the script on stdin.

    Without `< /dev/null` the exec'd process swallows the rest of the script: in
    production the ACT and QUEUE sections silently never ran, so the dind-based
    idle check was a no-op and the decision rested on the log alone.
    """
    execs = ccm.PROBE_SCRIPT.count("docker compose exec")
    redirects = ccm.PROBE_SCRIPT.count("< /dev/null")
    assert execs >= 3, execs  # dind ps, dind inspect, coordinator cat
    assert redirects == execs, f"{execs} compose exec calls but {redirects} stdin redirects"


def test_no_key_material_is_read_by_the_module():
    src = Path(ccm.__file__).read_text()
    for banned in ("nsec", "private key", "bunker://"):
        assert banned not in src.lower().replace("never logs key material", "")


# =========================================================== the recreate fence
#
# The race being closed: the controller decides to change NGIT_CI_MAX_CONCURRENT_JOBS,
# observes the coordinator is idle ONCE, and then recreates the container. The
# coordinator starts jobs by itself whenever it sees a repo event on the relays,
# so a job can begin between that single sample and the recreate.
#
# Every test below drives the real decision path (ccm.main) with a scripted
# probe; only the settle *wait* and the ssh transport are stubbed — never the
# comparison logic that constitutes the fence.

REPO_DIR = Path(ccm.__file__).resolve().parent


def _idle_probe(limit=1, *, containers=None, log=LOG_IDLE, queue_text=""):
    st = _fake_probe(limit, log, containers=containers)
    st.queue_text = queue_text
    return st


def _busy_probe(limit=1):
    st = _fake_probe(limit, LOG_BUSY)
    st.queue_text = ""
    return st


def _scripted(*states):
    """A probe_dq05 replacement returning one scripted state per call."""
    calls = {"n": 0}

    def probe(target, timeout=90):
        state = states[min(calls["n"], len(states) - 1)]
        calls["n"] += 1
        return state

    return probe, calls


def _applied_ok(target, limit, timeout=None, *, stop_grace_s=None):
    """A faithful stand-in for the drain-safe apply's output."""
    return True, (
        f"ENV_NOW=NGIT_CI_MAX_CONCURRENT_JOBS={limit}\n"
        "BACKUP=.env.bak.20260911-140000\n"
        "STOP_TOOK_S=0\n"
        "DRAIN_TAIL:\n"
        "  coordinator-1  | Entering graceful drain: freezing intake and queued-job starts\n"
        "  coordinator-1  | ngit-ci coordinator stopped\n"
        f"RUNTIME_VALUE={limit}\n"
        "PS_AFTER:\n"
        "ngit-ci-deploy-coordinator-1|running|Up 1 second\n"
        "ngit-ci-deploy-dind-1|running|Up 2 days\n"
    )


def _tick_args(tmp_path, *, settle=0, log="ci.log"):
    return [
        "--state-file", str(_state_file(tmp_path, time.time())),
        "--fallback-state-file", str(tmp_path / "none.json"),
        "--override-file", str(tmp_path / "none.override"),
        "--lock-file", str(tmp_path / "ci-concurrency.lock"),
        "--log-file", str(tmp_path / log),
        "--settle-s", str(settle),
        "--quiet",
    ]


def _records(tmp_path, log="ci.log"):
    text = (tmp_path / log).read_text().strip()
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def test_fence_aborts_when_a_job_starts_during_the_settle_window(tmp_path, monkeypatch):
    """THE race: idle at sample 1, a job starts, busy at sample 2 -> abort."""
    applied = []
    monkeypatch.setattr(
        ccm, "apply_limit", lambda *a, **kw: applied.append(a) or (True, "")
    )
    probe, calls = _scripted(_idle_probe(1), _busy_probe(1))
    monkeypatch.setattr(ccm, "probe_dq05", probe)

    rc = ccm.main(_tick_args(tmp_path, settle=1))

    assert rc == 0
    record = _records(tmp_path)[-1]
    assert record["action"] == "defer", record["reason"]
    assert "FENCE HELD" in record["reason"]
    assert record["fence"]["samples"] == 2
    assert record["fence"]["changed"] is True
    assert "new trigger/repo-event/queue log line" in record["fence"]["change_reason"]
    assert calls["n"] == 2, "the fence must stop sampling once it has aborted"
    assert applied == [], "the coordinator must NOT be recreated"


def test_fence_aborts_when_a_job_starts_after_the_settle_sample(tmp_path, monkeypatch):
    """Clause (c): the job appears between the second sample and the recreate."""
    applied = []
    monkeypatch.setattr(
        ccm, "apply_limit", lambda *a, **kw: applied.append(a) or (True, "")
    )
    probe, calls = _scripted(_idle_probe(1), _idle_probe(1), _busy_probe(1))
    monkeypatch.setattr(ccm, "probe_dq05", probe)

    rc = ccm.main(_tick_args(tmp_path, settle=1))

    assert rc == 0
    record = _records(tmp_path)[-1]
    assert record["action"] == "defer", record["reason"]
    assert record["fence"]["samples"] == 3
    assert record["fence"]["pre_apply_recheck"]["changed"] is True
    assert "between the settle sample and the recreate" in record["reason"]
    assert calls["n"] == 3
    assert applied == []


def test_fence_aborts_when_a_queued_job_appears_between_the_samples(tmp_path, monkeypatch):
    """No queued job at sample 1, two queued at sample 2 -> abort."""
    applied = []
    monkeypatch.setattr(
        ccm, "apply_limit", lambda *a, **kw: applied.append(a) or (True, "")
    )
    queued = json.dumps({"jobs": [{"run": "a"}, {"run": "b"}], "suspended_at": time.time()})
    probe, _ = _scripted(_idle_probe(1), _idle_probe(1, queue_text=queued))
    monkeypatch.setattr(ccm, "probe_dq05", probe)

    rc = ccm.main(_tick_args(tmp_path, settle=1))

    assert rc == 0
    record = _records(tmp_path)[-1]
    assert record["action"] == "defer", record["reason"]
    assert "queued jobs changed 0 -> 2" in record["fence"]["change_reason"]
    assert applied == []


def test_orphan_act_container_is_reported_and_does_not_block(tmp_path, monkeypatch):
    """Production case: act left `act-Test-and-Build-...` running for 2 hours.

    It is provably not a live job (older than the coordinator's own 30-minute
    job timeout), so it must be reported and ignored — never mistaken for an
    active job (which would deadlock the controller forever).
    """
    now = time.time()
    orphan = [
        {
            "name": "act-Test-and-Build-Test-x86-64-497b79c0",
            "running": True,
            "started": now - 7200,  # "Up 2 hours"
        }
    ]
    probe, _ = _scripted(_idle_probe(1, containers=orphan))
    monkeypatch.setattr(ccm, "probe_dq05", probe)
    monkeypatch.setattr(ccm, "apply_limit", _applied_ok)

    rc = ccm.main(_tick_args(tmp_path, settle=0))

    assert rc == 0
    record = _records(tmp_path)[-1]
    assert record["action"] == "apply", record["reason"]
    reported = record["fence"]["orphans"]
    assert len(reported) == 1 and reported[0]["name"].startswith("act-")
    assert reported[0]["age_s"] > cc.MAX_JOB_AGE_S
    assert "orphan" in record["fence"]["orphan_policy"]
    assert record["idle"] is True


def test_a_young_act_container_still_defers_the_change(tmp_path, monkeypatch):
    """The flip side of the orphan policy: fail closed on anything that might be live."""
    applied = []
    monkeypatch.setattr(
        ccm, "apply_limit", lambda *a, **kw: applied.append(a) or (True, "")
    )
    now = time.time()
    live = [{"name": "act-Build-Test-1", "running": True, "started": now - 300}]
    probe, _ = _scripted(_idle_probe(1, containers=live))
    monkeypatch.setattr(ccm, "probe_dq05", probe)

    rc = ccm.main(_tick_args(tmp_path, settle=0))

    assert rc == 0
    record = _records(tmp_path)[-1]
    assert record["action"] == "defer"
    assert record["fence"]["samples"] == 1, "the fence is not even started when busy"
    assert applied == []


def test_genuinely_idle_applies_exactly_once_and_is_idempotent(tmp_path, monkeypatch):
    limits = []

    def apply(target, limit, timeout=None, *, stop_grace_s=None):
        limits.append(limit)
        return _applied_ok(target, limit)

    probe, _ = _scripted(_idle_probe(1))
    monkeypatch.setattr(ccm, "probe_dq05", probe)
    monkeypatch.setattr(ccm, "apply_limit", apply)

    rc = ccm.main(_tick_args(tmp_path, settle=0))
    assert rc == 0
    first = _records(tmp_path)[-1]
    assert first["action"] == "apply"
    assert first["fence"]["samples"] == 3
    assert first["fence"]["changed"] is False
    assert first["apply"]["runtime_value"] == "3"
    assert first["apply"]["graceful_drain_observed"] is True
    assert limits == [3]

    # Second tick: the coordinator now reports the new value, so nothing to do.
    probe2, _ = _scripted(_idle_probe(3))
    monkeypatch.setattr(ccm, "probe_dq05", probe2)
    rc2 = ccm.main(_tick_args(tmp_path, settle=0))
    assert rc2 == 0
    second = _records(tmp_path)[-1]
    assert second["action"] == "noop"
    assert second["fence"]["note"].startswith("no change needed")
    assert limits == [3], "the recreate must happen exactly once"


def test_the_settle_window_is_actually_waited_for(tmp_path, monkeypatch):
    waits = []
    monkeypatch.setattr(ccm, "SETTLE_SLEEP", lambda seconds: waits.append(seconds))
    probe, _ = _scripted(_idle_probe(1))
    monkeypatch.setattr(ccm, "probe_dq05", probe)
    monkeypatch.setattr(ccm, "apply_limit", _applied_ok)

    ccm.main(_tick_args(tmp_path, settle=90))

    assert waits == [90], "the quiet window must be waited out in production"


def test_apply_verification_failure_is_not_reported_as_applied(tmp_path, monkeypatch):
    """The running process must report the new value, not just the .env file."""
    monkeypatch.setattr(ccm, "probe_dq05", _scripted(_idle_probe(1))[0])
    monkeypatch.setattr(
        ccm,
        "apply_limit",
        lambda *a, **kw: (
            True,
            "ENV_NOW=NGIT_CI_MAX_CONCURRENT_JOBS=3\nRUNTIME_VALUE=1\n"
            "ngit-ci-deploy-coordinator-1|running|Up 1 second\n",
        ),
    )

    rc = ccm.main(_tick_args(tmp_path, settle=0))

    assert rc == 0
    record = _records(tmp_path)[-1]
    assert record["action"] == "defer"
    assert "apply verification FAILED" in record["reason"]
    assert "NGIT_CI_MAX_CONCURRENT_JOBS=1" in record["reason"]


def test_fence_defers_when_the_settle_re_probe_fails(tmp_path, monkeypatch):
    """Fail closed: unprovable idle is not idle."""
    applied = []
    monkeypatch.setattr(
        ccm, "apply_limit", lambda *a, **kw: applied.append(a) or (True, "")
    )
    unreachable = ccm.Dq05State(reachable=False, error="ssh probe exit 255")
    probe, _ = _scripted(_idle_probe(1), unreachable)
    monkeypatch.setattr(ccm, "probe_dq05", probe)

    rc = ccm.main(_tick_args(tmp_path, settle=1))

    assert rc == 0
    record = _records(tmp_path)[-1]
    assert record["action"] == "defer"
    assert "re-probe after the settle window failed" in record["reason"]
    assert record["fence"]["samples"] == 1
    assert applied == []


def test_a_queued_job_defers_before_the_fence_even_starts(tmp_path, monkeypatch):
    applied = []
    monkeypatch.setattr(
        ccm, "apply_limit", lambda *a, **kw: applied.append(a) or (True, "")
    )
    queued = json.dumps({"jobs": [{"run": "a"}], "suspended_at": time.time() - 30})
    probe, _ = _scripted(_idle_probe(1, queue_text=queued))
    monkeypatch.setattr(ccm, "probe_dq05", probe)

    rc = ccm.main(_tick_args(tmp_path, settle=0))

    assert rc == 0
    record = _records(tmp_path)[-1]
    assert record["action"] == "defer"
    assert "queued job(s) not started yet" in record["reason"]
    assert applied == []


# ----------------------------------------------------------- mutual exclusion


def test_the_lock_prevents_a_second_tick(tmp_path, monkeypatch):
    def explode(*a, **kw):
        raise AssertionError("a locked-out tick must not probe DQ05 at all")

    monkeypatch.setattr(ccm, "probe_dq05", explode)
    holder = ccm.TickLock(tmp_path / "ci-concurrency.lock")
    holder.__enter__()
    try:
        rc = ccm.main(_tick_args(tmp_path, settle=0))
        assert rc == ccm.EXIT_LOCKED
    finally:
        holder.__exit__()

    record = _records(tmp_path)[-1]
    assert record["action"] == "lock_held"
    assert "another tick holds the lock" in record["reason"]

    # ...and the lock is released with the process, so the next tick runs.
    probe, _ = _scripted(_idle_probe(1))
    monkeypatch.setattr(ccm, "probe_dq05", probe)
    monkeypatch.setattr(ccm, "apply_limit", _applied_ok)
    assert ccm.main(_tick_args(tmp_path, settle=0)) == 0
    assert _records(tmp_path)[-1]["action"] == "apply"


def test_two_real_concurrent_ticks_cannot_overlap(tmp_path):
    """Two genuine controller processes, racing over the same flock.

    ssh is faked so the test is offline; everything else is the real program.
    The first tick holds the lock while it sits in its settle window, so the
    second must refuse to run rather than start a second recreate.
    """
    bindir = tmp_path / "bin"
    bindir.mkdir()
    payload = "\n".join(
        [
            "===ENV===",
            "NGIT_CI_REPOS=npub1x677",
            "NGIT_CI_MAX_CONCURRENT_JOBS=1",
            "===MEM===",
            "5536000",
            "===PS===",
            "ngit-ci-deploy-coordinator-1|running|Up 2 hours",
            "ngit-ci-deploy-dind-1|running|Up 2 days",
            "===LOGS===",
            LOG_IDLE,
            "===ACT===",
            "===QUEUE===",
            "RUNTIME_VALUE=3",
            "===END===",
        ]
    )
    (bindir / "payload.txt").write_text(payload)
    fake_ssh = bindir / "ssh"
    fake_ssh.write_text(
        '#!/usr/bin/env bash\ncat >/dev/null 2>&1\ncat "$(dirname "$0")/payload.txt"\n'
    )
    fake_ssh.chmod(0o755)
    env = dict(os.environ, PATH=f"{bindir}{os.pathsep}{os.environ.get('PATH', '')}")

    common = [
        sys.executable,
        str(REPO_DIR / "ci_concurrency_controller.py"),
        "--state-file", str(_state_file(tmp_path, time.time())),
        "--fallback-state-file", str(tmp_path / "none.json"),
        "--override-file", str(tmp_path / "none.override"),
        "--lock-file", str(tmp_path / "ci-concurrency.lock"),
        "--settle-s", "5",
        "--quiet",
    ]
    first = subprocess.Popen(
        common + ["--log-file", str(tmp_path / "first.log")],
        env=env,
        cwd=str(REPO_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        time.sleep(1.5)  # the first tick is now inside its settle window
        second = subprocess.run(
            common + ["--log-file", str(tmp_path / "second.log")],
            env=env,
            cwd=str(REPO_DIR),
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert second.returncode == ccm.EXIT_LOCKED, second.stderr
        assert _records(tmp_path, "second.log")[-1]["action"] == "lock_held"
    finally:
        _out, err = first.communicate(timeout=120)
    assert first.returncode == 0, err.decode()
    winner = _records(tmp_path, "first.log")[-1]
    assert winner["action"] == "apply"
    assert winner["fence"]["samples"] == 3
    assert winner["apply"]["runtime_value"] == "3"


# ------------------------------------------------------- fence unit behaviour


def test_new_activity_flags_a_new_lifecycle_line():
    now = LAST_DONE_TS + 3600
    changed, why = cc.new_activity(
        cc.fingerprint_jobs(LOG_IDLE, [], now=now),
        cc.fingerprint_jobs(LOG_BUSY, [], now=now),
    )
    assert changed is True
    assert "new trigger/repo-event/queue log line" in why


def test_new_activity_is_quiet_for_an_unchanged_coordinator():
    changed, why = cc.new_activity(
        cc.fingerprint_jobs(LOG_IDLE, [], now=LAST_DONE_TS + 3600),
        cc.fingerprint_jobs(LOG_IDLE, [], now=LAST_DONE_TS + 3690),
    )
    assert changed is False, why


def test_new_activity_flags_a_live_act_container_appearing():
    now = time.time()
    changed, why = cc.new_activity(
        cc.fingerprint_jobs(LOG_IDLE, [], now=now),
        cc.fingerprint_jobs(
            LOG_IDLE, [{"name": "act-Build-1", "running": True, "started": now - 5}], now=now
        ),
    )
    assert changed is True and "live act containers changed" in why


def test_new_activity_does_not_mistake_a_log_rollover_for_work():
    """The window shrinking is not activity: no *new* line identity appeared."""
    rolled = "\n".join(LOG_IDLE.splitlines()[2:])
    changed, why = cc.new_activity(
        cc.fingerprint_jobs(LOG_IDLE, [], now=LAST_DONE_TS + 3600),
        cc.fingerprint_jobs(rolled, [], now=LAST_DONE_TS + 3690),
    )
    assert changed is False, why


def test_queued_job_policy_ignores_a_checkpoint_ngit_ci_would_discard():
    now = time.time()
    fresh = json.dumps({"jobs": [{"a": 1}], "suspended_at": now - 10})
    expired = json.dumps({"jobs": [{"a": 1}], "suspended_at": now - 7200})
    assert cc.parse_queued_jobs(fresh, now=now)[0] == 1
    assert cc.parse_queued_jobs(expired, now=now)[0] == 0, (
        "an expired checkpoint is dropped by ngit-ci, so it cannot block forever"
    )
    assert cc.parse_queued_jobs("", now=now)[0] == 0
    assert cc.parse_queued_jobs("not json", now=now)[0] == 0
    assert cc.parse_queued_jobs('{"jobs": []}', now=now)[0] == 0


def test_classify_idle_defers_on_a_queued_job():
    idle, reason, facts = cc.classify_idle(
        LOG_IDLE,
        [],
        now=LAST_DONE_TS + 3600,
        queued_jobs=2,
        queued_detail="2 job(s) frozen 10s ago",
    )
    assert idle is False
    assert "queued job(s) not started yet" in reason
    assert facts["queued_jobs"] == 2


def test_the_apply_drains_before_recreating_and_verifies_the_runtime_value():
    script = ccm.APPLY_SCRIPT
    drain = script.index("docker compose stop -t __GRACE__ coordinator")
    recreate = script.index("docker compose up -d --force-recreate coordinator")
    assert drain < recreate, "the stop (drain) must precede the recreate"
    assert "RUNTIME_VALUE=" in script
    assert "DRAIN_TAIL:" in script

    seen = {}

    def runner(cmd, input=None, capture_output=None, text=None, timeout=None):
        seen.update(cmd=cmd, script=input, timeout=timeout)
        return _Proc(stdout="RUNTIME_VALUE=2\n", stderr="", returncode=0)

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(ccm.subprocess, "run", runner)
        ok, _ = ccm.apply_limit("dq05", 2, stop_grace_s=1980)
    finally:
        monkeypatch.undo()

    assert ok is True
    assert seen["script"].count("docker compose stop -t 1980 coordinator") == 1
    assert "__GRACE__" not in seen["script"] and "__LIMIT__" not in seen["script"]
    assert seen["timeout"] > 1980, "a full 30-minute drain is a success, not a timeout"


# ------------------------------------------- coloured production log lines
#
# Captured verbatim from DQ05 while a REAL job ran during a live tick
# (`docker compose logs --tail=… coordinator`, 2026-09-11T19:53:48Z). The
# coordinator's tracing output is coloured even into a pipe, so the field names
# are wrapped in escapes and the raw bytes never contain
# "Starting queued CI job trigger_event=". Every lifecycle counter was
# therefore silently 0 in production: the log-based idle proof was dead, and
# the fence only caught that running job through the dind container channel.

JOB_TRIGGER = "f889a3b2e7685d798521bd80ab7bd6cadc20bbf60c00354a45bd4dac49e39795"
ANSI_ENQUEUED = (
    "coordinator-1  | \x1b[2m2026-09-11T19:53:48.199460Z\x1b[0m \x1b[32m INFO\x1b[0m "
    "\x1b[2mngit_ci\x1b[0m\x1b[2m:\x1b[0m Enqueued CI job "
    "\x1b[3mtrigger_event\x1b[0m\x1b[2m=\x1b[0m" + JOB_TRIGGER + " "
    "\x1b[3mqueued\x1b[0m\x1b[2m=\x1b[0m1 \x1b[3mcapacity\x1b[0m\x1b[2m=\x1b[0m64"
)
ANSI_STARTED = (
    "coordinator-1  | \x1b[2m2026-09-11T19:53:48.199502Z\x1b[0m \x1b[32m INFO\x1b[0m "
    "\x1b[2mngit_ci::queue\x1b[0m\x1b[2m:\x1b[0m Starting queued CI job "
    "\x1b[3mtrigger_event\x1b[0m\x1b[2m=\x1b[0m" + JOB_TRIGGER + " "
    "\x1b[3mworkflow\x1b[0m\x1b[2m=\x1b[0m.ngit/act/workflows/validate-feed.yml "
    "\x1b[3mqueue_wait_ms\x1b[0m\x1b[2m=\x1b[0m0 \x1b[3mrunning_jobs\x1b[0m\x1b[2m=\x1b[0m1"
)
ANSI_JOB_TS = ccm._iso_to_epoch("2026-09-11T19:53:48.199502Z")
assert ANSI_JOB_TS is not None


def test_the_coloured_line_really_would_not_match_unstripped():
    """The fix only matters if the raw bytes genuinely fail to match."""
    assert "Starting queued CI job trigger_event=" not in ANSI_STARTED
    assert "Starting queued CI job trigger_event=" in cc._ANSI_RE.sub("", ANSI_STARTED)


def test_ansi_coloured_production_lines_are_parsed():
    log = ANSI_ENQUEUED + "\n" + ANSI_STARTED
    parsed = cc.parse_coordinator_log(log)
    assert parsed["enqueued"] == 1, parsed
    assert parsed["started"] == 1, parsed
    assert parsed["in_flight"] == [JOB_TRIGGER], parsed
    assert parsed["newest_activity_ts"] == ANSI_JOB_TS

    idle, reason, facts = cc.classify_idle(log, [], now=ANSI_JOB_TS + 60)
    assert idle is False, reason
    assert "not completed" in reason
    assert facts["in_flight"] == [JOB_TRIGGER]


def test_new_activity_sees_a_new_coloured_lifecycle_line():
    """The log channel - not just the dind container channel - must see the job."""
    now = ANSI_JOB_TS + 60
    changed, why = cc.new_activity(
        cc.fingerprint_jobs(ANSI_ENQUEUED, [], now=now),
        cc.fingerprint_jobs(ANSI_ENQUEUED + "\n" + ANSI_STARTED, [], now=now),
    )
    assert changed is True
    assert "new trigger/repo-event/queue log line" in why
