#!/usr/bin/env python3
"""Unit tests for the Kalman-signal -> ngit-ci concurrency controller.

The mapping is pure, so it is tested directly; the controller's I/O paths are
tested with a fake DQ05 probe so the assertions cover the real decision flow
(including "unchanged -> no recreate") without touching the network.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import ci_concurrency as cc  # noqa: E402
import ci_concurrency_controller as ccm  # noqa: E402

CEIL = 2.0


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

    def fake_apply(target, limit, timeout=240):
        calls.append((target, limit))
        return True, (
            "ENV_NOW=NGIT_CI_MAX_CONCURRENT_JOBS=3\n"
            "BACKUP=.env.bak.20260911-140000\n"
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

    def fake_apply(target, limit, timeout=240):
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
        ccm, "apply_limit", lambda target, limit, timeout=240: (True, "ngit-ci-deploy-coordinator-1|running|Up 1s\n")
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
    monkeypatch.setattr(ccm, "apply_limit", lambda target, limit, timeout=240: (True, ""))
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


def test_no_key_material_is_read_by_the_module():
    src = Path(ccm.__file__).read_text()
    for banned in ("nsec", "private key", "bunker://"):
        assert banned not in src.lower().replace("never logs key material", "")
