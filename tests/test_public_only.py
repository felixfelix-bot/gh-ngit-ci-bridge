"""Unit tests for the fail-closed PUBLIC-ONLY gate.

Run: pytest -q tests/            (or: python3 -m unittest discover -s tests)

The four cases the task calls out explicitly are marked REQUIRED:
  (a) public -> allowed
  (b) private -> skipped, with the observed reason
  (c) API error / NOT-FOUND -> skipped, fail-closed
  (d) private -> public flip is picked up on a later tick
"""

from __future__ import annotations

import os
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bridge  # noqa: E402
import public_only  # noqa: E402
from public_only import (  # noqa: E402
    API_ERROR,
    EMPTY,
    NOT_FOUND,
    TIMEOUT,
    NotPublicError,
    assert_public,
    audit,
    audit_table,
    check,
    require_public,
    visibility_of,
)


def runner(code: int, out: str, err: str = ""):
    """A fake subprocess runner returning a fixed (rc, stdout, stderr)."""

    def _run(cmd, timeout):
        _run.cmd = list(cmd)
        _run.timeout = timeout
        return code, out, err

    return _run


class Recorder:
    """A stand-in for bridge.Log that records (level, message) pairs."""

    def __init__(self):
        self.lines: list[tuple[str, str]] = []

    def __call__(self, message: str, level: str = "INFO") -> None:
        self.lines.append((level, message))

    def text(self) -> str:
        return "\n".join(m for _, m in self.lines)


class VisibilityOfTests(unittest.TestCase):
    def test_public_value_is_returned_verbatim(self):
        r = runner(0, "public\n")
        self.assertEqual(visibility_of("OpenTollGate/tollgate-rs", runner=r), "public")
        self.assertEqual(r.cmd, ["gh", "api", "repos/OpenTollGate/tollgate-rs", "--jq", ".visibility"])

    def test_not_found_maps_to_sentinel(self):
        r = runner(1, "", "gh: Not Found (HTTP 404)")
        self.assertEqual(visibility_of("ghost/repo", runner=r), NOT_FOUND)

    def test_other_api_error_maps_to_sentinel(self):
        r = runner(1, "", "API rate limit exceeded")
        self.assertEqual(visibility_of("o/r", runner=r), API_ERROR)

    def test_timeout_maps_to_sentinel(self):
        def boom(cmd, timeout):
            raise subprocess.TimeoutExpired(cmd, timeout)

        self.assertEqual(visibility_of("o/r", runner=boom), TIMEOUT)

    def test_transport_exception_maps_to_sentinel(self):
        def boom(cmd, timeout):
            raise OSError("gh not found")

        self.assertEqual(visibility_of("o/r", runner=boom), API_ERROR)

    def test_empty_output_maps_to_sentinel(self):
        self.assertEqual(visibility_of("o/r", runner=runner(0, "  \n")), EMPTY)


class CheckTests(unittest.TestCase):
    def test_public_allowed(self):
        """REQUIRED (a): public -> allowed."""
        result = check("OpenTollGate/tollgate-rs", query=lambda s: "public")
        self.assertTrue(result.allowed)
        self.assertEqual(result.visibility, "public")
        self.assertTrue(assert_public("OpenTollGate/tollgate-rs", query=lambda s: "public"))

    def test_private_skipped_with_reason(self):
        """REQUIRED (b): private -> skipped, reason carries visibility."""
        result = check("felixfelix-bot/secret", query=lambda s: "private")
        self.assertFalse(result.allowed)
        self.assertEqual(result.visibility, "private")
        self.assertIn("private", result.reason)
        self.assertIn("fail-closed", result.reason)

    def test_internal_skipped(self):
        self.assertFalse(check("o/r", query=lambda s: "internal").allowed)

    def test_api_error_and_not_found_fail_closed(self):
        """REQUIRED (c): API error / NOT-FOUND -> skipped, fail-closed."""
        for value in (API_ERROR, NOT_FOUND, TIMEOUT, EMPTY):
            result = check("o/r", query=lambda s, v=value: v)
            self.assertFalse(result.allowed, f"{value} must be denied")
            self.assertIn(value, result.reason)

    def test_exact_match_only(self):
        """Anything that is not exactly 'public' denies (fail-closed)."""
        for value in ("Public", "PUBLIC", "public ", "garbage", "", "0"):
            self.assertFalse(
                check("o/r", query=lambda s, v=value: v).allowed,
                f"{value!r} must be denied: only exact 'public' is allowed",
            )

    def test_require_public_raises_with_visibility(self):
        with self.assertRaises(NotPublicError) as ctx:
            require_public("o/r", query=lambda s: "private")
        self.assertEqual(ctx.exception.visibility, "private")
        self.assertEqual(ctx.exception.slug, "o/r")

    def test_require_public_passes_for_public(self):
        require_public("o/r", query=lambda s: "public")  # must not raise


class AuditTests(unittest.TestCase):
    def test_audit_preserves_order_and_marks_verdicts(self):
        vis = {"a/one": "public", "b/two": "private", "c/three": NOT_FOUND}
        results = audit(["a/one", "b/two", "c/three"], query=lambda s: vis[s])
        self.assertEqual([r.slug for r in results], ["a/one", "b/two", "c/three"])
        self.assertEqual([r.allowed for r in results], [True, False, False])
        table = audit_table(results)
        self.assertIn("a/one", table)
        self.assertIn("SKIP (not public)", table)
        self.assertIn("1 public/allowed | 2 skipped", table)

    def test_audit_table_handles_empty(self):
        self.assertEqual(audit_table([]), "(no configured repos)")


class TickGateTests(unittest.TestCase):
    """REQUIRED (d) plus the no-retry / fail-closed properties of one tick."""

    def test_private_to_public_is_picked_up_on_later_tick(self):
        """REQUIRED (d): a repo that flips private -> public is picked up."""
        state = {"visibility": "private"}

        def query(slug):
            return state["visibility"]

        allowed, skipped, _ = bridge.select_public_repos(["o/r"], query=query)
        self.assertEqual(allowed, [])
        self.assertEqual([r.slug for r in skipped], ["o/r"])

        state["visibility"] = "public"  # next tick
        allowed, skipped, _ = bridge.select_public_repos(["o/r"], query=query)
        self.assertEqual(allowed, ["o/r"])
        self.assertEqual(skipped, [])

    def test_public_to_private_is_caught_on_next_tick(self):
        state = {"visibility": "public"}

        def query(slug):
            return state["visibility"]

        allowed, _, _ = bridge.select_public_repos(["o/r"], query=query)
        self.assertEqual(allowed, ["o/r"])

        state["visibility"] = "private"
        allowed, skipped, _ = bridge.select_public_repos(["o/r"], query=query)
        self.assertEqual(allowed, [])
        self.assertEqual(skipped[0].visibility, "private")

    def test_select_skips_and_logs_observed_visibility(self):
        rec = Recorder()
        vis = {"good/repo": "public", "bad/repo": "private"}
        allowed, skipped, _ = bridge.select_public_repos(
            ["good/repo", "bad/repo"], log=rec, query=lambda s: vis[s]
        )
        self.assertEqual(allowed, ["good/repo"])
        self.assertEqual([r.slug for r in skipped], ["bad/repo"])
        self.assertIn("visibility=private", rec.text())
        self.assertTrue(all(level == "WARN" for level, _ in rec.lines))

    def test_select_never_drops_the_skipped_entry_from_the_report(self):
        """Non-public config entries are reported, never silently removed."""
        results = bridge.select_public_repos(["bad/repo"], query=lambda s: "private")[2]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].slug, "bad/repo")
        self.assertFalse(results[0].allowed)


class GuardTests(unittest.TestCase):
    """The point-of-no-return guard used before pushes/publishes."""

    def test_guard_allows_public(self):
        rec = Recorder()
        with patch.object(public_only, "visibility_of", return_value="public"):
            self.assertTrue(bridge.public_only_guard("o/r", rec))
        self.assertEqual(rec.lines, [])

    def test_guard_refuses_private_and_logs(self):
        rec = Recorder()
        with patch.object(public_only, "visibility_of", return_value="private"):
            self.assertFalse(bridge.public_only_guard("o/r", rec))
        self.assertIn("visibility=private", rec.text())
        self.assertEqual(rec.lines[0][0], "WARN")

    def test_guard_refuses_not_found_fail_closed(self):
        rec = Recorder()
        with patch.object(public_only, "visibility_of", return_value=NOT_FOUND):
            self.assertFalse(bridge.public_only_guard("ghost/repo", rec))
        self.assertIn(NOT_FOUND, rec.text())


class BridgeWiringTests(unittest.TestCase):
    """Assert the gate is wired into every dangerous path in bridge.py."""

    def test_bridge_has_no_ungated_push_or_publish(self):
        source = open(os.path.join(os.path.dirname(os.path.dirname(__file__)), "bridge.py")).read()
        # every push_to_ngit / nostr.publish call must be preceded (in source
        # order) by at least as many public_only_guard calls
        pushes = source.count("mirror.push_to_ngit(")
        publishes = source.count("nostr.publish(")
        guards = source.count("public_only_guard(")
        # definition + 3 call sites (prepare, push, two publishes) => >= 5
        self.assertGreaterEqual(guards, 5)
        self.assertGreaterEqual(guards, pushes + publishes)

    def test_config_load_audits_repos(self):
        source = open(os.path.join(os.path.dirname(os.path.dirname(__file__)), "bridge.py")).read()
        self.assertIn("select_public_repos(repos", source)
        self.assertIn("PUBLIC-ONLY gate (configured repo audit)", source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
