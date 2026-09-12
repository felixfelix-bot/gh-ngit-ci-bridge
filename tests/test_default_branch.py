"""Regression tests for per-repo branch selection in the bridge tick.

Run: pytest -q tests/test_default_branch.py
     (or: python3 -m unittest discover -s tests)

Before the fix the tick probed *every* branch in the config `branches` list for
*every* repo, so a repo whose real default branch is `master` was also probed on
`main` (and vice versa) and GitHub answered 404 once per tick, once per repo:

    felixfelix-bot/market@main: cannot list commits (gh: Not Found (HTTP 404))
    felixfelix-bot/microfips@master: cannot list commits (gh: Not Found (HTTP 404))

The tick is driven end-to-end through ``bridge.main()`` in --dry-run against a
fake GitHub API, so the assertion is on the branches the bridge actually probes,
not on a helper's internals. ``test_default_branch_is_probed_instead_of_the_allow_list``
is the REQUIRED regression: it fails on the unfixed bridge and passes after it.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bridge  # noqa: E402

# market's default branch is master, microfips' is main: the two orientations
# that make the unfixed bridge 404 on half of its probes.
DEFAULTS = {"felixfelix-bot/market": "master", "felixfelix-bot/microfips": "main"}
EXISTING = {
    "felixfelix-bot/market": {"master"},
    "felixfelix-bot/microfips": {"main"},
}


class FakeGithub:
    """Stands in for bridge.Github: records probes, 404s on unknown branches."""

    def __init__(self, log, defaults, existing):
        self.log = log
        self.defaults = dict(defaults)
        self.existing = {slug: set(branches) for slug, branches in existing.items()}
        self.default_calls: list[str] = []
        self.probed: list[tuple[str, str]] = []

    def default_branch(self, slug: str):
        self.default_calls.append(slug)
        return self.defaults.get(slug)

    def commits(self, slug: str, branch: str, limit: int = 20):
        self.probed.append((slug, branch))
        if branch not in self.existing.get(slug, set()):
            raise RuntimeError(
                f"gh api repos/{slug}/commits?sha={branch}&per_page={limit} "
                "failed rc=1: gh: Not Found (HTTP 404)"
            )
        return []

    def push_events(self, slug: str, limit: int = 30):
        return []

    def org_repos(self, org: str):
        return []


class FakeNostr:
    def __init__(self, cfg, log):
        self.cfg = cfg
        self.log = log

    def announce_map(self):
        return {}

    def latest_state_event(self, repo_id, owner_hex):
        return None

    def publish(self, *args, **kwargs):
        return "event-id"


class TickHarness(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def tick(self, repos, *, defaults=DEFAULTS, existing=EXISTING, state=None,
             branch_allow=("main", "master"), repo_cfg=None):
        """Run one --dry-run tick; returns (FakeGithub, rc, log text)."""
        state_dir = self.tmp / "state"
        state_dir.mkdir(exist_ok=True)
        if state is not None:
            (state_dir / "state.json").write_text(json.dumps(state))

        cfg = {
            "state_dir": str(state_dir),
            "log_file": str(state_dir / "bridge.log"),
            "cache_dir": str(self.tmp / "cache"),
            "coordinator_hex": "a" * 64,
            "ngit_owner_hex": "b" * 64,
            "relays": ["wss://relay.invalid"],
            "watched_identities": ["felixfelix-bot"],
            "email_to_login": {},
            "branches": list(branch_allow),
            "trigger_mode": "mirror",
            "commits_per_poll": 20,
            "mirror_refresh_minutes": 60,
            "orgs": [],
            "repos": {slug: (repo_cfg or {}).get(slug, {}) for slug in repos},
        }
        cfg_path = self.tmp / "config.json"
        cfg_path.write_text(json.dumps(cfg))

        holder: dict = {}

        def fake_github_class(log):
            fake = FakeGithub(log, defaults, existing)
            holder["gh"] = fake
            return fake

        def fake_gate(repos_, log=None, query=None):
            return list(repos_), [], []

        buffer = io.StringIO()
        with patch.object(bridge, "Github", fake_github_class), \
                patch.object(bridge, "Nostr", FakeNostr), \
                patch.object(bridge, "select_public_repos", fake_gate), \
                contextlib.redirect_stdout(buffer):
            rc = bridge.main(["--config", str(cfg_path), "--dry-run"])

        log_text = (state_dir / "bridge.log").read_text()
        holder["gh"].stdout = buffer.getvalue()
        return holder["gh"], rc, log_text


class BranchSelection(TickHarness):
    def test_default_branch_is_probed_instead_of_the_allow_list(self):
        """REQUIRED: each repo is probed on its real default branch only."""
        gh, rc, log_text = self.tick(["felixfelix-bot/market", "felixfelix-bot/microfips"])

        self.assertEqual(rc, bridge.EXIT_OK)
        self.assertEqual(
            gh.probed,
            [("felixfelix-bot/market", "master"), ("felixfelix-bot/microfips", "main")],
            "the unfixed bridge probes main and master for both repos",
        )
        self.assertNotIn("cannot list commits", log_text)
        self.assertNotIn("404", log_text)

    def test_default_branch_is_resolved_once_per_slug_per_tick(self):
        gh, rc, _ = self.tick(["felixfelix-bot/market", "felixfelix-bot/microfips"])

        self.assertEqual(rc, bridge.EXIT_OK)
        self.assertEqual(
            gh.default_calls, ["felixfelix-bot/market", "felixfelix-bot/microfips"]
        )

    def test_explicit_per_repo_branches_override_wins(self):
        """An explicit repos.<slug>.branches list is probed, no lookup needed."""
        gh, rc, _ = self.tick(
            ["felixfelix-bot/market"],
            repo_cfg={"felixfelix-bot/market": {"branches": ["release"]}},
        )

        self.assertEqual(rc, bridge.EXIT_OK)
        self.assertEqual(gh.probed, [("felixfelix-bot/market", "release")])
        self.assertEqual(gh.default_calls, [])

    def test_lookup_failure_falls_back_to_the_allow_list(self):
        """A failed/empty lookup keeps the old behaviour, and a 404 stays non-fatal."""
        gh, rc, log_text = self.tick(
            ["felixfelix-bot/market"],
            defaults={"felixfelix-bot/market": None},
        )

        self.assertEqual(rc, bridge.EXIT_OK, "a missing branch must not fail the tick")
        self.assertEqual(
            gh.probed,
            [("felixfelix-bot/market", "main"), ("felixfelix-bot/market", "master")],
        )
        self.assertIn("cannot list commits", log_text)

    def test_tracked_non_default_branch_is_still_probed(self):
        """A branch the bridge already mirrors is never dropped by the fix.

        ``felixfelix-bot/hermes-agent`` is the live shape of this: its default
        branch is ``nostr-adapter``, but ``main`` exists, is tracked in
        state.json and is mirrored today. Probing the default must not drop it.
        """
        gh, rc, _ = self.tick(
            ["felixfelix-bot/hermes-agent"],
            defaults={"felixfelix-bot/hermes-agent": "nostr-adapter"},
            existing={"felixfelix-bot/hermes-agent": {"main", "nostr-adapter"}},
            state={
                "version": 1,
                "seen": {},
                "failures": {},
                "heads": {"felixfelix-bot/hermes-agent@main": "c" * 40},
                "mirror_map": {},
                "mirror_map_refreshed_at": 0,
                "org_repo_cache": {},
                "counters": {"ticks": 0, "decisions": {}, "triggers": 0},
            },
        )

        self.assertEqual(rc, bridge.EXIT_OK)
        self.assertEqual(
            gh.probed,
            [
                ("felixfelix-bot/hermes-agent", "nostr-adapter"),
                ("felixfelix-bot/hermes-agent", "main"),
            ],
        )
        self.assertNotIn("cannot list commits", (self.tmp / "state" / "bridge.log").read_text())


if __name__ == "__main__":
    unittest.main()
