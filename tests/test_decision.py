"""Unit tests for the bridge decision logic.

Run: pytest -q tests/test_decision.py     (or: python3 -m unittest discover -s tests)

The four cases the task calls out explicitly are marked REQUIRED.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from decision import (  # noqa: E402
    MANUAL_9840,
    MIRROR_PUSH,
    SKIP,
    Commit,
    RepoSpec,
    decide,
    watched_matches,
)

WATCHED = [
    "felixfelix-bot",
    "c03rad0r",
    "Amperstrand",
    "Origami74",
    "Franchovy",
    "maximotodev",
    "hkarani",
]
EMAIL_MAP = {"felix@embedsmart.de": "felixfelix-bot"}


def commit(**kw) -> Commit:
    base = dict(sha="a" * 40, branch="main", message="feat: x")
    base.update(kw)
    return Commit(**base)


def repo(**kw) -> RepoSpec:
    base = dict(
        slug="OpenTollGate/tollgate-module-basic-go",
        ngit_repo_id="tollgate-module-basic-go",
        ngit_owner_hex="36bdeb" + "0" * 58,
        workflows=(".ngit/act/workflows/test.yml",),
    )
    base.update(kw)
    return RepoSpec(**base)


class RequiredCases(unittest.TestCase):
    def test_new_sha_and_watched_identity_triggers(self):
        """REQUIRED: new SHA + watched identity -> trigger."""
        decision = decide(
            commit(author_login="felixfelix-bot"),
            repo(),
            seen_shas=[],
            watched=WATCHED,
            email_map=EMAIL_MAP,
        )
        self.assertEqual(decision.action, MIRROR_PUSH)
        self.assertEqual(decision.code, "new_commit_watched_identity")
        self.assertTrue(decision.is_trigger)

    def test_unwatched_identity_does_not_trigger(self):
        """REQUIRED: unwatched identity -> no trigger."""
        decision = decide(
            commit(author_login="random-contributor", author_email="rand@example.com"),
            repo(),
            seen_shas=[],
            watched=WATCHED,
            email_map=EMAIL_MAP,
        )
        self.assertEqual(decision.action, SKIP)
        self.assertEqual(decision.code, "unwatched_identity")
        self.assertFalse(decision.is_trigger)

    def test_already_seen_sha_does_not_trigger(self):
        """REQUIRED: already-seen SHA -> no trigger (dedupe)."""
        sha = "b" * 40
        decision = decide(
            commit(sha=sha, author_login="c03rad0r"),
            repo(),
            seen_shas=[sha],
            watched=WATCHED,
            email_map=EMAIL_MAP,
        )
        self.assertEqual(decision.action, SKIP)
        self.assertEqual(decision.code, "already_seen_sha")

    def test_missing_mirror_skips_with_reason(self):
        """REQUIRED: missing ngit mirror -> skip with a reason."""
        decision = decide(
            commit(author_login="Amperstrand"),
            repo(slug="Amperstrand/bolty-rs", ngit_repo_id=None, ngit_owner_hex=None),
            seen_shas=[],
            watched=WATCHED,
            email_map=EMAIL_MAP,
        )
        self.assertEqual(decision.action, SKIP)
        self.assertEqual(decision.code, "no_ngit_mirror")
        self.assertIn("30617", decision.detail)


class IdentityResolution(unittest.TestCase):
    def test_email_map_resolves_to_watched_login(self):
        self.assertEqual(
            watched_matches(commit(author_email="felix@embedsmart.de"), WATCHED, EMAIL_MAP),
            {"felixfelix-bot"},
        )

    def test_github_noreply_email_encodes_login(self):
        self.assertEqual(
            watched_matches(
                commit(author_email="141745238+Amperstrand@users.noreply.github.com"),
                WATCHED,
                EMAIL_MAP,
            ),
            {"amperstrand"},
        )

    def test_push_actor_counts_even_when_author_is_unknown(self):
        decision = decide(
            commit(author_login="someone-else", actor_login="c03rad0r"),
            repo(),
            seen_shas=[],
            watched=WATCHED,
            email_map=EMAIL_MAP,
        )
        self.assertEqual(decision.action, MIRROR_PUSH)

    def test_identity_match_is_case_insensitive(self):
        self.assertTrue(watched_matches(commit(author_login="C03RAD0R"), WATCHED, EMAIL_MAP))


class FiltersAndFallbacks(unittest.TestCase):
    def test_unwatched_branch_is_skipped(self):
        decision = decide(
            commit(branch="release-notes", author_login="felixfelix-bot"),
            repo(slug="x/y"),
            seen_shas=[],
            watched=WATCHED,
            email_map=EMAIL_MAP,
            branch_allow=("main", "master"),
        )
        self.assertEqual(decision.code, "branch_not_watched")

    def test_empty_sha_is_skipped(self):
        decision = decide(commit(sha=""), repo(), [], WATCHED, EMAIL_MAP)
        self.assertEqual(decision.code, "missing_sha")

    def test_mirror_already_current_with_workflows_falls_back_to_manual(self):
        decision = decide(
            commit(author_login="felixfelix-bot"),
            repo(),
            seen_shas=[],
            watched=WATCHED,
            email_map=EMAIL_MAP,
            mirror_already_has_sha=True,
        )
        self.assertEqual(decision.action, MANUAL_9840)
        self.assertEqual(decision.code, "mirror_noop_manual_fallback")
        self.assertEqual(decision.workflows, (".ngit/act/workflows/test.yml",))

    def test_mirror_already_current_without_workflows_skips(self):
        decision = decide(
            commit(author_login="felixfelix-bot"),
            repo(workflows=()),
            seen_shas=[],
            watched=WATCHED,
            email_map=EMAIL_MAP,
            mirror_already_has_sha=True,
        )
        self.assertEqual(decision.action, SKIP)
        self.assertEqual(decision.code, "mirror_already_current_no_workflows")

    def test_manual_mode_without_workflows_skips_with_reason(self):
        decision = decide(
            commit(author_login="felixfelix-bot"),
            repo(workflows=()),
            seen_shas=[],
            watched=WATCHED,
            email_map=EMAIL_MAP,
            trigger_mode="manual",
        )
        self.assertEqual(decision.code, "no_workflows_at_commit")

    def test_manual_mode_with_workflows_triggers_manual(self):
        decision = decide(
            commit(author_login="felixfelix-bot"),
            repo(),
            seen_shas=[],
            watched=WATCHED,
            email_map=EMAIL_MAP,
            trigger_mode="manual",
        )
        self.assertEqual(decision.action, MANUAL_9840)
        self.assertEqual(decision.code, "manual_trigger_requested")

    def test_dedupe_beats_identity_check(self):
        """A replay of an already-handled sha reports the dedupe reason, not identity."""
        sha = "c" * 40
        decision = decide(
            commit(sha=sha, author_login="not-watched"),
            repo(),
            seen_shas=[sha],
            watched=WATCHED,
            email_map=EMAIL_MAP,
        )
        self.assertEqual(decision.code, "already_seen_sha")


if __name__ == "__main__":
    unittest.main(verbosity=2)
