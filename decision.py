"""Pure decision logic for the GitHub -> ngit CI bridge.

No I/O, no network, no subprocess: everything here is a deterministic function
of its arguments so the trigger decision can be unit-tested in isolation.

Decision vocabulary
-------------------
action == "mirror_push"  : publish the new commit to the repo's ngit mirror
                           (git-remote-nostr emits a kind-30618 repository-state
                           event; that is what the ngit-ci coordinator consumes).
action == "manual_9840"  : publish a kind-9840 Manual Trigger for a workflow file
                           that exists at the commit (used when a repo-state
                           change would be a no-op, or when explicitly requested).
action == "skip"         : no trigger, with a machine-readable reason code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping, Sequence

SKIP = "skip"
MIRROR_PUSH = "mirror_push"
MANUAL_9840 = "manual_9840"

# ---------------------------------------------------------------- data shapes


@dataclass(frozen=True)
class Commit:
    """One candidate commit as observed on GitHub."""

    sha: str
    branch: str
    message: str = ""
    author_login: str | None = None
    author_email: str | None = None
    #: login that performed the push (from /repos/{o}/{r}/events PushEvent),
    #: which can differ from the commit author (e.g. a maintainer pushing a
    #: contributor's patch).
    actor_login: str | None = None


@dataclass(frozen=True)
class RepoSpec:
    """What the bridge knows about one GitHub repository."""

    slug: str  # "owner/name"
    #: ngit repo-id (the `d` tag of the kind-30617 announcement) or None when no
    #: mirror exists. Repo *names* usually match the GitHub name, which is how
    #: auto-resolution by `d` tag works.
    ngit_repo_id: str | None = None
    #: hex pubkey that owns the ngit repo coordinate (maintainer).
    ngit_owner_hex: str | None = None
    #: `.ngit/act/workflows/*.yml` paths present at the candidate commit.
    workflows: Sequence[str] = field(default_factory=tuple)

    @property
    def has_mirror(self) -> bool:
        return bool(self.ngit_repo_id and self.ngit_owner_hex)


@dataclass(frozen=True)
class Decision:
    action: str
    code: str
    detail: str
    workflows: Sequence[str] = field(default_factory=tuple)

    @property
    def is_trigger(self) -> bool:
        return self.action in (MIRROR_PUSH, MANUAL_9840)

    def as_log_line(self, sha: str) -> str:
        head = f"sha={sha[:12]} action={self.action} reason={self.code}"
        if self.detail:
            head += f" detail={self.detail}"
        if self.workflows:
            head += f" workflows={len(self.workflows)}"
        return head


# ------------------------------------------------------------- identity rules


def candidate_identities(commit: Commit, email_map: Mapping[str, str]) -> set[str]:
    """Every GitHub login this commit could be attributed to.

    Sources, in the order GitHub itself exposes them:
      1. the commit author's resolved GitHub login,
      2. the pusher (push-event actor),
      3. an explicit email -> login mapping from config,
      4. GitHub noreply emails, which encode the login
         (`12345+login@users.noreply.github.com`).
    """
    found: set[str] = set()

    for value in (commit.author_login, commit.actor_login):
        if value:
            found.add(value.strip().lower())

    email = (commit.author_email or "").strip().lower()
    if email:
        if email in {k.lower() for k in email_map}:
            for key, login in email_map.items():
                if key.lower() == email:
                    found.add(login.strip().lower())
        if email.endswith("@users.noreply.github.com"):
            local = email.split("@", 1)[0]
            found.add(local.split("+")[-1].strip().lower())

    return found


def watched_matches(
    commit: Commit,
    watched: Iterable[str],
    email_map: Mapping[str, str],
) -> set[str]:
    """Intersection of this commit's identities with the watched set."""
    wanted = {w.strip().lower() for w in watched if w and w.strip()}
    return candidate_identities(commit, email_map) & wanted


# ------------------------------------------------------------------- decision


def decide(
    commit: Commit,
    repo: RepoSpec,
    seen_shas: Iterable[str],
    watched: Iterable[str],
    email_map: Mapping[str, str] | None = None,
    branch_allow: Sequence[str] = ("main", "master"),
    mirror_already_has_sha: bool = False,
    trigger_mode: str = "mirror",
) -> Decision:
    """Classify one commit.

    Order matters and is deliberate: cheap, unambiguous vetoes first, so the
    reason a commit did *not* trigger is always the most specific one
    ("already_seen_sha" beats "unwatched_identity" for a replay of a commit that
    was already handled).
    """
    email_map = email_map or {}

    if not commit.sha:
        return Decision(SKIP, "missing_sha", "commit has no sha")

    if commit.sha in set(seen_shas):
        return Decision(SKIP, "already_seen_sha", "sha already handled in an earlier tick")

    if branch_allow and commit.branch not in set(branch_allow):
        return Decision(
            SKIP, "branch_not_watched", f"branch {commit.branch!r} not in {list(branch_allow)}"
        )

    matches = watched_matches(commit, watched, email_map)
    if not matches:
        who = commit.author_login or commit.actor_login or commit.author_email or "unknown"
        return Decision(SKIP, "unwatched_identity", f"identity {who!r} is not watched")

    if not repo.has_mirror:
        return Decision(
            SKIP,
            "no_ngit_mirror",
            f"{repo.slug} has no kind-30617 ngit announcement; cannot publish repo state",
        )

    who = ",".join(sorted(matches))

    if trigger_mode == "manual" or mirror_already_has_sha:
        # A repo-state push would be a no-op (mirror already carries the sha) or
        # the operator asked for manual replays. Only a 9840 can still produce a
        # run, and only for workflow files that exist at the commit.
        if not repo.workflows:
            code = (
                "mirror_already_current_no_workflows"
                if mirror_already_has_sha
                else "no_workflows_at_commit"
            )
            return Decision(SKIP, code, "no .ngit/act/workflows/*.yml at commit to replay")
        return Decision(
            MANUAL_9840,
            "manual_trigger_requested" if trigger_mode == "manual" else "mirror_noop_manual_fallback",
            f"identity {who}",
            tuple(repo.workflows),
        )

    detail = f"identity {who}"
    if repo.workflows:
        detail += f"; {len(repo.workflows)} workflow file(s) at commit"
    return Decision(MIRROR_PUSH, "new_commit_watched_identity", detail, tuple(repo.workflows))
