#!/usr/bin/env python3
"""Fail-closed "public only" gate for GitHub repositories.

Nostr relays are **public and permanent**. A ref mirrored to an ngit mirror, or
a kind-30617 repository announcement, is readable by anyone forever and cannot
be revoked. Mirroring a *private* GitHub repository would therefore publish
private source code irreversibly. The operator's rule, verbatim:

    "Please make sure only the public repos get mirrored. Private repos don't
     belong on ngit."

This module is the single, reusable implementation of that rule. It is
**fail closed**: the *only* value that lets a repository through is GitHub
itself reporting `visibility == "public"`. Everything else --- `private`,
`internal`, an unknown/empty value, a NOT-FOUND, a timeout, or any API error ---
denies. There is deliberately no allow-list override and no caching: the answer
must come from GitHub at the moment of the decision, so a repo that flips
private is caught on the very next tick.

Public API
----------
    visibility_of("owner/name")        -> observed string ("public", "private",
                                          "internal", "NOT-FOUND", "TIMEOUT",
                                          "API-ERROR", "EMPTY")
    check("owner/name")                -> GateResult(slug, visibility, allowed, reason)
    assert_public("owner/name")        -> bool
    require_public("owner/name")       -> None (raises NotPublicError)
    audit([...])                       -> list[GateResult]
    audit_table(results)               -> str (fixed-width report)

The module never prints and never logs on its own: callers decide how to report
the observed visibility value.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

#: sentinel values returned by visibility_of() when GitHub does not give a
#: usable answer. All of them are, by construction, not "public".
NOT_FOUND = "NOT-FOUND"
API_ERROR = "API-ERROR"
TIMEOUT = "TIMEOUT"
EMPTY = "EMPTY"

#: the one and only value that permits a mirror/announcement.
PUBLIC = "public"

#: a runner takes (argv, timeout) and returns (returncode, stdout, stderr).
Runner = Callable[[Sequence[str], int], "tuple[int, str, str]"]


def _default_runner(cmd: Sequence[str], timeout: int) -> tuple[int, str, str]:
    proc = subprocess.run(
        list(cmd),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


def visibility_of(
    owner_repo: str,
    runner: Runner | None = None,
    timeout: int = 30,
) -> str:
    """Return the observed GitHub visibility of ``owner/name``.

    Never raises. Anything other than a clean, non-empty API answer is mapped to
    a sentinel that the gate treats as "not public":

    * gh exits non-zero with a not-found/404 -> ``NOT-FOUND``
    * gh exits non-zero for any other reason -> ``API-ERROR``
    * the runner times out                   -> ``TIMEOUT``
    * gh exits 0 but prints nothing          -> ``EMPTY`
    """
    run = runner or _default_runner
    cmd = ["gh", "api", f"repos/{owner_repo}", "--jq", ".visibility"]
    try:
        code, out, err = run(cmd, timeout)
    except subprocess.TimeoutExpired:
        return TIMEOUT
    except Exception:  # noqa: BLE001 - any transport failure must deny
        return API_ERROR

    if code != 0:
        blob = f"{err}\n{out}".lower()
        if "not found" in blob or "404" in blob or "could not resolve" in blob:
            return NOT_FOUND
        return API_ERROR

    value = (out or "").strip()
    return value if value else EMPTY


@dataclass(frozen=True)
class GateResult:
    """Outcome of the public-only gate for one repository."""

    slug: str
    visibility: str
    allowed: bool
    reason: str

    def __str__(self) -> str:  # pragma: no cover - convenience only
        verdict = "allow" if self.allowed else "SKIP"
        return f"{self.slug}: visibility={self.visibility} -> {verdict} ({self.reason})"


def check(owner_repo: str, query: Callable[[str], str] | None = None) -> GateResult:
    """Gate one repository. ``query`` is injectable for tests."""
    visibility = (query or visibility_of)(owner_repo)
    allowed = visibility == PUBLIC
    if allowed:
        reason = "github reports visibility=public"
    else:
        reason = (
            f"github reports visibility={visibility!r}, not 'public'; "
            "refused by the fail-closed public-only gate"
        )
    return GateResult(owner_repo, visibility, allowed, reason)


def assert_public(owner_repo: str, query: Callable[[str], str] | None = None) -> bool:
    """True only when GitHub reports the repo as exactly ``public``."""
    return check(owner_repo, query=query).allowed


class NotPublicError(RuntimeError):
    """Raised by require_public() when a repository fails the gate."""

    def __init__(self, result: GateResult):
        super().__init__(f"{result.slug}: {result.reason}")
        self.result = result
        self.slug = result.slug
        self.visibility = result.visibility


def require_public(owner_repo: str, query: Callable[[str], str] | None = None) -> None:
    """Raise :class:`NotPublicError` unless GitHub reports the repo public."""
    result = check(owner_repo, query=query)
    if not result.allowed:
        raise NotPublicError(result)


def audit(
    owner_repos: Iterable[str],
    query: Callable[[str], str] | None = None,
) -> list[GateResult]:
    """Gate every repository, preserving order and duplicates."""
    return [check(slug, query=query) for slug in owner_repos]


def audit_table(results: Sequence[GateResult]) -> str:
    """Fixed-width ``repo -> visibility -> verdict`` table for the tick log."""
    if not results:
        return "(no configured repos)"
    width = max(len(r.slug) for r in results)
    vwidth = max(len(r.visibility) for r in results)
    header = f"{'repo':<{width}}  {'visibility':<{vwidth}}  verdict"
    lines = [header, "-" * len(header)]
    for result in results:
        verdict = "allow" if result.allowed else "SKIP (not public)"
        lines.append(f"{result.slug:<{width}}  {result.visibility:<{vwidth}}  {verdict}")
    allowed = sum(1 for r in results if r.allowed)
    lines.append("")
    lines.append(f"{len(results)} repos | {allowed} public/allowed | {len(results) - allowed} skipped")
    return "\n".join(lines)
