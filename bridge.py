#!/usr/bin/env python3
"""gh-ngit-ci-bridge - make GitHub commits by watched identities trigger ngit-ci.

ngit-ci is Nostr-only: it reacts to NIP-34 repository-state events (kind 30618)
and Service Requests / Manual Triggers on relays. A GitHub push changes nothing
on Nostr, so commits authored or pushed by the watched identities are invisible
to the coordinator. This poller closes that gap:

    GitHub new commit  ->  decide (identity? new sha? mirror?)  ->  trigger

Trigger paths
  1. mirror_push  - push the ref to the repo's ngit mirror. git-remote-nostr
                    publishes the updated kind-30618 repository state, which is
                    exactly the event the coordinator consumes.
  2. manual_9840  - publish a kind-9840 Manual Trigger (NIP-C1) naming the
                    workflow file at the commit; used when a repo-state change
                    would be a no-op or when manual replay is requested.

Design notes
  * state (last-seen SHAs, mirror map, org repo cache) lives OUTSIDE the repo,
    in ~/.local/state/gh-ngit-ci-bridge/state.json
  * --dry-run never mutates state and never publishes anything
  * a flock on the state directory serialises ticks so overlapping timers cannot
    double-fire
  * the log never contains key material: every line goes through redact()
  * every commit considered is reported with its decision and the reason
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from decision import (  # noqa: E402
    MANUAL_9840,
    MIRROR_PUSH,
    Commit,
    Decision,
    RepoSpec,
    decide,
)
from public_only import (  # noqa: E402
    GateResult,
    audit_table,
    check,
)

EXIT_OK = 0
EXIT_ERROR = 2
EXIT_LOCKED = 3
EXIT_CONFIG = 4

NSEC_RE = re.compile(r"nsec1[02-9ac-hj-np-z]{20,}")
STATE_VERSION = 1
DEFAULT_STATE_DIR = "~/.local/state/gh-ngit-ci-bridge"


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------- logging


class Log:
    """Append-only logger that redacts anything resembling a secret key."""

    def __init__(self, path: Path | None, verbose: bool = False, secrets: tuple[str, ...] = ()):
        self.path = path
        self.verbose = verbose
        self.secrets = tuple(s for s in secrets if s)
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            os.chmod(path.parent, 0o700)

    def redact(self, text: str) -> str:
        text = NSEC_RE.sub("nsec1<REDACTED>", text)
        for secret in self.secrets:
            if len(secret) > 20:
                text = text.replace(secret, "<REDACTED>")
                # pubkey material is not secret, but the key hex is
                text = text.replace(secret[:32], "<REDACTED>")
        return text

    def __call__(self, message: str, level: str = "INFO") -> None:
        line = f"{utcnow()} {level:<5} {self.redact(message)}"
        print(line, flush=True)
        if self.path:
            with self.path.open("a") as handle:
                handle.write(line + "\n")

    def debug(self, message: str) -> None:
        if self.verbose:
            self(message, "DEBUG")


# ------------------------------------------------------------------- subprocess


def run(cmd: list[str], env: dict | None = None, cwd: str | None = None, stdin: str | None = None,
        timeout: int = 300) -> tuple[int, str, str]:
    proc = subprocess.run(
        cmd,
        input=stdin,
        capture_output=True,
        text=True,
        env=env,
        cwd=cwd,
        timeout=timeout,
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


# ------------------------------------------------------------------------ state


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "version": STATE_VERSION,
            "seen": {},
            "failures": {},
            "heads": {},
            "mirror_map": {},
            "mirror_map_refreshed_at": 0,
            "org_repo_cache": {},
            "counters": {"ticks": 0, "decisions": {}, "triggers": 0},
        }
    with path.open() as handle:
        state = json.load(handle)
    state.setdefault("seen", {})
    state.setdefault("failures", {})
    state.setdefault("heads", {})
    state.setdefault("mirror_map", {})
    state.setdefault("org_repo_cache", {})
    state.setdefault("counters", {"ticks": 0, "decisions": {}, "triggers": 0})
    return state


def record_failure(state: dict, sha: str, slug: str, branch: str, detail: str, cfg: dict) -> None:
    """Remember a failed trigger without consuming the commit.

    A failed publish must not silently swallow a commit: it is retried on the
    next tick up to `max_trigger_attempts` times; after that it is recorded as
    seen so the log stops repeating a permanently broken push.
    """
    entry = state["failures"].get(sha, {"attempts": 0})
    entry["attempts"] = entry.get("attempts", 0) + 1
    entry.update({"slug": slug, "branch": branch, "at": utcnow(), "reason": detail[:200]})
    state["failures"][sha] = entry
    if entry["attempts"] >= cfg.get("max_trigger_attempts", 3):
        state["seen"][sha] = {
            "slug": slug, "branch": branch, "at": utcnow(),
            "action": "error", "reason": detail[:200], "attempts": entry["attempts"],
        }
        state["failures"].pop(sha, None)


def save_state(path: Path, state: dict) -> None:
    # keep only the most recent 5000 handled shas
    seen = state["seen"]
    if len(seen) > 5000:
        ordered = sorted(seen.items(), key=lambda kv: kv[1].get("at", ""), reverse=True)[:5000]
        state["seen"] = dict(ordered)
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
    tmp.replace(path)


# --------------------------------------------------------- public-only gate


def public_only_guard(slug: str, log: Log) -> bool:
    """Fail-closed re-check immediately before a dangerous operation.

    Returns True only when GitHub reports ``slug`` as exactly ``public``. A
    denial is a PERMANENT skip for this tick: callers must ``continue`` without
    calling record_failure(), so the repo never enters the failure/retry map and
    cannot wedge the head marker. The check is live (never cached) and repeated
    every tick, so a repo that flips from private to public is picked up later.
    """
    result = check(slug)
    if result.allowed:
        return True
    log(
        f"PUBLIC-ONLY gate: refusing {slug} (visibility={result.visibility}) - "
        f"{result.reason}",
        "WARN",
    )
    return False


def select_public_repos(
    repos: list[str],
    log: Log | None = None,
    query=None,
) -> tuple[list[str], list[GateResult], list[GateResult]]:
    """One tick's public-only gate over a repo list.

    Returns ``(allowed_slugs, skipped_results, all_results)``. Skipped repos are
    logged with their observed visibility and reported, but never removed from
    config.json. ``query`` is injectable so this is unit-testable without a
    network call.
    """
    results = [check(slug, query=query) for slug in repos]
    allowed = [r.slug for r in results if r.allowed]
    skipped = [r for r in results if not r.allowed]
    if log is not None:
        for result in skipped:
            log(
                f"PUBLIC-ONLY gate: skipping {result.slug} "
                f"(visibility={result.visibility}) - {result.reason}",
                "WARN",
            )
    return allowed, skipped, results


def resolve_branches(
    slug: str,
    spec_cfg: dict,
    branch_allow: list[str],
    gh: "Github",
    state: dict[str, Any],
    log: Log,
) -> list[str]:
    """Branches to probe for one repo on this tick.

    An explicit per-repo ``repos.<slug>.branches`` list always wins. Otherwise
    the repo's real default branch is resolved from GitHub - one API call per
    slug per tick, not one per commit - and that is what gets probed, so a repo
    whose default branch is neither ``main`` nor ``master`` no longer asks
    GitHub for a branch it does not have (a 404 and a WARN line every tick).

    ``branch_allow`` stays the fallback whenever the lookup fails or returns
    nothing, and an allow-listed branch that already carries a tracked head is
    kept: those are branches the repo genuinely has, so mirroring never loses a
    branch it was already following.
    """
    override = spec_cfg.get("branches")
    if override:
        return list(override)

    default = gh.default_branch(slug)
    if not default:
        log(
            f"{slug}: default branch unresolved, falling back to {list(branch_allow)}",
            "WARN",
        )
        return list(branch_allow)

    branches = [default]
    heads = state.get("heads", {})
    for candidate in branch_allow:
        if candidate != default and f"{slug}@{candidate}" in heads:
            branches.append(candidate)
    return branches


# ------------------------------------------------------------------- gh helpers


class Github:
    def __init__(self, log: Log):
        self.log = log

    def api(self, path: str, paginate: bool = False) -> Any:
        cmd = ["gh", "api", path]
        if paginate:
            cmd.append("--paginate")
        code, out, err = run(cmd, timeout=120)
        if code != 0:
            raise RuntimeError(f"gh api {path} failed rc={code}: {err.strip()[:200]}")
        return json.loads(out) if out.strip() else None

    def commits(self, slug: str, branch: str, limit: int = 20) -> list[dict]:
        data = self.api(f"repos/{slug}/commits?sha={branch}&per_page={limit}")
        return data or []

    def push_events(self, slug: str, limit: int = 30) -> list[dict]:
        try:
            data = self.api(f"repos/{slug}/events?per_page={limit}")
        except RuntimeError:
            return []
        return data or []

    def org_repos(self, org: str) -> list[str]:
        try:
            data = self.api(f"orgs/{org}/repos?per_page=100&type=all", paginate=True)
        except RuntimeError:
            try:
                data = self.api(f"users/{org}/repos?per_page=100", paginate=True)
            except RuntimeError:
                return []
        return [
            item["full_name"]
            for item in (data or [])
            if isinstance(item, dict) and not item.get("archived") and "full_name" in item
        ]

    def default_branch(self, slug: str) -> str | None:
        try:
            repo = self.api(f"repos/{slug}")
        except RuntimeError:
            return None
        return (repo or {}).get("default_branch")


# ---------------------------------------------------------------- ngit helpers


class Nostr:
    def __init__(self, cfg: dict, log: Log):
        self.cfg = cfg
        self.log = log
        self.relays = cfg["relays"]

    def announce_map(self) -> dict[str, dict]:
        """{repo-id: {owner_hex, clone, relays}} from kind-30617 on the relays."""
        found: dict[str, dict] = {}
        cmd = ["nak", "req", "-k", "30617", "-a", self.cfg["ngit_owner_hex"], *self.relays]
        code, out, err = run(cmd, timeout=90)
        if code != 0:
            self.log(f"30617 query failed rc={code}: {err.strip()[:160]}", "WARN")
            return found
        for line in out.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("kind") != 30617:
                continue
            repo_id = next((t[1] for t in event["tags"] if t[0] == "d"), None)
            if not repo_id:
                continue
            found[repo_id] = {
                "owner_hex": event["pubkey"],
                "clone": [t[1] for t in event["tags"] if t[0] == "clone"],
                "event_id": event["id"],
                "created_at": event["created_at"],
            }
        return found

    def latest_state_event(self, repo_id: str, owner_hex: str) -> dict | None:
        cmd = [
            "nak", "req", "-k", "30618", "-d", repo_id, "-a", owner_hex, "-l", "1",
            *self.relays,
        ]
        code, out, err = run(cmd, timeout=60)
        if code != 0:
            self.log(f"30618 query failed rc={code}: {err.strip()[:160]}", "WARN")
            return None
        events = []
        for line in out.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("kind") == 30618:
                events.append(event)
        events.sort(key=lambda e: (e.get("created_at", 0), e.get("id", "")))
        return events[-1] if events else None

    def publish(self, kind: int, content: str, tags: list[list[str]]) -> str | None:
        """Sign with the maintainer key and publish; returns the event id."""
        signer = Path(__file__).resolve().parent / "nostr_event.py"
        cmd = [self.cfg.get("signer_python", "python3"), str(signer), "-k", str(kind), "-c", content]
        for tag in tags:
            if len(tag) == 2:
                cmd += ["-t", f"{tag[0]}={tag[1]}"]
            else:
                cmd += ["-T", f"{tag[0]}=" + ";".join(tag[1:])]
        code, out, err = run(cmd, timeout=60)
        if code != 0 or not out.strip():
            self.log(f"signing failed rc={code}: {self.log.redact(err.strip()[:200])}", "ERROR")
            return None
        signed = out.strip().splitlines()[-1]

        code, out, err = run(["nak", "event", *self.relays], stdin=signed + "\n", timeout=90)
        if code != 0:
            self.log(f"publish failed rc={code}: {self.log.redact(err.strip()[:200])}", "ERROR")
            return None
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    return json.loads(line).get("id")
                except json.JSONDecodeError:
                    continue
        return None


# ------------------------------------------------------------------- git mirror


class Mirror:
    """Lazy working clones used only when a trigger actually fires."""

    def __init__(self, cfg: dict, log: Log):
        self.cfg = cfg
        self.log = log
        self.cache_dir = Path(os.path.expanduser(cfg["cache_dir"]))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self.cache_dir, 0o700)

    def _repo_dir(self, slug: str) -> Path:
        return self.cache_dir / slug.replace("/", "__")

    def prepare(self, slug: str, branch: str, sha: str) -> tuple[bool, str]:
        """Make sure `sha` exists locally; returns (ok, detail)."""
        directory = self._repo_dir(slug)
        if not (directory / ".git").exists():
            self.log(f"clone cache miss, cloning {slug} (one-off)", "INFO")
            directory.parent.mkdir(parents=True, exist_ok=True)
            code, _, err = run(
                ["git", "clone", "--quiet", f"https://github.com/{slug}.git", str(directory)],
                timeout=900,
            )
            if code != 0:
                return False, f"clone failed: {self.log.redact(err.strip()[:200])}"
        code, _, err = run(
            ["git", "-C", str(directory), "fetch", "--quiet", "origin",
             f"+refs/heads/{branch}:refs/remotes/origin/{branch}"],
            timeout=600,
        )
        if code != 0:
            return False, f"fetch failed: {self.log.redact(err.strip()[:200])}"
        code, _, _ = run(["git", "-C", str(directory), "cat-file", "-e", f"{sha}^{{commit}}"])
        if code != 0:
            return False, f"commit {sha[:12]} not present after fetch"
        return True, "ok"

    def file_sha256(self, slug: str, sha: str, path: str) -> str | None:
        code, out, _ = run(["git", "-C", str(self._repo_dir(slug)), "show", f"{sha}:{path}"], timeout=60)
        if code != 0:
            return None
        return hashlib.sha256(out.encode()).hexdigest()

    def workflows_at(self, slug: str, sha: str) -> list[str]:
        code, out, _ = run(
            ["git", "-C", str(self._repo_dir(slug)), "ls-tree", "-r", "--name-only", sha,
             "--", ".ngit/act/workflows"],
            timeout=60,
        )
        if code != 0:
            return []
        return [line.strip() for line in out.splitlines() if line.strip().endswith((".yml", ".yaml"))]

    def _write_ngit_identity(self, directory: Path) -> None:
        """Put nostr.nsec/npub into the clone's LOCAL git config.

        git-remote-nostr resolves the signing identity from the repository's own
        config, NOT from GIT_CONFIG_* environment injection (verified
        2026-09-11: with the env set it still reported the machine-global
        ngit account and refused the push as a non-maintainer). The value is
        written by editing the file directly so the key never appears in argv,
        and the config is kept at mode 600 inside a 700 cache directory.
        """
        cfg = directory / ".git" / "config"
        text = cfg.read_text()
        lines = text.splitlines()
        nsec = self._key_nsec()
        npub = self.cfg["ngit_owner_npub"]
        def set_key(lines: list[str], key: str, value: str) -> list[str]:
            for index, line in enumerate(lines):
                if line.strip().startswith(f"{key} ="):
                    lines[index] = f"\t{key} = {value}"
                    return lines
            for index, line in enumerate(lines):
                if line.strip() == "[nostr]":
                    lines.insert(index + 1, f"\t{key} = {value}")
                    return lines
            lines += ["[nostr]", f"\t{key} = {value}"]
            return lines

        lines = set_key(lines, "npub", npub)
        lines = set_key(lines, "nsec", nsec)
        cfg.write_text("\n".join(lines) + "\n")
        os.chmod(cfg, 0o600)

    def _ngit_url(self, repo_id: str) -> str:
        return f"nostr://{self.cfg['ngit_owner_npub']}/relay.ngit.dev/{repo_id}"

    def _key_nsec(self) -> str:
        key_file = Path(os.path.expanduser(self.cfg["key_file"]))
        match = NSEC_RE.search(key_file.read_text())
        if not match:
            raise RuntimeError(f"no nsec in {key_file}")
        return match.group(0)

    def configure(self, slug: str, repo_id: str) -> tuple[bool, str]:
        directory = self._repo_dir(slug)
        if not (directory / ".git").exists():
            return False, "no cache clone"
        try:
            self._write_ngit_identity(directory)
        except OSError as exc:
            return False, f"cannot write nostr identity config: {exc}"
        run(["git", "-C", str(directory), "remote", "remove", "ngit"])  # may not exist
        code, _, err = run(["git", "-C", str(directory), "remote", "add", "ngit", self._ngit_url(repo_id)])
        if code != 0:
            return False, f"cannot add ngit remote: {self.log.redact(err.strip()[:200])}"
        return True, "ok"

    def mirror_head(self, repo_id: str, branch: str) -> str | None:
        """Ground truth: what the ngit mirror actually serves over https."""
        url = f"https://relay.ngit.dev/{self.cfg['ngit_owner_npub']}/{repo_id}.git"
        code, out, _ = run(["git", "ls-remote", url, f"refs/heads/{branch}"], timeout=120)
        if code != 0:
            return None
        for line in out.splitlines():
            parts = line.split()
            if len(parts) == 2:
                return parts[0]
        return None

    def push_to_ngit(self, slug: str, repo_id: str, branch: str, sha: str) -> tuple[bool, str]:
        """Publish a branch to the ngit mirror and VERIFY it landed.

        git-remote-nostr is unreliable about its own exit status: a maintainer
        rejection prints `error refs/heads/<b> your nostr account ... isn't
        listed as a maintainer` and still exits 0, and a successful push can
        print `Error: could not update remote_ref locally` while the ref was in
        fact written. So the return value is decided by `git ls-remote` against
        the mirror, never by the push exit code.
        """
        directory = self._repo_dir(slug)
        ok, detail = self.configure(slug, repo_id)
        if not ok:
            return False, detail

        staging = f"refs/bridge-publish/{branch}"
        code, _, err = run(["git", "-C", str(directory), "update-ref", staging, sha], timeout=60)
        if code != 0:
            return False, f"cannot stage ref: {self.log.redact(err.strip()[:200])}"

        code, out, err = run(
            ["git", "-C", str(directory), "push", "ngit", f"{staging}:refs/heads/{branch}"],
            timeout=600,
        )
        transport = self.log.redact((out + "\n" + err).strip())[-400:]

        for attempt in range(3):
            head = self.mirror_head(repo_id, branch)
            if head == sha:
                return True, f"verified_mirror_head={head[:12]} transport={transport[-160:]}"
            time.sleep(self.cfg.get("mirror_verify_retry_seconds", 5) * (attempt + 1))
        head = self.mirror_head(repo_id, branch)
        return False, f"mirror head={head[:12] if head else None} != {sha[:12]}; transport={transport[-240:]}"


# ------------------------------------------------------------------------- main


def parse_args(argv: list[str]) -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="bridge GitHub commits into ngit-ci")
    parser.add_argument("--config", default=str(here / "config.json"))
    parser.add_argument("--dry-run", action="store_true", help="classify only; never publish or persist")
    parser.add_argument("--repo", action="append", default=[], help="restrict to owner/name (repeatable)")
    parser.add_argument("--sha", help="classify this specific commit sha (requires --repo)")
    parser.add_argument("--refresh-mirrors", action="store_true", help="re-query kind-30617 now")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        with open(args.config) as handle:
            cfg = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    state_dir = Path(os.path.expanduser(cfg.get("state_dir", DEFAULT_STATE_DIR)))
    state_dir.mkdir(parents=True, exist_ok=True)
    log = Log(Path(os.path.expanduser(cfg["log_file"])) if cfg.get("log_file") else None, args.verbose)

    # ---- single-flight lock (skipped for dry runs, which mutate nothing)
    lock_handle = None
    if not args.dry_run:
        lock_path = state_dir / "lock"
        lock_handle = lock_path.open("w")
        try:
            fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log("another tick holds the lock; exiting", "WARN")
            return EXIT_LOCKED

    state: dict[str, Any] = load_state(state_dir / "state.json")
    gh = Github(log)
    nostr = Nostr(cfg, log)
    mirror = Mirror(cfg, log)

    watched = cfg["watched_identities"]
    email_map = cfg.get("email_to_login", {})
    branch_allow = cfg.get("branches", ["main", "master"])
    trigger_mode = cfg.get("trigger_mode", "mirror")

    # ---- mirror map: repo-id -> owner, refreshed from kind-30617
    ttl = cfg.get("mirror_refresh_minutes", 60) * 60
    if args.refresh_mirrors or (time.time() - state.get("mirror_map_refreshed_at", 0) > ttl):
        log("refreshing ngit mirror map from kind-30617 announcements", "INFO")
        state["mirror_map"] = nostr.announce_map()
        state["mirror_map_refreshed_at"] = int(time.time())
        log(f"mirror map: {len(state['mirror_map'])} repos announced on ngit")

    # ---- repo list: explicit + org expansion
    repos: list[str] = list(cfg.get("repos", {}).keys())
    for org in cfg.get("orgs", []):
        cache = state["org_repo_cache"].get(org, {})
        if time.time() - cache.get("fetched_at", 0) > cfg.get("org_cache_minutes", 360) * 60:
            found = gh.org_repos(org)
            state["org_repo_cache"][org] = {"fetched_at": int(time.time()), "repos": found}
            log(f"org {org}: {len(found)} repos")
        repos += state["org_repo_cache"][org]["repos"]
    repos = sorted(dict.fromkeys(repos))

    # ---- PUBLIC-ONLY gate (fail-closed), applied at config load / tick start.
    # GitHub repos that are not exactly `public` are never mirrored to nostr:
    # nostr relays are public and permanent, so a private repo would leak
    # irreversibly. Non-public entries are REPORTED and skipped for this tick;
    # they are never silently removed from config.json, and because the gate is
    # re-evaluated on every tick a repo that flips from private to public is
    # picked up on a later tick.
    allowed, gate_skipped, gate_results = select_public_repos(repos, log=log)
    print("PUBLIC-ONLY gate (configured repo audit):", flush=True)
    print(audit_table(gate_results), flush=True)
    if gate_skipped:
        log(
            f"PUBLIC-ONLY gate: {len(gate_skipped)} of {len(gate_results)} repos refused "
            "(kept in config.json, reported as skipped, not retried as failures): "
            + ", ".join(f"{r.slug}={r.visibility}" for r in gate_skipped),
            "WARN",
        )
    repos = allowed

    if args.repo:
        wanted = {r.lower() for r in args.repo}
        repos = [r for r in repos if r.lower() in wanted]
        if not repos:
            # explicitly requested repos that are outside the configured set are
            # gated too, so --repo cannot smuggle in a private repository
            repos, extra_skipped, extra_results = select_public_repos(list(args.repo), log=log)
            if extra_skipped:
                print(audit_table(extra_results), flush=True)

    log(f"tick start: {len(repos)} repos, dry_run={args.dry_run}, trigger_mode={trigger_mode}")

    decisions: list[tuple[str, str, Decision]] = []
    triggers_fired = 0

    for slug in repos:
        spec_cfg = cfg.get("repos", {}).get(slug, {})
        branches = resolve_branches(slug, spec_cfg, branch_allow, gh, state, log)
        repo_id = spec_cfg.get("ngit_repo_id")
        owner_hex = spec_cfg.get("ngit_owner_hex", cfg["ngit_owner_hex"])

        if repo_id is None:
            # auto-resolve: the ngit repo-id is normally the GitHub repo name
            guessed = slug.split("/")[-1]
            if guessed in state["mirror_map"]:
                repo_id = guessed
                owner_hex = state["mirror_map"][guessed]["owner_hex"]
            elif guessed.lower() in state["mirror_map"]:
                repo_id = guessed.lower()
                owner_hex = state["mirror_map"][guessed.lower()]["owner_hex"]

        for branch in branches:
            if args.sha:
                raw = gh.api(f"repos/{slug}/commits/{args.sha}")
                if not raw:
                    continue
                commit_rows = [raw]
            else:
                try:
                    commit_rows = gh.commits(slug, branch, cfg.get("commits_per_poll", 20))
                except RuntimeError as exc:
                    # branch does not exist, repo private/renamed, API hiccup
                    log(f"{slug}@{branch}: cannot list commits ({str(exc)[:120]})", "WARN")
                    continue
                if not commit_rows:
                    continue

            head_key = f"{slug}@{branch}"
            known_head = state["heads"].get(head_key)
            fresh: list[dict] = []
            for row in commit_rows:
                sha = row.get("sha")
                if not sha:
                    continue
                if sha == known_head:
                    break
                if sha in state["seen"] and known_head is None:
                    break
                fresh.append(row)
                if known_head is None and not cfg.get("baseline_on_first_run", True):
                    break

            if args.sha:
                fresh = commit_rows
            fresh = list(reversed(fresh))  # oldest first

            events_by_sha: dict[str, str] = {}
            if fresh:
                for event in gh.push_events(slug):
                    if event.get("type") == "PushEvent":
                        head = (event.get("payload") or {}).get("head")
                        actor = (event.get("actor") or {}).get("login")
                        if head and actor:
                            events_by_sha.setdefault(head, actor)

            for row in fresh:
                sha = row["sha"]
                commit = Commit(
                    sha=sha,
                    branch=branch,
                    message=(row.get("commit", {}).get("message") or "").splitlines()[0],
                    author_login=(row.get("author") or {}).get("login"),
                    author_email=(row.get("commit", {}).get("author") or {}).get("email"),
                    actor_login=events_by_sha.get(sha),
                )
                # --sha means "classify this commit as if it were new": never
                # treat it as a first-run baseline.
                is_baseline = (
                    not args.sha
                    and known_head is None
                    and state["heads"].get(head_key) is None
                )

                if is_baseline and cfg.get("baseline_on_first_run", True):
                    decision = Decision("skip", "baseline_first_run",
                                        "first sight of this repo@branch: recording head without triggering")
                    state["seen"][sha] = {"slug": slug, "branch": branch, "at": utcnow(),
                                          "action": "baseline"}
                    decisions.append((slug, branch, decision))
                    log(f"{slug}@{branch} {decision.as_log_line(sha)} msg={commit.message[:60]!r}")
                    continue

                # Manual replay (or a mirror that already carries the sha) is the
                # only path that needs the workflow list at the commit, and that
                # requires the commit locally -> prepare the cache clone first.
                workflows: tuple[str, ...] = ()
                needs_manual = trigger_mode == "manual" or state["seen"].get(sha, {}).get("mirror_sha") == sha
                if needs_manual and bool(repo_id and owner_hex) and not args.dry_run:
                    prepared, prep_detail = mirror.prepare(slug, branch, sha)
                    if prepared:
                        workflows = tuple(mirror.workflows_at(slug, sha))
                    else:
                        log(f"{slug}@{branch} {sha[:12]} prepare failed: {prep_detail}", "WARN")
                elif needs_manual and bool(repo_id and owner_hex) and args.dry_run:
                    workflows = tuple(cfg.get("workflow_probe", {}).get(slug, ()))

                spec = RepoSpec(
                    slug=slug,
                    ngit_repo_id=repo_id,
                    ngit_owner_hex=owner_hex,
                    workflows=workflows,
                )

                decision = decide(
                    commit,
                    spec,
                    seen_shas=state["seen"].keys(),
                    watched=watched,
                    email_map=email_map,
                    branch_allow=branch_allow,
                    mirror_already_has_sha=state["seen"].get(sha, {}).get("mirror_sha") == sha,
                    trigger_mode=trigger_mode,
                )
                decisions.append((slug, branch, decision))
                log(f"{slug}@{branch} {decision.as_log_line(sha)} msg={commit.message[:60]!r}")

                if not decision.is_trigger:
                    if not args.dry_run:
                        state["seen"][sha] = {"slug": slug, "branch": branch, "at": utcnow(),
                                              "action": decision.action, "reason": decision.code}
                    continue

                if args.dry_run:
                    continue

                # ---- real trigger
                # Fail-closed re-check at the point of no return: never clone,
                # push or publish for a repo GitHub does not report as `public`.
                # A denial is a PERMANENT skip (no record_failure), so it is not
                # retried and cannot wedge the head marker.
                if not public_only_guard(slug, log):
                    continue

                ok, detail = mirror.prepare(slug, branch, sha)
                if not ok:
                    log(f"{slug}@{branch} {sha[:12]} mirror prepare failed: {detail}", "ERROR")
                    record_failure(state, sha, slug, branch, detail, cfg)
                    continue

                if decision.action == MIRROR_PUSH:
                    # gate immediately before the ref is pushed to the ngit remote
                    if not public_only_guard(slug, log):
                        continue
                    pushed, push_detail = mirror.push_to_ngit(slug, repo_id, branch, sha)
                    if pushed:
                        # The relay can take a few seconds to index the freshly
                        # published 30618, so poll for it. This lookup is evidence
                        # only — success is already proven by verified_mirror_head.
                        event = None
                        for attempt in range(cfg.get("state_event_poll_attempts", 4)):
                            time.sleep(cfg.get("state_event_wait_seconds", 8) if attempt == 0 else 6)
                            event = nostr.latest_state_event(repo_id, owner_hex)
                            if event and sha in json.dumps(event.get("tags", [])):
                                break
                        event_id = event["id"] if event else None
                        carried = bool(event and sha in json.dumps(event.get("tags", [])))
                        log(
                            f"TRIGGER mirror_push {slug}@{branch} sha={sha[:12]} "
                            f"repo_state_event={event_id} commit_in_state={carried} "
                            f"({push_detail.split(' transport=')[0]})"
                        )
                        triggers_fired += 1
                        # success clears any earlier failure for this sha, so the
                        # head marker is free to advance again
                        state["failures"].pop(sha, None)
                        state["seen"][sha] = {
                            "slug": slug, "branch": branch, "at": utcnow(),
                            "action": "mirror_push", "repo_state_event": event_id,
                            "mirror_sha": sha,
                        }
                        # A 9840 is only needed when the mirror push cannot produce
                        # a state change for the coordinator to act on.
                        manual_targets = spec.workflows or tuple(mirror.workflows_at(slug, sha))
                        if cfg.get("also_manual_when_workflows", False) and manual_targets:
                            # gate immediately before the 9840 publish call
                            if not public_only_guard(slug, log):
                                continue
                            for wf in manual_targets:
                                sha256 = mirror.file_sha256(slug, sha, wf)
                                if not sha256:
                                    continue
                                event_id = nostr.publish(
                                    9840, "",
                                    [
                                        ["p", cfg["coordinator_hex"]],
                                        ["a", f"30617:{owner_hex}:{repo_id}"],
                                        ["c", sha],
                                        ["w", wf, sha256],
                                        ["r", f"refs/heads/{branch}"],
                                    ],
                                )
                                log(f"TRIGGER manual_9840 {slug} w={wf} event={event_id}")
                                triggers_fired += 1
                    else:
                        log(f"TRIGGER mirror_push FAILED {slug} sha={sha[:12]}: {push_detail}", "ERROR")
                        record_failure(state, sha, slug, branch, push_detail, cfg)
                else:  # MANUAL_9840
                    # gate immediately before the 9840 publish call
                    if not public_only_guard(slug, log):
                        continue
                    for wf in decision.workflows:
                        sha256 = mirror.file_sha256(slug, sha, wf)
                        if not sha256:
                            log(f"workflow {wf} not found at {sha[:12]}", "WARN")
                            continue
                        event_id = nostr.publish(
                            9840, "",
                            [
                                ["p", cfg["coordinator_hex"]],
                                ["a", f"30617:{owner_hex}:{repo_id}"],
                                ["c", sha],
                                ["w", wf, sha256],
                                ["r", f"refs/heads/{branch}"],
                            ],
                        )
                        log(f"TRIGGER manual_9840 {slug} w={wf} event={event_id}")
                        triggers_fired += 1
                        state["seen"][sha] = {"slug": slug, "branch": branch, "at": utcnow(),
                                              "action": "manual_9840", "event": event_id}

            # advance the head marker for this repo@branch — but NEVER past a
            # commit whose trigger failed, otherwise the failure would be
            # silently swallowed (the next tick must see it again).
            if commit_rows and not args.sha:
                newest = commit_rows[0]["sha"]
                if newest in state["failures"]:
                    if known_head:
                        state["heads"][head_key] = known_head
                    else:
                        state["heads"].pop(head_key, None)
                    log(f"{slug}@{branch} head marker held back at "
                        f"{(known_head or 'none')[:12]} (unresolved failure {newest[:12]})", "WARN")
                else:
                    state["heads"][head_key] = newest
                    if known_head is None:
                        log(f"{slug}@{branch} baseline head recorded {newest[:12]}")

    # ---- stats + summary
    counters = state["counters"]
    counters["ticks"] = counters.get("ticks", 0) + 1
    counters["triggers"] = counters.get("triggers", 0) + triggers_fired
    for _, _, decision in decisions:
        counters["decisions"][decision.code] = counters["decisions"].get(decision.code, 0) + 1

    considered = len(decisions)
    skips = sum(1 for _, _, d in decisions if not d.is_trigger)
    log(f"tick done: considered={considered} triggers={triggers_fired} skips={skips}")

    if not args.dry_run:
        save_state(state_dir / "state.json", state)
    return EXIT_OK


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        sys.exit(130)
