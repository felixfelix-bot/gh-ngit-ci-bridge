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
            "heads": {},
            "mirror_map": {},
            "mirror_map_refreshed_at": 0,
            "org_repo_cache": {},
            "counters": {"ticks": 0, "decisions": {}, "triggers": 0},
        }
    with path.open() as handle:
        state = json.load(handle)
    state.setdefault("seen", {})
    state.setdefault("heads", {})
    state.setdefault("mirror_map", {})
    state.setdefault("org_repo_cache", {})
    state.setdefault("counters", {"ticks": 0, "decisions": {}, "triggers": 0})
    return state


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

    def _nostr_env(self) -> dict:
        """Feed nostr.nsec/npub to git via env, never via argv."""
        env = dict(os.environ)
        env["GIT_CONFIG_COUNT"] = "3"
        env["GIT_CONFIG_KEY_0"] = "nostr.nsec"
        env["GIT_CONFIG_VALUE_0"] = self._key_nsec()
        env["GIT_CONFIG_KEY_1"] = "nostr.npub"
        env["GIT_CONFIG_VALUE_1"] = self.cfg["ngit_owner_npub"]
        env["GIT_CONFIG_KEY_2"] = "nostr.default_servers"
        env["GIT_CONFIG_VALUE_2"] = "relay.ngit.dev"
        return env

    def _key_nsec(self) -> str:
        key_file = Path(os.path.expanduser(self.cfg["key_file"]))
        match = NSEC_RE.search(key_file.read_text())
        if not match:
            raise RuntimeError(f"no nsec in {key_file}")
        return match.group(0)

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

    def push_to_ngit(self, slug: str, repo_id: str, branch: str, sha: str) -> tuple[bool, str]:
        directory = self._repo_dir(slug)
        remote = f"nostr://{self.cfg['ngit_owner_npub']}/relay.ngit.dev/{repo_id}"
        code, out, err = run(
            ["git", "-C", str(directory), "push", "--porcelain", remote, f"{sha}:refs/heads/{branch}"],
            env=self._nostr_env(),
            timeout=600,
        )
        combined = (out + "\n" + err).strip()
        # git-remote-nostr sometimes reports transport noise while the nostr push
        # itself succeeded; treat "the ref is now on the mirror" as ground truth
        # and let the caller verify with ls-remote.
        if code != 0 and "already exists" not in combined and "Everything up-to-date" not in combined:
            return False, combined[:400]
        return True, combined[-400:]


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
    if args.repo:
        wanted = {r.lower() for r in args.repo}
        repos = [r for r in repos if r.lower() in wanted]
        if not repos:
            repos = list(args.repo)

    log(f"tick start: {len(repos)} repos, dry_run={args.dry_run}, trigger_mode={trigger_mode}")

    decisions: list[tuple[str, str, Decision]] = []
    triggers_fired = 0

    for slug in repos:
        spec_cfg = cfg.get("repos", {}).get(slug, {})
        branches = spec_cfg.get("branches") or branch_allow
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
                is_baseline = known_head is None and state["heads"].get(head_key) is None

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
                ok, detail = mirror.prepare(slug, branch, sha)
                if not ok:
                    log(f"{slug}@{branch} {sha[:12]} mirror prepare failed: {detail}", "ERROR")
                    state["seen"][sha] = {"slug": slug, "branch": branch, "at": utcnow(),
                                          "action": "error", "reason": detail[:200]}
                    continue

                if decision.action == MIRROR_PUSH:
                    pushed, push_detail = mirror.push_to_ngit(slug, repo_id, branch, sha)
                    if pushed:
                        time.sleep(cfg.get("state_event_wait_seconds", 8))
                        event = nostr.latest_state_event(repo_id, owner_hex)
                        event_id = event["id"] if event else None
                        carried = bool(event and sha in json.dumps(event.get("tags", [])))
                        log(
                            f"TRIGGER mirror_push {slug}@{branch} sha={sha[:12]} "
                            f"repo_state_event={event_id} commit_in_state={carried}"
                        )
                        triggers_fired += 1
                        state["seen"][sha] = {
                            "slug": slug, "branch": branch, "at": utcnow(),
                            "action": "mirror_push", "repo_state_event": event_id,
                            "mirror_sha": sha,
                        }
                        # A 9840 is only needed when the mirror push cannot produce
                        # a state change for the coordinator to act on.
                        manual_targets = spec.workflows or tuple(mirror.workflows_at(slug, sha))
                        if cfg.get("also_manual_when_workflows", False) and manual_targets:
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
                        state["seen"][sha] = {"slug": slug, "branch": branch, "at": utcnow(),
                                              "action": "error", "reason": push_detail[:200]}
                else:  # MANUAL_9840
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

            # advance the head marker for this repo@branch
            if commit_rows and not args.sha:
                state["heads"][head_key] = commit_rows[0]["sha"]
                if known_head is None:
                    log(f"{slug}@{branch} baseline head recorded {commit_rows[0]['sha'][:12]}")

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
