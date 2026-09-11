#!/usr/bin/env python3
"""Audit which configured repos can actually trigger ngit-ci today.

For every repo the bridge watches, report:
  * whether an ngit mirror exists (kind-30617 announcement under the maintainer key)
  * whether the repo carries `.ngit/act/workflows/*.yml` at its default branch
  * therefore whether a GitHub commit there can produce a CI run

Usage:
    python3 audit.py                    # table for every configured repo
    python3 audit.py --json audit.json  # also write machine-readable output
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from bridge import Github, Log, Nostr, load_state  # noqa: E402

RESET = "\033[0m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"


def workflows_at_head(gh: Github, slug: str, branch: str) -> list[str] | None:
    try:
        data = gh.api(f"repos/{slug}/contents/.ngit/act/workflows?ref={branch}")
    except RuntimeError:
        return None
    if not isinstance(data, list):
        return None
    return sorted(item["name"] for item in data if item.get("name", "").endswith((".yml", ".yaml")))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(HERE / "config.json"))
    parser.add_argument("--json")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    cfg = json.load(open(args.config))
    state_dir = Path(os.path.expanduser(cfg.get("state_dir", "~/.local/state/gh-ngit-ci-bridge")))
    state = load_state(state_dir / "state.json")
    log = Log(None, args.verbose)
    gh = Github(log)

    mirrors = state.get("mirror_map") or {}
    if not mirrors:
        mirrors = Nostr(cfg, log).announce_map()

    repos = list(cfg.get("repos", {}).keys())
    for org in cfg.get("orgs", []):
        repos += (state.get("org_repo_cache", {}).get(org, {}) or {}).get("repos", [])
    repos = sorted(dict.fromkeys(repos))

    rows = []
    for slug in repos:
        override = cfg.get("repos", {}).get(slug, {}).get("ngit_repo_id")
        repo_id = override or slug.split("/")[-1]
        mirrored = repo_id in mirrors or (override is not None and override in mirrors)
        if not mirrored and repo_id.lower() in mirrors:
            repo_id = repo_id.lower()
            mirrored = True

        branch = gh.default_branch(slug) or "main"
        wfs = workflows_at_head(gh, slug, branch) if mirrored else None

        if not mirrored:
            verdict, why = "CANNOT TRIGGER", "no ngit mirror (no kind-30617 announcement)"
        elif not wfs:
            verdict, why = "CANNOT RUN CI", "mirror exists but no .ngit/act/workflows/ at HEAD"
        else:
            verdict, why = "OK", f"{len(wfs)} workflow file(s)"
        rows.append(
            {
                "repo": slug,
                "ngit_repo_id": repo_id if mirrored else None,
                "mirror": mirrored,
                "workflows": wfs or [],
                "verdict": verdict,
                "reason": why,
            }
        )

    width = max(len(r["repo"]) for r in rows)
    for row in rows:
        colour = {"OK": GREEN, "CANNOT RUN CI": YELLOW, "CANNOT TRIGGER": RED}[row["verdict"]]
        print(
            f"{row['repo']:<{width}}  {colour}{row['verdict']:<14}{RESET} "
            f"mirror={'yes' if row['mirror'] else 'no ':<3} "
            f"{row['reason']}"
        )

    ok = sum(1 for r in rows if r["verdict"] == "OK")
    print(
        f"\n{len(rows)} configured repos | "
        f"{ok} fully triggerable | "
        f"{sum(1 for r in rows if r['mirror'])} mirrored | "
        f"{sum(1 for r in rows if not r['mirror'])} without an ngit mirror"
    )
    if args.json:
        Path(args.json).write_text(json.dumps(rows, indent=2))
        print(f"json written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
