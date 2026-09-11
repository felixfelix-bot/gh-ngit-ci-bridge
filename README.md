# gh-ngit-ci-bridge

Makes **new commits from a set of watched GitHub identities** trigger [ngit-ci]
(the self-hosted Nostr CI coordinator), which by design can only see Nostr.

```
GitHub push by a watched identity
        │  (gh api: commits + push events)
        ▼
   bridge.py  ── decide(commit, repo, state) ──┐
        │                                      │  skip + reason
        │                                      └──────────────► log only
        ▼  trigger
   push ref to the repo's ngit mirror  ──► git-remote-nostr publishes kind 30618
        │                                    (repo state — what the coordinator consumes)
        └─ fallback: kind-9840 Manual Trigger with `w` = workflow path + content sha256
```

## Why

ngit-ci reacts to NIP-34 repository-state events (30618), Service Requests
(9843) and Manual Triggers (9840) on relays. A GitHub push emits none of those,
so commits authored or pushed by `felixfelix-bot`, `c03rad0r`, `Amperstrand`,
`Origami74`, `Franchovy`, `maximotodev` or `hkarani` are invisible to the
coordinator. The bridge publishes the ref to the repo's ngit mirror, which emits
the 30618 the coordinator already listens for.

## Files

| file | purpose |
|------|---------|
| `bridge.py` | the poller: discovery, decision, trigger, lock, logging |
| `decision.py` | pure decision logic (unit-tested, no I/O) |
| `nostr_event.py` | signs an event with the key in `key_file` — key never enters argv |
| `config.json` | committed config: identities, orgs/repos, relays, intervals. No secrets |
| `audit.py` | reports, per watched repo, whether a CI run is even possible (mirror? workflows?) |
| `tests/test_decision.py` | unit tests for the decision matrix |
| `systemd/*` | user timer + service (every 5 minutes) |
| `install.sh` | installs and enables the timer |
| `.ngit/act/workflows/bridge-smoke.yml` | the repo's own ngit-CI workflow |

## Hard-won lessons about git-remote-nostr (read before touching the push path)

1. **It reads `nostr.nsec`/`nostr.npub` from the repository's own `.git/config`** and ignores
   `GIT_CONFIG_*` environment injection. Injecting via the environment makes it sign as the
   machine-global ngit account, which is rejected as a non-maintainer.
2. **It exits 0 when it rejects the push** (`your nostr account … isn't listed as a
   maintainer`) and prints `Error: could not update remote_ref locally` on pushes that
   succeeded. Never trust its exit status — verify with
   `git ls-remote https://relay.ngit.dev/<npub>/<repo>.git refs/heads/<branch>`.
3. The source side of a push refspec must be a **ref name**; pushing a bare SHA fails with
   `cannot find ref <sha>`. The bridge stages `refs/bridge-publish/<branch>` first.
4. `ngit init` **replaces `origin` with a `nostr://` remote**. Keep a separate `origin` for
   GitHub and a `ngit` remote for Nostr.

## Install

```bash
./install.sh                       # copies units, enables gh-ngit-ci-bridge.timer
systemctl --user start gh-ngit-ci-bridge.service        # run one tick now
journalctl --user -u gh-ngit-ci-bridge.service -n 50    # tick output
tail -f ~/.local/state/gh-ngit-ci-bridge/bridge.log     # bridge log
```

The timer runs the service every 5 minutes. The service type is `oneshot`; exit
code `3` means another tick held the lock (normal, not an error).

## Usage

```bash
python3 bridge.py --dry-run                      # classify only, publish nothing, keep state
python3 bridge.py --dry-run --repo OpenTollGate/tollgate-module-basic-go \
                  --sha <sha>                    # classify one specific commit
python3 bridge.py --refresh-mirrors              # re-read kind-30617 announcements now
python3 bridge.py --repo owner/name              # real run, one repo only
python3 audit.py                                 # what can trigger CI today, per repo
```

Exit codes: `0` ok, `2` runtime error, `3` another tick holds the lock,
`4` config error.

## State (outside the repo)

`~/.local/state/gh-ngit-ci-bridge/state.json` holds last-seen head SHAs, the set
of handled commit SHAs (dedupe: a SHA never triggers twice), the kind-30617
mirror map, the org repo cache and counters. It is never committed.

Clones used to push are created lazily on first trigger under
`~/.local/share/gh-ngit-ci-bridge/cache/` (mode 700).

## Trigger paths

1. **mirror_push** — `git push nostr://<npub>/relay.ngit.dev/<repo-id> <sha>:refs/heads/<branch>`.
   The nostr signing key is handed to git through `GIT_CONFIG_*` environment
   variables, never through argv. After the push the bridge re-reads the newest
   kind-30618 for the repo and logs its event id plus whether the new commit is
   actually in the published state.
2. **manual_9840** — NIP-C1 Manual Trigger (`p` = coordinator only, `a` = 30617
   coordinate, `c` = commit, `w` = workflow path + sha256 of the file at that
   commit, `r` = ref). Used when a repo-state change would be a no-op (mirror
   already carries the sha) or when `trigger_mode` is `manual`.

Both paths log the resulting Nostr event id.

## Config notes

* `orgs` are expanded at runtime through `gh api orgs/<org>/repos`, so repos
  migrated to ngit later are picked up automatically — the repo list is not
  hard-coded.
* A GitHub repo maps to an ngit repo-id by the `d` tag of its kind-30617
  announcement, normally the repository name; `ngit_repo_id` overrides it.
* `baseline_on_first_run: true` records the current head without triggering, so
  enabling the bridge does not retro-fire CI for old commits.
* `trigger_mode`: `mirror` (default) or `manual`.

## Authorisation

The coordinator runs with `request-required` policy: a push-driven run needs a
standing Service Request (kind 9843) for that repo. Manual Triggers bypass the
gate by design. If a repo has no standing SR the coordinator logs
`Skipping push trigger until an authorized Service Request is observed` — the
bridge still publishes the state event, so the run happens once an SR exists.

[ngit-ci]: https://relay.ngit.dev/npub15qydau2hjma6ngxkl2cyar74wzyjshvl65za5k5rl69264ar2exs5cyejr/relay.ngit.dev/ngit-ci.git
