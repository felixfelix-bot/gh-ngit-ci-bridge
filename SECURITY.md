# Security invariants

## Public GitHub repositories only (fail-closed)

**Nostr relays are public and permanent.** An ngit mirror push publishes a
kind-30618 repository-state event, and `ngit init` publishes a kind-30617
repository announcement, to relays such as `wss://relay.ngit.dev` and
`wss://gitnostr.com`. Both are readable by anyone and **cannot be revoked**.
Mirroring a private GitHub repository would therefore publish private source
code irreversibly.

The operator's rule, verbatim:

> Please make sure only the public repos get mirrored. Private repos don't
> belong on ngit.

Therefore:

> **Invariant:** only GitHub repositories whose visibility is exactly `public`
> may be mirrored, announced, or CI-triggered on nostr.

### The gate is fail-closed

Implemented once in [`public_only.py`](public_only.py). The *only* value that
lets a repository through is GitHub reporting `visibility == "public"` via

```bash
gh api repos/<owner>/<name> --jq .visibility
```

Every other outcome — `private`, `internal`, an unknown value, an empty
response, a NOT-FOUND (deleted/renamed or inaccessible), a timeout, or any API
error — **denies**. There is no allow-list override, no environment switch, and
no caching: the answer is taken live from GitHub at the moment of the decision,
so a repository that flips from public to private is refused on the very next
tick and one that flips from private to public is picked up on a later tick.

### Where it is enforced

`bridge.py` enforces the gate at every path that can cause a mirror push or a
kind-30617/kind-9840 publish:

1. **Config load / tick start** — every configured (and org-discovered) repo is
   audited and a `repo -> visibility -> verdict` table is printed. Any
   non-public entry is refused for the tick and reported as skipped. Entries
   are **never silently removed from `config.json`**.
2. **Before `Mirror.prepare`** — no private repo is ever cloned into the cache.
3. **Immediately before `Mirror.push_to_ngit`** — the git push to the ngit
   remote.
4. **Immediately before each `Nostr.publish` (kind 9840)** — the manual-trigger
   publish paths.

A repository that fails the gate is a **permanent skip for the tick**, not a
transient failure: it is logged with its observed visibility value, it is *not*
recorded in the failure/retry map, and it does not participate in the
head-marker logic, so it cannot wedge the bridge. It is re-evaluated on the
next tick.

Migration scripts that publish kind-30617 announcements or push mirrors fall
under the same invariant: gate them with the same
`gh api repos/<o>/<r> --jq .visibility` check and fail closed.

### Tests

`tests/test_public_only.py` covers public -> allowed, private -> skipped with
the observed reason, API error / NOT-FOUND -> skipped (fail-closed), and the
private -> public flip being picked up on a later tick. Run with `make test`.
