#!/usr/bin/env python3
"""Sign a Nostr event with the maintainer key held in a 600-mode key file.

Reads the key file named by NOSTR_KEY_FILE (default: ~/.hermes/.ngit-new-key),
extracts the first `nsec1...` token, and prints the fully signed event JSON on
stdout. The secret is never written to argv, to the environment of a child
process, or to any log.

Why this exists: `nak event --sec <nsec>` puts the signing key in argv, which is
world-readable via /proc/<pid>/cmdline on Linux. Here the key stays in this
process's memory and the *signed* event is piped to `nak event` for publication.

Requires coincurve (`/opt/miniconda/bin/python3` has it).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path

NSEC_RE = re.compile(r"nsec1[02-9ac-hj-np-z]{20,}")

CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"


def bech32_decode_nsec(nsec: str) -> bytes:
    """Decode an nsec1... bech32 string to its 32 secret bytes."""
    pos = nsec.rfind("1")
    data = [CHARSET.index(c) for c in nsec[pos + 1:].lower()]
    # strip the 6-symbol checksum
    data = data[:-6]
    # convert 5-bit groups to 8-bit
    acc = 0
    bits = 0
    out = bytearray()
    for value in data:
        acc = (acc << 5) | value
        bits += 5
        if bits >= 8:
            bits -= 8
            out.append((acc >> bits) & 0xFF)
    if len(out) != 32:
        raise ValueError(f"unexpected nsec payload length: {len(out)}")
    return bytes(out)


def load_key(key_file: str) -> bytes:
    text = Path(os.path.expanduser(key_file)).read_text()
    match = NSEC_RE.search(text)
    if not match:
        raise SystemExit(f"no nsec found in {key_file}")
    return bech32_decode_nsec(match.group(0))


def build_event(secret: bytes, kind: int, tags: list[list[str]], content: str) -> dict:
    import coincurve  # imported late so --help works without the dep

    priv = coincurve.PrivateKey(secret)
    pubkey = priv.public_key.format(compressed=True)[1:].hex()
    created_at = int(time.time())

    canonical = json.dumps(
        [0, pubkey, created_at, kind, tags, content],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    event_id = hashlib.sha256(canonical.encode()).hexdigest()
    sig = priv.sign_schnorr(bytes.fromhex(event_id), aux_randomness=os.urandom(32)).hex()

    return {
        "id": event_id,
        "pubkey": pubkey,
        "created_at": created_at,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": sig,
    }


def main(argv: list[str]) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="sign a nostr event and print it")
    parser.add_argument("-k", "--kind", type=int, required=True)
    parser.add_argument("-c", "--content", default="")
    parser.add_argument("-t", "--tag", action="append", default=[], help="tag as name=value")
    parser.add_argument(
        "-T",
        "--multitag",
        action="append",
        default=[],
        help="tag as name=v1;v2;v3 (one tag, several values)",
    )
    parser.add_argument("--key-file", default=os.environ.get("NOSTR_KEY_FILE", "~/.hermes/.ngit-new-key"))
    parser.add_argument("--print-pubkey", action="store_true", help="print signer pubkey instead of an event")
    args = parser.parse_args(argv)

    secret = load_key(args.key_file)
    if args.print_pubkey:
        import coincurve

        print(coincurve.PrivateKey(secret).public_key.format(compressed=True)[1:].hex())
        return 0

    tags: list[list[str]] = []
    for item in args.tag:
        name, _, value = item.partition("=")
        tags.append([name, value])
    for item in args.multitag:
        name, _, value = item.partition("=")
        tags.append([name, *value.split(";")])

    event = build_event(secret, args.kind, tags, args.content)
    print(json.dumps(event, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
