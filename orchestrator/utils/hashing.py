"""Canonical JSON hashing utilities."""

import hashlib
import json
from pathlib import Path


def canonical_json_bytes(data: dict) -> bytes:
    """Stable serialisation: sorted keys, no extra whitespace.

    json.dumps(sort_keys=True) recursively sorts all nested dict keys.
    """
    return json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")


def hash_artifact(data: dict) -> str:
    """SHA-256 hex digest of canonical JSON.

    Nested dicts are also sorted via json.dumps(sort_keys=True).
    Returns a 64-character lowercase hex string.
    """
    return hashlib.sha256(canonical_json_bytes(data)).hexdigest()


def hash_file_bytes(path: Path) -> str:
    """SHA-256 hex digest of raw file bytes (byte-for-byte, NOT canonical JSON).

    Not compatible with hash_artifact — they hash the same content differently.
    Used exclusively by RunIndex.json to record stable file-content fingerprints.
    """
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
