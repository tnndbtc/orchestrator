"""Canonical JSON hashing utilities."""

import hashlib
import json


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
