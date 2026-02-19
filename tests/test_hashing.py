"""Tests for orchestrator.utils.hashing — canonical JSON and SHA-256 hashing."""

import pytest

from orchestrator.utils.hashing import canonical_json_bytes, hash_artifact


class TestCanonicalJsonBytes:
    def test_canonical_stable_key_order(self):
        """Same content, different insertion order → identical bytes."""
        a = canonical_json_bytes({"b": 2, "a": 1})
        b = canonical_json_bytes({"a": 1, "b": 2})
        assert a == b

    def test_nested_key_order_stable(self):
        """Nested dicts are also sorted by json.dumps(sort_keys=True)."""
        a = canonical_json_bytes({"outer": {"z": 9, "a": 1}, "b": 2})
        b = canonical_json_bytes({"b": 2, "outer": {"a": 1, "z": 9}})
        assert a == b

    def test_returns_bytes(self):
        result = canonical_json_bytes({"key": "value"})
        assert isinstance(result, bytes)

    def test_no_extra_whitespace(self):
        result = canonical_json_bytes({"a": 1})
        assert b" " not in result


class TestHashArtifact:
    def test_hash_deterministic(self):
        """Same data produces the same hash on repeated calls."""
        data = {"key": "value", "number": 42}
        h1 = hash_artifact(data)
        h2 = hash_artifact(data)
        assert h1 == h2

    def test_hash_changes_with_content(self):
        """Different values produce different hashes."""
        h1 = hash_artifact({"key": "value1"})
        h2 = hash_artifact({"key": "value2"})
        assert h1 != h2

    def test_hash_length(self):
        """SHA-256 produces a 64-character hex string."""
        h = hash_artifact({"any": "data"})
        assert len(h) == 64

    def test_hash_is_lowercase_hex(self):
        """Hash contains only lowercase hexadecimal characters."""
        h = hash_artifact({"any": "data"})
        assert all(c in "0123456789abcdef" for c in h)

    def test_hash_key_order_independent(self):
        """Key order does not affect the hash."""
        h1 = hash_artifact({"b": 2, "a": 1})
        h2 = hash_artifact({"a": 1, "b": 2})
        assert h1 == h2
