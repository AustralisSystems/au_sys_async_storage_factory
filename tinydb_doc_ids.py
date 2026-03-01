from __future__ import annotations
"""
Utility helpers for deterministic TinyDB document identifiers.

TinyDB assigns incremental doc_ids by default, which causes race conditions
when multiple processes write to the same file.  The Storage & DB Factory spec
requires deterministic identifiers so concurrent writers reuse the same IDs.
This module centralises the hashing logic so any TinyDB integration (StorageFactory,
local secrets provider, etc.) can share the exact same behaviour.
"""


from hashlib import blake2b

__all__ = ["deterministic_doc_id"]

_DOC_ID_DIGEST_BYTES = 8


def deterministic_doc_id(key: str) -> int:
    """
    Compute a deterministic positive doc_id for the given key.

    TinyDB reserves doc_id 0, so the function always returns values >= 1.
    """
    digest = blake2b(key.encode("utf-8"), digest_size=_DOC_ID_DIGEST_BYTES).digest()
    value = int.from_bytes(digest, "big")
    return value or 1
