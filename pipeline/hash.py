"""
pipeline.hash — content-addressable verification utilities.

The ZIM file is hashed once at pipeline startup and compared against the
lock value stored in config.  This guarantees deterministic, reproducible
builds from identical inputs.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

_CHUNK = 1 << 20  # 1 MiB read chunks


def sha256_file(path: str | Path) -> str:
    """Return the lowercase hex SHA-256 digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(_CHUNK):
            h.update(chunk)
    return h.hexdigest()


def verify_zim(zim_path: str | Path, expected_sha256: str | None) -> None:
    """
    Verify the ZIM file matches the expected SHA-256 hash.

    Raises ``ValueError`` if the hash does not match.
    Raises ``FileNotFoundError`` if the ZIM file does not exist.
    Skips verification if ``expected_sha256`` is ``None``.
    """
    zim_path = Path(zim_path)
    if not zim_path.exists():
        raise FileNotFoundError(f"ZIM file not found: {zim_path.resolve()}")

    if expected_sha256 is None:
        return

    actual = sha256_file(zim_path)
    if actual != expected_sha256.lower():
        raise ValueError(
            f"ZIM hash mismatch.\n"
            f"  Expected : {expected_sha256.lower()}\n"
            f"  Actual   : {actual}\n"
            f"  File     : {zim_path.resolve()}\n"
            "Update config.input.zim_sha256 or re-download the ZIM file."
        )


def compute_build_hash(manifest_path: str | Path) -> str:
    """
    Compute a deterministic build hash from the manifest file.

    The hash is the SHA-256 of the lexicographically sorted JSON
    representation of the manifest entries (keyed on article path).
    This is stable across filesystem layouts and Python dict ordering.
    """
    manifest_path = Path(manifest_path)
    with manifest_path.open() as fh:
        entries: list[dict] = json.load(fh)

    # Sort by path for determinism; then re-serialise canonically.
    entries.sort(key=lambda e: e["path"])
    canonical = json.dumps(entries, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def write_build_hash(manifest_path: str | Path, output_path: str | Path) -> str:
    """Compute and write the build hash to *output_path*.  Returns the hash."""
    digest = compute_build_hash(manifest_path)
    Path(output_path).write_text(digest + "\n")
    return digest
