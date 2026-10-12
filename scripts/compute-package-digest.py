#!/usr/bin/env python3.12
"""Canonical implementation of package_sha256 for cross-tool-awareness.

Walks the tracked package tree, excludes documented generated artifacts,
deterministically zeros the `package_sha256` field in manifest.toml during
hashing, and prints the SHA-256 hex digest. Deterministic cross-host.

See spec §13 and §13.1 (release sequence).
"""

import hashlib
import re
import sys
from pathlib import Path

PKG_ROOT = Path(__file__).resolve().parent.parent

# Excluded directory names (not file names). .git must be excluded — its
# contents change with every git operation and would make the digest
# non-deterministic across clones and fresh commits.
EXCLUDE_DIRS = {
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "build",
    "dist",
    "*.egg-info",
    ".venv",
    "venv",
    ".idea",
    ".vscode",
}
# Excluded file suffixes.
EXCLUDE_SUFFIXES = {".pyc", ".pyo", ".pyd"}
# Excluded file names.
EXCLUDE_NAMES = {".DS_Store"}

MANIFEST_REL = "manifest.toml"
DIGEST_FIELD = "package_sha256"
ZEROED_VALUE = "0" * 64

_DIGEST_FIELD_RE = re.compile(
    r'^(?P<prefix>\s*' + DIGEST_FIELD + r'\s*=\s*)"(?P<value>[^"\n]*)"(?P<suffix>.*)$',
    re.MULTILINE,
)


def iter_tracked_files(root: Path):
    """Yield paths (relative to root) of files in sorted order, applying exclusions."""
    out = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        # Exclude by any directory component.
        if any(part in EXCLUDE_DIRS or part.endswith(".egg-info") for part in p.parts):
            continue
        if p.suffix in EXCLUDE_SUFFIXES:
            continue
        if p.name in EXCLUDE_NAMES:
            continue
        rel = p.relative_to(root)
        out.append(rel)
    out.sort()
    return out


def canonicalize_manifest(contents: str) -> str:
    """Return manifest contents with the digest field zeroed."""
    return _DIGEST_FIELD_RE.sub(
        lambda m: f'{m.group("prefix")}"{ZEROED_VALUE}"{m.group("suffix")}',
        contents,
    )


def compute_digest(root: Path) -> str:
    h = hashlib.sha256()
    for rel in iter_tracked_files(root):
        p = root / rel
        # Mix in a stable path separator byte-sequence then the path.
        h.update(b"\x00PATH\x00")
        h.update(str(rel).encode("utf-8"))
        h.update(b"\x00SIZE\x00")
        if rel.as_posix() == MANIFEST_REL:
            # Deterministically zero the digest field during hashing.
            text = p.read_text(encoding="utf-8")
            canon = canonicalize_manifest(text)
            data = canon.encode("utf-8")
        else:
            data = p.read_bytes()
        h.update(str(len(data)).encode("ascii"))
        h.update(b"\x00DATA\x00")
        h.update(data)
    return h.hexdigest()


def main(argv):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=PKG_ROOT,
                        help="Package root (default: this script's parent/..)")
    parser.add_argument("--write", action="store_true",
                        help="Write the computed digest into manifest.toml")
    args = parser.parse_args(argv)

    digest = compute_digest(args.root)
    print(digest)

    if args.write:
        manifest = args.root / MANIFEST_REL
        text = manifest.read_text(encoding="utf-8")
        new_text = _DIGEST_FIELD_RE.sub(
            lambda m: f'{m.group("prefix")}"{digest}"{m.group("suffix")}',
            text,
        )
        if new_text == text:
            print("(no change to manifest)", file=sys.stderr)
        else:
            manifest.write_text(new_text, encoding="utf-8")
            print(f"wrote digest to {manifest}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
