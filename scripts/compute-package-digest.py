#!/usr/bin/env python3.12
"""Canonical implementation of package_sha256 for cross-tool-awareness (v0.2.0).

Primary enumeration path: `git ls-files -z` from the package root. This is the
authoritative file set — honest "tracked by git" semantics, no file-system
walk heuristics. Falls back to a walk+exclude for non-git test fixtures.

The `package_sha256` field in `manifest.toml` is deterministically zeroed
during hashing, so writing the computed digest back into that field does not
change the digest.

See spec §13 and §13.1 (release sequence).
"""

from __future__ import annotations

import hashlib
import re
import subprocess
import sys
from pathlib import Path

PKG_ROOT = Path(__file__).resolve().parent.parent

# Fallback walk-mode exclusions (used only when not in a git repo).
EXCLUDE_DIRS = {
    ".git",
    "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache",
    "build", "dist",
    ".venv", "venv", ".idea", ".vscode",
}
EXCLUDE_SUFFIXES = {".pyc", ".pyo", ".pyd"}
EXCLUDE_NAMES = {".DS_Store"}

MANIFEST_REL = "manifest.toml"
DIGEST_FIELD = "package_sha256"
ZEROED_VALUE = "0" * 64

_DIGEST_FIELD_RE = re.compile(
    r'^(?P<prefix>\s*' + DIGEST_FIELD + r'\s*=\s*)"(?P<value>[^"\n]*)"(?P<suffix>.*)$',
    re.MULTILINE,
)


def _git_ls_files(root: Path) -> list[Path] | None:
    """Return the authoritative git-tracked file list, or None if not a git repo."""
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"],
            check=True,
            capture_output=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    raw = result.stdout
    if not raw:
        return []
    names = [p for p in raw.split(b"\x00") if p]
    return sorted({Path(n.decode("utf-8")) for n in names})


def _walk_fallback(root: Path) -> list[Path]:
    out = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if any(part in EXCLUDE_DIRS for part in p.parts):
            continue
        if p.suffix in EXCLUDE_SUFFIXES:
            continue
        if p.name in EXCLUDE_NAMES:
            continue
        out.append(p.relative_to(root))
    out.sort()
    return out


def iter_tracked_files(root: Path, *, require_git: bool = False) -> list[Path]:
    """Enumerate tracked files. git ls-files is primary; walk fallback for
    non-git fixtures."""
    files = _git_ls_files(root)
    if files is None:
        if require_git:
            raise RuntimeError(f"{root} is not a git repository")
        return _walk_fallback(root)
    return files


def canonicalize_manifest(contents: str) -> str:
    return _DIGEST_FIELD_RE.sub(
        lambda m: f'{m.group("prefix")}"{ZEROED_VALUE}"{m.group("suffix")}',
        contents,
    )


def compute_digest(root: Path, *, require_git: bool = False) -> tuple[str, list[Path]]:
    h = hashlib.sha256()
    files = iter_tracked_files(root, require_git=require_git)
    for rel in files:
        p = root / rel
        h.update(b"\x00PATH\x00")
        h.update(str(rel).encode("utf-8"))
        h.update(b"\x00SIZE\x00")
        if rel.as_posix() == MANIFEST_REL:
            text = p.read_text(encoding="utf-8")
            canon = canonicalize_manifest(text)
            data = canon.encode("utf-8")
        else:
            data = p.read_bytes()
        h.update(str(len(data)).encode("ascii"))
        h.update(b"\x00DATA\x00")
        h.update(data)
    return h.hexdigest(), files


def main(argv):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=PKG_ROOT)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--count", action="store_true",
                        help="Print tracked file count on stderr.")
    parser.add_argument("--require-git", action="store_true",
                        help="Fail if --root is not a git repo (recommended for releases).")
    args = parser.parse_args(argv)

    digest, files = compute_digest(args.root, require_git=args.require_git)
    print(digest)
    if args.count:
        print(f"tracked_files: {len(files)}", file=sys.stderr)

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
