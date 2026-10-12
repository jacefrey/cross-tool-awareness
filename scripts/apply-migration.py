#!/usr/bin/env python3.12
"""Guarded CLI wrapper for migrations.apply.

Preview by default. --execute required to mutate; --yes suppresses prompt.
"""

import argparse
import sys
import time
from pathlib import Path

# Allow running from a package-local checkout without install.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent / "src"))

from cross_tool_awareness import apply as apply_migration  # noqa: E402
from cross_tool_awareness.location import resolve_bridge_db_path  # noqa: E402


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=None,
                        help="Path to bridge.db (default: resolve_bridge_db_path())")
    parser.add_argument("--backup-dir", type=Path, required=True,
                        help="Backup directory (e.g. machine-manager/backups/awareness/)")
    parser.add_argument("--execute", action="store_true",
                        help="Actually mutate state. Default is preview.")
    parser.add_argument("--yes", action="store_true",
                        help="Skip interactive confirmation (requires --execute).")
    args = parser.parse_args(argv)

    db_path = args.db_path or resolve_bridge_db_path()
    now = int(time.time())

    if not args.execute:
        preview = apply_migration(db_path, backup_dir=args.backup_dir, now=now, dry_run=True)
        print(f"[preview] db_path={db_path}")
        print(f"[preview] backup_dir={args.backup_dir}")
        print(f"[preview] status={preview.status} reason={preview.reason}")
        print(f"[preview] expected_version={preview.version}")
        print("[preview] re-run with --execute --yes to apply")
        return 0

    if not args.yes:
        print(f"Will apply awareness schema to {db_path}")
        print(f"Backup will be written under {args.backup_dir}")
        response = input("Type 'yes' to proceed: ")
        if response.strip().lower() != "yes":
            print("aborted")
            return 1

    result = apply_migration(db_path, backup_dir=args.backup_dir, now=now)
    print(f"status: {result.status}")
    print(f"version: {result.version}")
    if result.backup is not None:
        print(f"backup: {result.backup.path}")
        print(f"backup_sha256: {result.backup.backup_sha256.hex()}")
    if result.reason:
        print(f"reason: {result.reason}")
    return 0 if result.status in ("applied", "already_at_version") else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
