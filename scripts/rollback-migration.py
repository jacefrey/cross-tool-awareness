#!/usr/bin/env python3.12
"""Guarded CLI wrapper for migrations.rollback.

Preview by default. --execute required to mutate; --yes suppresses prompt.
No --force; refuses on in-flight state (claimed/notified/ambiguous/blocked
deliveries, live capabilities, or live dispatcher lease).
"""

import argparse
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent / "src"))

from cross_tool_awareness import rollback as rollback_migration  # noqa: E402
from cross_tool_awareness.location import resolve_bridge_db_path  # noqa: E402


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--backup-dir", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args(argv)

    db_path = args.db_path or resolve_bridge_db_path()
    now = int(time.time())

    if not args.execute:
        preview = rollback_migration(db_path, backup_dir=args.backup_dir,
                                     now=now, dry_run=True)
        print(f"[preview] db_path={db_path}")
        print(f"[preview] backup_dir={args.backup_dir}")
        print(f"[preview] status={preview.status} reason={preview.reason}")
        print("[preview] re-run with --execute --yes to rollback")
        return 0

    if not args.yes:
        print(f"Will rollback awareness schema from {db_path}")
        response = input("Type 'yes' to proceed: ")
        if response.strip().lower() != "yes":
            print("aborted")
            return 1

    result = rollback_migration(db_path, backup_dir=args.backup_dir, now=now)
    print(f"status: {result.status}")
    if result.backup is not None:
        print(f"backup: {result.backup.path}")
    if result.reason:
        print(f"reason: {result.reason}")
    if result.preserved_message_ids:
        print(f"preserved_message_count: {len(result.preserved_message_ids)}")
    return 0 if result.status == "rolled_back" else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
