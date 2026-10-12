"""Resolve the shared bridge.db path.

Default: /Users/jacefrey/.local/share/claude-bridge/bridge.db (machine-manager
convention; see CLAUDE.md § Machine-level databases).

Override via CROSS_TOOL_AWARENESS_DB_PATH env var; tests set this to a tmpdir path.
"""

import os
from pathlib import Path

DEFAULT_BRIDGE_DB_PATH = "/Users/jacefrey/.local/share/claude-bridge/bridge.db"
ENV_VAR = "CROSS_TOOL_AWARENESS_DB_PATH"


def resolve_bridge_db_path() -> Path:
    """Return the configured bridge.db path. Does NOT create or probe the file."""
    override = os.environ.get(ENV_VAR)
    if override:
        return Path(override)
    return Path(DEFAULT_BRIDGE_DB_PATH)
