"""Candidate version is declared once, consistently, across every surface."""
from __future__ import annotations

import re
import tomllib
from pathlib import Path

import cross_tool_awareness
from cross_tool_awareness.migrations import PACKAGE_VERSION

ROOT = Path(cross_tool_awareness.__file__).parents[2]
CANDIDATE = "0.5.1"


def test_versions_agree():
    py = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    mf = tomllib.loads((ROOT / "manifest.toml").read_text())["package_version"]
    assert py == mf == PACKAGE_VERSION == cross_tool_awareness.__version__ == CANDIDATE


def test_release_doc_and_changelog_name_the_version():
    assert (ROOT / "docs" / "releases" / f"v{CANDIDATE}.md").exists()
    changelog = (ROOT / "CHANGELOG.md").read_text()
    assert re.search(rf"^## {re.escape(CANDIDATE)} ", changelog, re.M)
    assert re.search(r"^## 0\.5\.0 .*withdrawn", changelog, re.M | re.I)


def test_withdrawn_candidate_doc_is_bannered():
    text = (ROOT / "docs" / "releases" / "v0.5.0.md").read_text()
    assert "WITHDRAWN CANDIDATE" in text.splitlines()[0]
