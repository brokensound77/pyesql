"""Ensures pyproject.toml and pyesql.__version__ stay in sync."""

import tomllib
from pathlib import Path

import pyesql


def test_version_matches_pyproject():
    pyproject = tomllib.loads((Path(__file__).parent.parent / "pyproject.toml").read_text())
    assert pyesql.__version__ == pyproject["project"]["version"]
