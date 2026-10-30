"""Fixtures shared by the olf-bench tests.

`fixtures/project` is the descriptor, Floe-contract and CSV subset of the
OpenLakeForge reference project at the commit `pyproject.toml` pins: what the
dataset generator and the inventory read, without the dbt, Dagster and
dashboard code a deployment also needs.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

FIXTURE_PROJECT = Path(__file__).parent / "fixtures" / "project"


@pytest.fixture(autouse=True)
def _isolate_toolchain(monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory) -> None:
    """Without this, any olf call that resolves kubectl or terraform downloads real binaries into ~/.openlakeforge."""
    monkeypatch.setenv("OLF_TOOLCHAIN_MODE", "host")
    monkeypatch.setenv("OLF_HOME", str(tmp_path_factory.mktemp("olf-home")))


@pytest.fixture
def external_project(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    shutil.copytree(FIXTURE_PROJECT, root)
    return root
