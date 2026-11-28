"""Shared fixtures: tests never read the config of the user running them."""
import os

import pytest


@pytest.fixture(autouse=True)
def _no_user_config(tmp_path_factory, monkeypatch):
    """
    No user config file, no root file, no `BRAINSPRESSO_OPT_*` variable.

    The root of the datasets (`BDP_PATH`, else the current folder, whose
    `.brainspresso/config.toml` is read even with `--out`) is an empty
    folder; tests that need another set or unset it.
    """
    base = tmp_path_factory.getbasetemp()
    monkeypatch.setenv("BRAINSPRESSO_CONFIG_HOME",
                       str(base / "no-config-home"))
    monkeypatch.setenv("BDP_PATH", str(base / "no-config-root"))
    for var in list(os.environ):
        if var.startswith("BRAINSPRESSO_OPT_"):
            monkeypatch.delenv(var)
