"""Shared fixtures: tests never read the config of the user running them."""
import os

import pytest


@pytest.fixture(autouse=True)
def _no_user_config(tmp_path_factory, monkeypatch):
    """No user config file, no `BRAINSPRESSO_OPT_*` variable."""
    home = tmp_path_factory.getbasetemp() / "no-config-home"
    monkeypatch.setenv("BRAINSPRESSO_CONFIG_HOME", str(home))
    for var in list(os.environ):
        if var.startswith("BRAINSPRESSO_OPT_"):
            monkeypatch.delenv(var)
