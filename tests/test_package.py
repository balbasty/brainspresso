"""
Smoke tests for the installed package: every module imports, every
dataset command is registered, and package data is shipped.

`brainspresso/__init__.py` skips datasets that fail to import (with a
warning) so that one broken dataset does not break the CLI; these tests
make such failures fail CI (e.g. subpackages or template files missing
from a wheel). Commands whose optional dependencies are missing are not
registered (see `brainspresso.utils.deps`).
"""
import importlib
import json
import pkgutil
import subprocess
import sys
from pathlib import Path

import pytest

import brainspresso
from brainspresso import _datasets
from brainspresso.cli import app


def all_modules():
    root = Path(brainspresso.__file__).parent
    for info in pkgutil.walk_packages([str(root)], "brainspresso."):
        yield info.name


# Optional dependencies that are not part of the `test` extra. A module
# that fails to import *only* because one of them is missing is skipped;
# any other import error fails.
OPTIONAL_DEPENDENCIES = {"datalad", "xlrd", "openpyxl", "pymupdf", "nrrd"}


def import_or_skip(module):
    try:
        return importlib.import_module(module)
    except ModuleNotFoundError as e:
        if (e.name or "").split(".")[0] in OPTIONAL_DEPENDENCIES:
            pytest.skip(f"{module} needs {e.name}")
        raise


@pytest.mark.parametrize("module", sorted(all_modules()))
def test_import(module):
    import_or_skip(module)


@pytest.mark.parametrize("dataset", _datasets)
def test_dataset_import(dataset):
    import_or_skip(f"brainspresso.datasets.{dataset}")


def test_cli_commands():
    commands = set(app._commands)
    expected = {
        "abide1", "abide2", "adhd200", "cobre", "corr", "gsp", "ixi",
        "oasis1", "oasis2", "oasis3", "openneuro", "unc-wisconsin",
        "bullitt",
        "fcon1000", "nki-rs", "hbn", "sald", "slim", "mpi-lemon", "acpi",
        "natview", "simon",
    }
    assert expected <= commands


# Run in a fresh interpreter, with optional dependencies made unimportable
HIDE_DEPS = """
import sys, json
for name in ("xlrd", "openpyxl"):
    sys.modules[name] = None
from brainspresso.cli import app
from brainspresso.utils.deps import UNAVAILABLE
print(json.dumps({
    "commands": sorted(app._commands),
    "ixi": sorted(app["ixi"]._commands),
    "oasis1": sorted(app["oasis1"]._commands),
    "unavailable": [[u.command, list(u.missing), u.hint] for u in UNAVAILABLE],
}))
"""


def test_missing_optional_dependencies():
    out = subprocess.run(
        [sys.executable, "-c", HIDE_DEPS],
        capture_output=True, text=True, check=True,
    )
    # Nothing is logged about missing extras
    assert out.stderr == ""
    info = json.loads(out.stdout)
    # Commands whose dependencies are missing are not registered...
    assert "roast" not in info["ixi"] and "harvest" in info["ixi"]
    assert "roast" not in info["oasis1"] and "harvest" in info["oasis1"]
    # ... but other commands still are
    assert {"ixi", "oasis1", "openneuro", "corr", "adhd200"} <= set(
        info["commands"]
    )
    # ... and they are recorded, with a hint
    unavailable = {u[0]: u for u in info["unavailable"]}
    assert unavailable["ixi roast"][1] == ["xlrd"]
    assert unavailable["ixi roast"][2] == "pip install 'brainspresso[ixi]'"
    assert {"oasis1 roast", "oasis2 roast"} <= set(unavailable)


def test_package_data():
    root = Path(brainspresso.__file__).parent
    assert (root / "freesurfer" / "lut" / "FreeSurferColorLUT.txt").exists()
    assert (root / "datasets" / "IXI" / "templates" / "README").exists()
    templates = root / "datasets" / "OASIS" / "III" / "templates"
    assert any((templates / "phenotype" / "UDSv3").glob("*.json"))
    bullitt = root / "datasets" / "Bullitt" / "templates"
    for name in ("participants.tsv", "acq-flash_T1w.json",
                 "derivatives_bidsignore"):
        assert (bullitt / name).exists()
