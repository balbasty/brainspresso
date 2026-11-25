"""
Smoke tests for the installed package: every module imports, every
dataset command is registered, and package data is shipped.

`brainspresso/__init__.py` silently skips datasets that fail to import,
so these tests make such failures visible (e.g. subpackages or template
files missing from a wheel).
"""
import importlib
import pkgutil
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
OPTIONAL_DEPENDENCIES = {"datalad"}


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
        "oasis1", "oasis2", "oasis3",
    }
    assert expected <= commands


def test_package_data():
    root = Path(brainspresso.__file__).parent
    assert (root / "freesurfer" / "lut" / "FreeSurferColorLUT.txt").exists()
    assert (root / "datasets" / "IXI" / "templates" / "README").exists()
    templates = root / "datasets" / "OASIS" / "III" / "templates"
    assert any((templates / "phenotype" / "UDSv3").glob("*.json"))
