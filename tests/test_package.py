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


# Modules (and their submodules) that need a dependency which is not
# part of the `test` extra.
OPTIONAL = {
    "brainspresso.datasets.OpenNeuro": "datalad",
}


@pytest.mark.parametrize("module", sorted(all_modules()))
def test_import(module):
    for prefix, dependency in OPTIONAL.items():
        if module == prefix or module.startswith(prefix + "."):
            pytest.importorskip(dependency)
    importlib.import_module(module)


@pytest.mark.parametrize("dataset", _datasets)
def test_dataset_import(dataset):
    importlib.import_module(f"brainspresso.datasets.{dataset}")


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
