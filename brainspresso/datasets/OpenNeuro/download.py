from pathlib import Path

from datalad.cli.main import main as datalad

from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.datasets.OpenNeuro.command import openneuro


@openneuro.command(name="harvest")
def download(
    path: str | None = None,
    *,
    id: str,
    name: str | None = None,
    log: str | None = None,
    level: str = "info",
):
    setup_filelog(log, level=level)
    path = Path(get_tree_path(path))
    name = name or id
    path = path / name

    datalad([
        "datlad",
        "clone",
        f"git@github.com:OpenNeuroDatasets/{id}.git",
        str(path)
    ])
