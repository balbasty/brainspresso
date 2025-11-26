from pathlib import Path

from datalad.cli.main import main as datalad

from braindataprep.utils.path import get_tree_path
from braindataprep.utils.log import setup_filelog
from braindataprep.datasets.OpenNeuro.command import openneuro


@openneuro.command
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
