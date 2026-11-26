"""
Curated INDI projects on s3://fcp-indi, harvested as BIDS.

Each project is a `Project` (see `engine.py`) registered as a top-level
command with `harvest` and `info` subcommands.
"""
from brainspresso.datasets.INDI.command import register
from brainspresso.datasets.INDI.fcon1000 import FCON1000
from brainspresso.datasets.INDI.projects import PROJECTS as _PROJECTS

PROJECTS = [FCON1000(), *_PROJECTS]
for _project in PROJECTS:
    register(_project)
