"""
Staging: run a task on node-local scratch.

Interfaces only (see `brainspresso.grind`). Chosen when a plan is run
(`--stage none|inputs|all`, `--scratch`, `--keep-scratch-on-failure`;
`percolate.options`), never part of the plan or of the signature.

Inside the task:

1. the inputs are resolved and the signature is computed from the
   originals;
2. **stage in** (`stage_in`, "inputs" and "all"): the resolved inputs
   (the raw query and upstream outputs of each `Requirement`, with the
   metadata they inherit) are copied into a minimal tree under scratch
   that mirrors their dataset-relative paths. Read locks are held only
   while copying;
3. the stage runs with the scratch paths (`Work.inputs`); a container
   gets the staged folders bound in place of the originals, at the
   same paths inside it;
4. **stage out** (`stage_out`, "all"): what a command wrote under
   scratch is committed to the dataset by actions (`File` writers under
   exclusive locks, renamed into place), one per file, never by a raw
   copy: locks and the outputs of the task record stay correct;
5. scratch is removed, unless the task failed and
   `keep_scratch_on_failure` is set.

BIDS Apps: stage in builds a minimal BIDS dataset on scratch
(`dataset_description.json` and the files of the participant that the
selection and the overrides of the dataset keep), which is the
`bids_dir` of the app. Only then can files be selected for an app that
has no filter file ("which T1w").
"""
from __future__ import annotations

from typing import Iterator

from brainspresso.actions.action import Action
from brainspresso.grind.context import GrindContext
from brainspresso.grind.model import Stage, Work

__all__ = ["stage_in", "stage_out"]


def stage_in(ctx: GrindContext, stage: Stage, work: Work) -> Work:
    """
    Copy the inputs of a task to its scratch folder.

    Parameters
    ----------
    ctx : GrindContext
        Context of the run
    stage : Stage
        The stage
    work : Work
        The unit, with its inputs in the dataset

    Returns
    -------
    Work
        The same, with `inputs` (and, under "all", the folder that
        commands write into) on scratch; unchanged under "none"

    Raises
    ------
    OSError
        If scratch is missing or too small
    """
    raise NotImplementedError


def stage_out(ctx: GrindContext, stage: Stage, work: Work
              ) -> Iterator[Action]:
    """
    Yield the actions that commit staged outputs to the dataset.

    Parameters
    ----------
    ctx : GrindContext
        Context of the run
    stage : Stage
        The stage (declared outputs and claims)
    work : Work
        The staged unit

    Yields
    ------
    Action
        One copy per file found under the staged output folder, to its
        path in `work.root`; files outside the declared outputs and
        claims are an error
    """
    raise NotImplementedError
