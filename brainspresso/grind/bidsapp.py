"""
BIDS Apps as pipelines.

Interfaces only (see `brainspresso.grind`). A BIDS App is a container
with the command line `<app> <bids_dir> <out_dir> <level> [options]`.
`BidsAppPipeline` wraps any of them: a `participant` stage, run as
`<app> <bids_dir> <out_dir> participant --participant_label <sub>`, and
an optional `group` stage. `bids_dir` is the raw dataset (mounted
read-only).

Each participant task runs with its own `out_dir` (scratch under
`--stage all`, else a folder of the task under `<state>/work/`), never
the derivative dataset: concurrent apps would race on
`dataset_description.json`, `logs/` and shared reference data, and
overwrite the provenance that grind writes. What an app writes cannot
be listed beforehand: each task is one action that claims its output
paths (`BidsAppPipeline.claims`). Once the app has exited, the claim
yields one action per file found under the claimed paths of `out_dir`,
which commits it to the derivative through `actions.file.File`; these
are the outputs of the task record, and a claim that commits nothing
is an error. The rest of `out_dir`, by kind:

* `dataset_description.json`: not committed. The `finalize` task
  writes the description after the apps (`grind.provenance`), keeping
  the fields that the app's copy adds;
* `logs/`: committed to `logs/<unit label>/`;
* `BidsAppPipeline.shared` paths (reference data that every
  participant writes alike): committed once, under an exclusive lock,
  first writer wins;
* anything else: left in `out_dir`, reported, removed with it.

The `group` stage is one task per dataset and writes to the derivative
dataset itself.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator, Mapping

from brainspresso.actions.action import Action
from brainspresso.actions.engine import Producer
from brainspresso.grind.context import GrindContext
from brainspresso.grind.model import Pipeline, Stage, Work

__all__ = ["BidsAppConfig", "BidsAppPipeline", "BidsAppStage"]


@dataclass(frozen=True)
class BidsAppConfig:
    """
    Options common to every BIDS App.

    The fields are the contract; step 7 makes it a pydantic model (a
    `grind.model.Config`) that apps extend with their own options.

    Attributes
    ----------
    args : tuple[str, ...]
        Extra arguments, passed as they are
    bids_filter : Mapping[str, Mapping[str, str]] | None
        Content of the filter file of apps that take one
        (`--bids-filter-file`). A selection finer than subjects and
        sessions needs it, or staging (`--stage inputs|all` copies only
        the selected files, `grind.staging`); else it is refused, with
        a message that names `--stage inputs`
    skip_bids_validation : bool
        Pass the app's flag that skips its validation of the dataset
    files : Mapping[str, str]
        Host files the app needs, by option (`{"--fs-license-file":
        "~/license.txt"}`); mounted read-only
    """

    args: tuple[str, ...] = ()
    bids_filter: Mapping[str, Mapping[str, str]] | None = None
    skip_bids_validation: bool = False
    files: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class BidsAppStage(Stage):
    """
    One analysis level of a BIDS App.

    Attributes
    ----------
    analysis_level : str
        Third argument of the app ("participant" or "group")
    """

    analysis_level: str = "participant"

    def command(self, ctx: GrindContext, work: Work) -> list[str]:
        """
        Build the argv of the app for one unit.

        Parameters
        ----------
        ctx : GrindContext
            Context of the run
        work : Work
            The unit and its configuration

        Returns
        -------
        list[str]
            `[<bids_dir>, <out_dir>, <level>, "--participant_label",
            <sub>, ...]`, with paths as seen in the container
        """
        raise NotImplementedError

    def run(self, ctx: GrindContext, work: Work
            ) -> Iterator[Action | Producer]:
        """
        Yield the action that runs the app on one unit.

        Parameters
        ----------
        ctx : GrindContext
            Context of the run
        work : Work
            The unit and its configuration

        Yields
        ------
        Action
            One action, with a claim on the output paths of the unit;
            it runs the app in the task's own `out_dir` and yields the
            actions that commit what it claimed
        """
        raise NotImplementedError


@dataclass(frozen=True)
class BidsAppPipeline(Pipeline):
    """
    A pipeline that wraps a BIDS App.

    `container` (the image of the app) is required. Its stages are
    derived, not declared: "participant" (level "subject", or "session"
    with a `session_flag`) and, with `group=True`, "group" (level
    "dataset": an app takes one `bids_dir`), after the participants that
    succeeded.

    Attributes
    ----------
    group : bool
        Whether the app has a group level to run
    session_flag : str | None
        Option that restricts a run to one session (`--session-id`);
        None: one task per subject
    claims : tuple[str, ...]
        Templates (`grind.model.format_output`) of what a participant
        task writes, relative to `out_dir`. Empty: `sub-{sub}/` and
        `sub-{sub}.html` for subject tasks, `sub-{sub}/ses-{ses}/` with
        a `session_flag`. Each must use every entity of the level
        (`validate` refuses `sub-{sub}/` with a `session_flag`: session
        tasks would overwrite each other); an app that writes
        per-subject files whatever the session is run per subject
    shared : tuple[str, ...]
        Paths, relative to `out_dir`, that every participant writes
        alike (`sourcedata/freesurfer/fsaverage/`): committed once
    out_subdir : str | None
        Folder of `out_dir` that the app nests its outputs in, if any
    """

    group: bool = False
    session_flag: str | None = None
    claims: tuple[str, ...] = ()
    shared: tuple[str, ...] = ()
    out_subdir: str | None = None
