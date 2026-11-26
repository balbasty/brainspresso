"""
Grind on the action engine: its context and its policy.

Interfaces only (see `brainspresso.grind`). As `roast.RoastContext` and
`roast.RoastPolicy` do for roast: a task of the graph is one shard of
the engine whose producer is `Stage.run`, so reporting, task states,
file locks and the submit seam are shared with roast.
"""
from __future__ import annotations

from pathlib import Path
from typing import Mapping, Sequence

from brainspresso.actions.action import Action
from brainspresso.actions.engine import Context, Policy
from brainspresso.bids.layout import DerivativeLayout, Layout
from brainspresso.grind.containers import ContainerRuntime
from brainspresso.grind.execution import Profile
from brainspresso.grind.model import Config, Pipeline
from brainspresso.grind.overrides import GrindOverrides

__all__ = ["GrindContext", "GrindPolicy"]


class GrindContext(Context):
    """
    What a stage needs to know about the run, for one dataset.

    The `actions.engine.Context` of grind: `root` and `state` are those
    of the layout, `stage` is "grind", and task records pick up
    `pipeline`, which is therefore the **name** of the pipeline (the
    engine stores `ctx.pipeline` as it is); the `Pipeline` itself is
    `spec`. It is pickled to workers and cluster jobs: it holds no open
    resource.

    The other fields that grind adds to task records
    (`grind.graph.GRIND_FIELDS`) are not read from the context: each
    task gives them to the engine as the extra record fields of its
    shard (`Shard(record=...)`, step 7), and the engine merges them,
    with the fields it does not own, into the record that exists (the
    "pending" record written at submission) instead of replacing it.

    Parameters
    ----------
    layout : Layout
        Layout of the dataset (the home of the run for group tasks)
    pipeline : Pipeline
        The pipeline that runs (kept as `spec`)
    config : Config | None
        Effective configuration for this dataset
    overrides : GrindOverrides | None
        What the dataset declares
    profile : Profile | None
        Execution profile (backend, runtime, scratch)
    dataset : str | None
        Reference of the dataset
    variant : str | None
        Variant of the derivative (`derivatives/<pipeline>-<variant>/`)
    variants : Mapping[str, str | None] | None
        Variant of each required pipeline (`Scope.variants`)
    locks : Sequence[Path]
        State folders whose grind lock a task of this context holds
        shared while it runs, in the order they are taken (parents
        before sites): the dataset and its sites parent; for the home
        of a run, also every site or dataset that group tasks read

    Attributes
    ----------
    stage : str
        "grind" (task states)
    pipeline : str
        Name of the pipeline (task states)
    spec : Pipeline
        The pipeline
    """

    stage = "grind"

    def __init__(
        self,
        layout: Layout,
        pipeline: Pipeline,
        config: Config | None = None,
        *,
        overrides: GrindOverrides | None = None,
        profile: Profile | None = None,
        dataset: str | None = None,
        variant: str | None = None,
        variants: Mapping[str, str | None] | None = None,
        locks: Sequence[Path] = (),
    ):
        raise NotImplementedError

    @property
    def derivative(self) -> DerivativeLayout:
        """Layout of the derivative dataset that the pipeline writes."""
        raise NotImplementedError

    @property
    def runtime(self) -> ContainerRuntime:
        """Container runtime of the profile."""
        raise NotImplementedError

    def required(self, pipeline: str) -> DerivativeLayout:
        """
        Return the derivative of a required pipeline.

        In the variant that the run chose for it (`variants`), else
        its default variant (`derivatives/<pipeline>/`); never the
        variant of the pipeline that runs.

        Parameters
        ----------
        pipeline : str
            A name listed in `Pipeline.requires`

        Returns
        -------
        DerivativeLayout
            Its derivative dataset, in the same dataset

        Raises
        ------
        KeyError
            If the pipeline does not require it
        """
        raise NotImplementedError

    def scratch(self, task: str) -> Path:
        """
        Return the scratch folder of a task.

        Parameters
        ----------
        task : str
            Task id

        Returns
        -------
        Path
            A folder under the scratch of the profile, created empty
        """
        raise NotImplementedError


class GrindPolicy(Policy):
    """
    Acceptance policy of grind.

    Every action of a task that runs is accepted, with `IfExists`
    "overwrite" unless it sets its own: whether a unit is skipped is
    decided for the whole task, from its signature (`grind.graph`), and
    the roast default "skip" would keep stale files under a new
    signature. Outputs outside the derivative dataset of the task (or
    its scratch folder) are rejected.
    """

    def accept(self, action: Action, ctx: GrindContext, *,
               staging: bool = False) -> bool:
        """
        Decide whether an action of a task runs.

        Parameters
        ----------
        action : Action
            The action
        ctx : GrindContext
            Context of the run
        staging : bool
            Whether the action is staging

        Returns
        -------
        bool
            True, unless an output is outside what the task may write

        Raises
        ------
        Rejected
            If an output is outside the derivative and scratch folders
        """
        raise NotImplementedError

    def prepare(self, action: Action, ctx: GrindContext) -> None:
        """
        Set `IfExists` "overwrite" on an action that does not set one.

        Parameters
        ----------
        action : Action
            The action (accepted)
        ctx : GrindContext
            Context of the run
        """
        raise NotImplementedError

    def fingerprint(self, ctx: GrindContext) -> dict:
        """
        Describe the run for the signature of shards without one.

        Tasks carry their own signature (`grind.graph.signature`); this
        is for the shards that a stage yields inside its task.

        Parameters
        ----------
        ctx : GrindContext
            Context of the run

        Returns
        -------
        dict
            `{"pipeline": ..., "version": ..., "config": ...}`
        """
        raise NotImplementedError
