"""
What a dataset declares for grind: defaults and exceptions.

Interfaces only (see `brainspresso.grind`). Pure data: a dataset may
declare `grind = GrindOverrides(...)`; nothing else of this package is
for dataset modules. The same fields may also come from the config
file (separate PR), in `[<name>.grind]` and, per pipeline,
`[<name>.grind.<pipeline>]` sections (`[abide1.grind.fs]`),
which win over the declaration.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from brainspresso.grind.model import Query

__all__ = ["GrindOverrides"]


@dataclass(frozen=True)
class GrindOverrides:
    """
    Per-dataset defaults and exceptions of grind.

    Applied by `bx <ds> grind` and, per dataset, by a cross-dataset
    run. They change the effective configuration and the resolved
    inputs, hence the task signatures.

    Attributes
    ----------
    exclude_sub : tuple[str, ...]
        Subjects that are never ground (labels, with or without `sub-`)
    exclude : tuple[str, ...]
        Glob patterns of files never used as inputs (bad scans)
    inputs : Mapping[str, Query]
        Narrows requirements: `{requirement name or suffix: Query}`,
        e.g. which of several T1w to use
        (`{"T1w": Query(entities={"acq": ("mprage",)})}`)
    species : str
        Species of the participants (pipelines pick templates from it)
    template : str | None
        Default standard space (a TemplateFlow identifier)
    config : Mapping[str, Mapping[str, Any]]
        Configuration defaults per pipeline: `{pipeline: {field: value}}`
    resources : Mapping[str, Mapping[str, Any]]
        Resource overrides per `"<pipeline>/<stage>"` (large images)
    """

    exclude_sub: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    inputs: Mapping[str, Query] = field(default_factory=dict)
    species: str = "homo sapiens"
    template: str | None = None
    config: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    resources: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
