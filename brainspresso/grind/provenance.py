"""
Provenance of derivatives: `GeneratedBy` and `SourceDatasets`.

Interfaces only (see `brainspresso.grind`). The description of
`derivatives/<pipeline>/` is written when a run is planned and
completed by its `finalize` task.
"""
from __future__ import annotations

from typing import Any, Sequence

from brainspresso.bids.layout import DerivativeLayout
from brainspresso.grind.containers import Container
from brainspresso.grind.graph import DatasetScope
from brainspresso.grind.model import Pipeline

__all__ = ["describe_derivative", "generated_by", "source_datasets"]


def generated_by(pipeline: Pipeline,
                 container: Container | None = None) -> list[dict[str, Any]]:
    """
    Build the `GeneratedBy` field of a derivative.

    Parameters
    ----------
    pipeline : Pipeline
        The pipeline
    container : Container | None
        Image it ran in (default: that of the pipeline)

    Returns
    -------
    list[dict[str, Any]]
        The pipeline (`Name`, `Version`, `Description`, `CodeURL`,
        `Container: {Type, Tag, URI}`), then brainspresso
    """
    raise NotImplementedError


def source_datasets(
    derivative: DerivativeLayout,
    requires: Sequence[str] = (),
    datasets: Sequence[DatasetScope] = (),
) -> list[dict[str, str]]:
    """
    Build the `SourceDatasets` field of a derivative.

    Parameters
    ----------
    derivative : DerivativeLayout
        The derivative dataset
    requires : Sequence[str]
        Required pipelines, whose derivatives are sources too
    datasets : Sequence[DatasetScope]
        Cross-dataset outputs: every input dataset

    Returns
    -------
    list[dict[str, str]]
        `{"URL": <relative URL>}` entries (and `DOI` when known): the
        raw dataset first
    """
    raise NotImplementedError


def describe_derivative(
    derivative: DerivativeLayout,
    pipeline: Pipeline,
    *,
    container: Container | None = None,
    datasets: Sequence[DatasetScope] = (),
) -> dict[str, Any]:
    """
    Build the `dataset_description.json` of a derivative.

    An existing description is completed, not replaced: fields that a
    tool wrote are kept, and `GeneratedBy` entries of other versions
    stay.

    Parameters
    ----------
    derivative : DerivativeLayout
        The derivative dataset
    pipeline : Pipeline
        The pipeline that writes it
    container : Container | None
        Image it runs in
    datasets : Sequence[DatasetScope]
        Cross-dataset outputs: every input dataset

    Returns
    -------
    dict[str, Any]
        `Name`, `BIDSVersion`, `DatasetType: "derivative"`,
        `GeneratedBy`, `SourceDatasets`
    """
    raise NotImplementedError
