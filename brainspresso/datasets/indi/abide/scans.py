"""
Roast helpers shared by ABIDE-I and ABIDE-II.

* Scans: `scan_actions` (an image, then its sidecar). The pieces it is
  made of have their own homes since step 6: `convert.nifti`
  (`image_ext`, `header_tr`), `roast.sidecars` (`TemplateError`,
  `template_config`, `read_template`, `bold_sidecar`, `HeaderSidecar`),
  `roast.archives` (reading the archives).
* Metadata: `roast_meta` (dataset files, participants and phenotype
  tables from the phenotypic CSV files).
"""
from __future__ import annotations

from functools import partial
from logging import getLogger
from pathlib import Path
from typing import Callable, Iterator, Mapping

from brainspresso.actions import Action, Fail
from brainspresso.datasets.indi.abide.tables import Table, write_table
from brainspresso.roast import RoastContext, copy_templates
from brainspresso.roast.sidecars import TemplateError, template_sidecar

lg = getLogger(__name__)

__all__ = ["roast_meta", "scan_actions"]


def scan_actions(
    ctx: RoastContext, stem: Path, ext: str, key: str,
    find: Callable[[], Path], extract: Callable[[Path], Action],
    open_image: Callable, image_name: str, *, bold: bool = False,
) -> Iterator[Action]:
    """
    Convert one scan: the image, then its sidecar.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    stem : Path
        Output path without extension (`.../sub-<label>_T1w`)
    ext : str
        Extension of the image (`.nii.gz`, `.nii`)
    key : str
        Key of the scan (progress tag)
    find : callable() -> Path
        Finds the sidecar template (raises `TemplateError` if none)
    extract : callable(dst) -> Action
        Action that writes the image to `dst`
    open_image : callable() -> file
        Opens the source image (bold templates without RepetitionTime)
    image_name : str
        Name of the source image
    bold : bool
        A bold image (its sidecar requires `RepetitionTime` and
        `TaskName`, `roast.sidecars.bold_sidecar`)

    Yields
    ------
    Action
        The image, then its sidecar (or a `Fail` naming the missing
        template, when sidecars are written)
    """
    image = Path(f"{stem}{ext}")
    yield extract(image)
    if not ctx.accepts("sidecar"):
        return
    try:
        path = find()
    except TemplateError as e:
        yield Fail(f"{image_name}: {e}", name=f"{stem.name}.json", tag=key)
        return
    yield from template_sidecar(path, stem, key, bold=bold, image=image,
                                open_image=open_image, image_name=image_name)


def roast_meta(ctx: RoastContext, csvs: list[Path], missing: str,
               participants: Table, phenotypes: Mapping[str, Table], *,
               pristine_phenotypes: bool = False) -> Iterator[Action]:
    """
    Write the dataset files, the participants and phenotype tables.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast (keys `meta`, `pheno`)
    csvs : list[Path]
        Phenotypic CSV files (empty: they are missing)
    missing : str
        What is missing if `csvs` is empty (message of the `Fail`)
    participants : Table
        The participants table
    phenotypes : dict[str, Table]
        The phenotype tables (`phenotype/<name>.tsv`)
    pristine_phenotypes : bool
        Write the phenotype tables to `.brainspresso/orig/` (trimmed to
        the subjects on disk, as the participants table), else
        directly

    Yields
    ------
    Action
        Copies of the templates, the pristine participants table, then
        the phenotype tables and their data dictionaries
    """
    raw = ctx.layout.raw
    if ctx.wants("meta"):
        yield from copy_templates(ctx, ["README", "dataset_description.json",
                                        "participants.json"])
        if csvs:
            yield Action(
                csvs, ctx.layout.original(raw / "participants.tsv"),
                partial(write_table, participants, csvs),
                mode="t", input="path", tag="meta",
            )
        else:
            yield Fail(f"{missing} not found", name="participants.tsv",
                       tag="meta")
    if ctx.wants("pheno"):
        folder = ctx.layout.phenotype
        if not csvs:
            yield Fail(f"{missing} not found", name="phenotype",
                       tag="pheno")
        for name, table in phenotypes.items() if csvs else ():
            dst = folder / f"{name}.tsv"
            yield Action(
                csvs, ctx.layout.original(dst) if pristine_phenotypes
                else dst,
                partial(write_table, table, csvs),
                mode="t", input="path", tag="pheno",
            )
        yield from copy_templates(
            ctx, {f"phenotype/{name}.json": folder / f"{name}.json"
                  for name in phenotypes}, tag="pheno")
