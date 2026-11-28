"""
Roast helpers shared by ABIDE-I and ABIDE-II.

* Archives: `ARCHIVE_ERRORS` (a corrupt or truncated gz tarball),
  `member_parts` (normalised member names), `image_ext`.
* Sidecars: `TemplateError`, `template_config` (the templates of a
  shard, in its signature), `scan_actions` (an image, then its sidecar),
  `bold_sidecar`, `HeaderSidecar` and `header_tr` (a bold template
  without `RepetitionTime` or `TaskName`; also used by COBRE).
* Metadata: `roast_meta` (dataset files, participants and phenotype
  tables from the phenotypic CSV files).
"""
from __future__ import annotations

import gzip
import hashlib
import re
import tarfile
import zlib
from functools import partial
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import IO, Callable, Iterator, Mapping

import nibabel as nib

from brainspresso.actions import Action, CopyJSON, Fail, WriteJSON
from brainspresso.datasets.indi.abide.tables import Table, write_table
from brainspresso.roast import RoastContext, copy_templates
from brainspresso.utils.io import read_json, write_json

lg = getLogger(__name__)

__all__ = [
    "ARCHIVE_ERRORS",
    "NIFTI",
    "HeaderSidecar",
    "TemplateError",
    "bold_sidecar",
    "header_tr",
    "image_ext",
    "member_parts",
    "read_template",
    "roast_meta",
    "scan_actions",
    "template_config",
]

NIFTI = (".nii.gz", ".nii")

#: What reading a corrupt or truncated (gz) tarball raises
ARCHIVE_ERRORS = (tarfile.TarError, EOFError, zlib.error, OSError)

#: plausible RepetitionTime (s) read from a NIfTI header
TR_RANGE: tuple[float, float] = (0.1, 30.)


class TemplateError(LookupError):
    """No sidecar template for a (site, template) pair."""


def image_ext(name: str) -> str | None:
    """`.nii.gz`/`.nii` of an image name, in any case (None: not one)."""
    lower = name.lower()
    return next((e for e in NIFTI if lower.endswith(e)), None)


def member_parts(name: str) -> tuple[str, ...] | None:
    """
    Normalised parts of an archive member name.

    Parameters
    ----------
    name : str
        Member name (`./ABIDEII-NYU_1/...`, `/ABIDEII-NYU_1/...`)

    Returns
    -------
    tuple[str] | None
        Its parts without `.`, empty parts or a leading `/` (a member
        never names an absolute path); None if a part is `..`
    """
    parts = tuple(p for p in PurePosixPath(name.replace("\\", "/")).parts
                  if p not in ("/", "."))
    if ".." in parts:
        return None
    return parts


def template_config(find: Callable[[str], Path], names, site: str) -> dict:
    """
    Describe the sidecar templates of a site (in a shard's signature).

    Parameters
    ----------
    find : callable(str) -> Path
        Finds a template by name (raises `TemplateError` if none)
    names : list[str]
        Names of the templates a shard may use
    site : str
        Site name, in the spelling of the data

    Returns
    -------
    dict
        `{"site": site, "templates": {name: [path, sha256] | None}}`: a
        changed template re-runs the shard
    """
    out = {}
    for name in names:
        try:
            path = Path(find(name))
        except TemplateError:
            out[name] = None
            continue
        try:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as e:            # (a `Fail` of its sidecars)
            digest = f"unreadable: {type(e).__name__}"
        out[name] = [path.as_posix(), digest]
    return {"site": site, "templates": out}


def header_tr(fileobj: IO[bytes], name: str = "") -> float | None:
    """
    Read the RepetitionTime of a NIfTI-1 header.

    Parameters
    ----------
    fileobj : file
        Opened image (gzip-compressed if `name` ends with `.gz`)
    name : str
        Its name

    Returns
    -------
    float | None
        `pixdim[4]` in seconds (time unit of `xyzt_units`); None if
        unset, of unknown unit or not plausible (`TR_RANGE`)
    """
    if name.endswith(".gz"):
        fileobj = gzip.GzipFile(fileobj=fileobj)
    hdr = nib.Nifti1Header.from_fileobj(fileobj)
    if hdr["dim"][0] < 4:
        return None
    scale = {"sec": 1., "msec": 1e-3, "usec": 1e-6}.get(
        hdr.get_xyzt_units()[1])
    if scale is None:
        return None
    tr = float(hdr["pixdim"][4]) * scale
    lo, hi = TR_RANGE
    return round(tr, 6) if lo <= tr <= hi else None


class HeaderSidecar(WriteJSON):
    """
    A bold sidecar whose template lacks REQUIRED fields.

    `RepetitionTime`, if the template has none, is read from the NIfTI
    header when the action runs: from the image written next to it if
    there is one, else from the source (`--sidecars only`: the member
    being read, so that the archive is still read forward only). If the
    header gives none: `default_tr` (and `on_default()` is called), else
    an error. The fields of `extra` (`TaskName`) that the template lacks
    are added.

    Parameters
    ----------
    template : path
        Sidecar template
    dst : path
        Output sidecar
    image : path
        Output image
    open_image : callable() -> file
        Opens the source image
    image_name : str
        Name of the source image (`.gz`: compressed)
    extra : dict, optional
        Fields added if the template lacks them (`TaskName`)
    default_tr : float, optional
        RepetitionTime if the header gives none (default: an error)
    on_default : callable(), optional
        Called when `default_tr` is used (a count of the dataset)
    **kwargs
        Options of `WriteJSON`
    """

    def __init__(self, template, dst, image, open_image: Callable,
                 image_name: str, extra: Mapping | None = None,
                 default_tr: float | None = None,
                 on_default: Callable[[], None] | None = None, **kwargs):
        self.template = Path(template)
        self.image = Path(image)
        self.open_image = open_image
        self.image_name = image_name
        self.extra = dict(extra or {})
        self.default_tr = default_tr
        self.on_default = on_default
        super().__init__({}, dst, src=template, **kwargs)

    def action(self, file):
        """Write the template, with the RepetitionTime of the header."""
        meta = read_json(self.template)
        meta = {**meta, **{k: v for k, v in self.extra.items()
                           if k not in meta}}
        if "RepetitionTime" in meta:
            return write_json(meta, file)
        try:
            if self.image.is_file():
                with open(self.image, "rb") as f:
                    tr = header_tr(f, self.image.name)
            else:
                with self.open_image() as f:
                    tr = header_tr(f, self.image_name)
        except Exception as e:
            lg.debug(f"{self.image_name}: {e}")
            tr = None
        if tr is None and self.default_tr is not None:
            tr = self.default_tr
            if self.on_default is not None:
                self.on_default()
        if tr is None:
            raise ValueError(
                f"{Path(self.dst[0]).name}: template {self.template.name} "
                f"({self.template.parent.name}) has no RepetitionTime "
                f"(REQUIRED) and the NIfTI header of {self.image_name} "
                f"gives none")
        return write_json({**meta, "RepetitionTime": tr}, file)


def bold_fields(stem: str | Path) -> dict:
    """
    Give the REQUIRED fields of a bold sidecar that its name tells.

    Parameters
    ----------
    stem : path
        Output path of the scan, without extension

    Returns
    -------
    dict
        `{"TaskName": <label of the task entity>}` (empty without one)
    """
    match = re.search(r"(?:^|_)task-([A-Za-z0-9]+)(?:_|$)", Path(stem).name)
    return {"TaskName": match[1]} if match else {}


def bold_sidecar(template: Path, content: dict, stem: Path, image: Path,
                 open_image: Callable, image_name: str, *,
                 default_tr: float | None = None,
                 on_default: Callable[[], None] | None = None,
                 **kwargs) -> Action:
    """
    Make the sidecar of a bold image, with its REQUIRED fields.

    A copy of the template if it has them all; else a `HeaderSidecar`
    that adds `TaskName` (from the task entity of the name) and reads
    `RepetitionTime` from the NIfTI header if the template has none.

    Parameters
    ----------
    template : Path
        Sidecar template
    content : dict
        Its content
    stem : Path
        Output path of the scan, without extension
    image : Path
        Output image
    open_image : callable() -> file
        Opens the source image
    image_name : str
        Name of the source image
    default_tr, on_default
        RepetitionTime if the header gives none, and its hook
        (`HeaderSidecar`; default: an error)
    **kwargs
        Options of the action (`tag`)

    Returns
    -------
    Action
    """
    extra = {k: v for k, v in bold_fields(stem).items() if k not in content}
    if "RepetitionTime" in content and not extra:
        return CopyJSON(template, f"{stem}.json", **kwargs)
    return HeaderSidecar(template, f"{stem}.json", image, open_image,
                         image_name, extra=extra, default_tr=default_tr,
                         on_default=on_default, **kwargs)


def read_template(path: Path, stem: Path,
                  key: str) -> tuple[dict | None, Fail | None]:
    """
    Read a sidecar template.

    Parameters
    ----------
    path : Path
        Template
    stem : Path
        Output path of the scan, without extension
    key : str
        Key of the scan (progress tag)

    Returns
    -------
    content : dict | None
        Its content (None if it cannot be read)
    error : Fail | None
        A `Fail` of the sidecar if it cannot be read or is not a JSON
        object
    """
    try:
        content = read_json(path)
    except (OSError, ValueError) as e:
        message = f"cannot be read ({type(e).__name__}: {e})"
    else:
        if isinstance(content, dict):
            return content, None
        message = "not a JSON object"
    return None, Fail(f"template {path}: {message}",
                      name=f"{stem.name}.json", tag=key)


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
        `TaskName`, `bold_sidecar`)

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
    template, error = read_template(path, stem, key)
    if error is not None:
        yield error
        return
    if bold:
        yield bold_sidecar(path, template, stem, image, open_image,
                           image_name, tag=key)
        return
    yield CopyJSON(path, f"{stem}.json", tag=key)


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
