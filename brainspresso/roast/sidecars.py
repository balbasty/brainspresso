"""
Sidecars written from templates.

* Templates in signatures: `template_config(find, names)`, `{name:
  [path, sha256] | None}`, for the `config` of a shard;
* reading a template: `read_template` (a `Fail` of the sidecar if it
  cannot be read), `TemplateError` (no template, from a `find`);
* writing a sidecar: `copy_sidecar` (a template found with
  `ctx.resource`, else a `Fail` named after the sidecar),
  `template_sidecar` (a template already found and read: a copy, or a
  bold sidecar);
* bold sidecars (`RepetitionTime` and `TaskName` are REQUIRED):
  `bold_sidecar`, `HeaderSidecar`, and the TR policy in one function,
  `repetition_time` (the template's, else the NIfTI header's,
  `convert.nifti.header_tr`, else a documented default, else an error).

```python
yield ExtractMember(tar.archive, member.info, image, tag=key)
yield from copy_sidecar(ctx, "T1w.json", f"{stem}.json", tag=key)
```
"""
from __future__ import annotations

import hashlib
import re
from logging import getLogger
from pathlib import Path
from typing import Callable, Iterator, Mapping

from brainspresso.actions import Action, CopyJSON, Fail, WriteJSON
from brainspresso.convert.nifti import header_tr
from brainspresso.roast.context import RoastContext
from brainspresso.roast.helpers import attempt
from brainspresso.utils.io import read_json, write_json

lg = getLogger(__name__)

__all__ = [
    "HeaderSidecar",
    "TemplateError",
    "bold_fields",
    "bold_sidecar",
    "copy_sidecar",
    "read_template",
    "repetition_time",
    "template_config",
    "template_sidecar",
]


class TemplateError(LookupError):
    """No sidecar template (e.g. for a site and a template name)."""


def template_config(find: Callable[[str], Path | None],
                    names) -> dict:
    """
    Describe sidecar templates, for the `config` of a shard.

    Templates are signed by their content in the shard's `config`, never
    listed in its `inputs`: a changed template re-runs the shards that
    use it, and a template found elsewhere (`sourcedata/` instead of the
    package) re-runs them too.

    Parameters
    ----------
    find : callable(str) -> Path | None
        Finds a template by name (`ctx.resource`, or a site's own
        lookup): None, or `TemplateError`, if there is none
    names : list[str]
        Names of the templates that a shard may use

    Returns
    -------
    dict
        `{name: [path, sha256] | None}` (None: not found; a template
        that cannot be read has the digest `"unreadable: <Type>"`, its
        sidecars fail when written)
    """
    out = {}
    for name in names:
        try:
            path = find(name)
        except TemplateError:
            path = None
        if path is None:
            out[name] = None
            continue
        path = Path(path)
        try:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as e:            # (a `Fail` of its sidecars)
            digest = f"unreadable: {type(e).__name__}"
        out[name] = [path.as_posix(), digest]
    return out


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
    name = f"{Path(stem).name}.json"
    content, error = attempt(read_json, path, name=name, tag=key,
                             what=f"template {path}")
    if error is not None:
        return None, error
    if isinstance(content, dict):
        return content, None
    return None, Fail(f"template {path}: not a JSON object", name=name,
                      tag=key)


def copy_sidecar(ctx: RoastContext, name: str, dst: str | Path, *,
                 tag: str | None = None) -> Iterator[Action]:
    """
    Copy a sidecar template found with `ctx.resource`.

    Parameters
    ----------
    ctx : RoastContext
        Context of the roast
    name : str
        Name of the template (`T1w.json`, `<site>/dwi.json`)
    dst : path
        Output sidecar
    tag : str, optional
        Progress group (default: that of the producer)

    Yields
    ------
    Action
        A `CopyJSON` of the template; if there is none, a `Fail` named
        after the sidecar when sidecars are written (else nothing)
    """
    template = ctx.resource(name)
    if template is not None:
        yield CopyJSON(template, dst, tag=tag)
    elif ctx.accepts("sidecar"):
        yield Fail(f"{name} not found (sidecar template)",
                   name=Path(dst).name, tag=tag)


def template_sidecar(
    template: Path, stem: Path, key: str, *, bold: bool = False,
    image: Path | None = None, open_image: Callable | None = None,
    image_name: str = "", default_tr: float | None = None,
    on_default: Callable[[], None] | None = None,
) -> Iterator[Action]:
    """
    Write the sidecar of a scan from its template.

    Parameters
    ----------
    template : Path
        The template (found)
    stem : Path
        Output path of the scan, without extension
    key : str
        Key of the scan (progress tag)
    bold : bool
        A bold image (`bold_sidecar`: the other arguments are its own)
    image, open_image, image_name, default_tr, on_default
        See `bold_sidecar`

    Yields
    ------
    Action
        A copy of the template (bold: `bold_sidecar`); a `Fail` of the
        sidecar if the template cannot be read or is not a JSON object
    """
    content, error = read_template(template, stem, key)
    if error is not None:
        yield error
        return
    if bold:
        yield bold_sidecar(template, content, stem, image, open_image,
                           image_name, default_tr=default_tr,
                           on_default=on_default, tag=key)
        return
    yield CopyJSON(template, f"{stem}.json", tag=key)


def repetition_time(
    template: Mapping,
    header: Callable[[], float | None] | None = None,
    default: float | None = None,
) -> tuple[float | None, str]:
    """
    Choose the RepetitionTime of a bold sidecar: the TR policy.

    The one place that decides it. Today: the template's if it has one
    (the header is not read), else that of the NIfTI header, else
    `default` (a TR documented for the dataset, COBRE), else none (an
    error of the sidecar, ABIDE).

    Parameters
    ----------
    template : dict
        Content of the template
    header : callable() -> float | None, optional
        Reads the TR of the NIfTI header (`convert.nifti.header_tr`;
        None: not read yet)
    default : float, optional
        TR if the header gives none

    Returns
    -------
    tr : float | None
        The RepetitionTime (None: none, or the header must be read)
    source : {"template", "header", "default", "none"}
        Where it comes from; `"header"` with `tr` None: call again with
        `header`; `"none"`: no TR
    """
    if "RepetitionTime" in template:
        return template["RepetitionTime"], "template"
    if header is None:
        return None, "header"
    tr = header()
    if tr is not None:
        return tr, "header"
    if default is not None:
        return default, "default"
    return None, "none"


class HeaderSidecar(WriteJSON):
    """
    A bold sidecar whose template lacks REQUIRED fields.

    `RepetitionTime` is chosen by `repetition_time` when the action
    runs; a NIfTI header is read from the image written next to it if
    there is one, else from the source (`--sidecars only`: the member
    being read, so that the archive is still read forward only). If
    neither gives one: `default_tr` (and `on_default()` is called), else
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

    def header_tr(self) -> float | None:
        """Read the TR of the NIfTI header (None if unreadable or none)."""
        try:
            if self.image.is_file():
                with open(self.image, "rb") as f:
                    return header_tr(f, self.image.name)
            with self.open_image() as f:
                return header_tr(f, self.image_name)
        except Exception as e:
            lg.debug(f"{self.image_name}: {e}")
            return None

    def action(self, file):
        """Write the template, with the RepetitionTime of the policy."""
        meta = read_json(self.template)
        meta = {**meta, **{k: v for k, v in self.extra.items()
                           if k not in meta}}
        tr, source = repetition_time(meta, self.header_tr, self.default_tr)
        if source == "template":
            return write_json(meta, file)
        if source == "default" and self.on_default is not None:
            self.on_default()
        if tr is None:
            raise ValueError(
                f"{self.declared[0].name}: template {self.template.name} "
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

    A copy of the template if it has them all (`repetition_time` takes
    its TR without the header); else a `HeaderSidecar` that adds
    `TaskName` (from the task entity of the name) and the TR of the
    policy.

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
    if not extra and repetition_time(content)[1] == "template":
        return CopyJSON(template, f"{stem}.json", **kwargs)
    return HeaderSidecar(template, f"{stem}.json", image, open_image,
                         image_name, extra=extra, default_tr=default_tr,
                         on_default=on_default, **kwargs)
