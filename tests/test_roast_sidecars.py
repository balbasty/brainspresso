"""Sidecars from templates (`roast.sidecars`) and `roast.helpers.attempt`"""
import io
import json
from types import SimpleNamespace

import pytest
from roast_utils import nii_bytes

from brainspresso.actions import CopyJSON, Fail
from brainspresso.convert import ConvertError
from brainspresso.roast.helpers import attempt
from brainspresso.roast.sidecars import (
    HeaderSidecar,
    TemplateError,
    bold_sidecar,
    copy_sidecar,
    read_template,
    repetition_time,
    template_config,
    template_sidecar,
)


def context(folder, accepts=True):
    """The `resource` and `accepts` of a roast context."""
    return SimpleNamespace(
        resource=lambda name: folder / name if (folder / name).exists()
        else None,
        accepts=lambda kind: accepts)


def run(action) -> dict:
    """Run an action, return its last status."""
    return list(action)[-1]


def test_template_config_find_raises(tmp_path):
    """A `find` that raises `TemplateError` (ABIDE sites) is "none"."""
    (tmp_path / "T1w.json").write_text("{}")

    def find(name):
        if name != "T1w.json":
            raise TemplateError(name)
        return tmp_path / name

    config = template_config(find, ["T1w.json", "bold.json"])
    assert config["T1w.json"][0] == (tmp_path / "T1w.json").as_posix()
    assert config["bold.json"] is None


@pytest.mark.parametrize("content, message", [
    (None, "cannot be read (FileNotFoundError"),
    ("{", "cannot be read (JSONDecodeError"),
    ("[1, 2]", "not a JSON object"),
])
def test_read_template_errors(tmp_path, content, message):
    path = tmp_path / "bold.json"
    if content is not None:
        path.write_text(content)
    data, error = read_template(path, tmp_path / "sub-1_bold", "rest")
    assert data is None
    assert isinstance(error, Fail)
    assert error.message.startswith(f"template {path}: {message}")
    assert (error.name, error.tag) == ("sub-1_bold.json", "rest")


def test_read_template(tmp_path):
    (tmp_path / "T1w.json").write_text('{"EchoTime": 0.003}')
    assert read_template(tmp_path / "T1w.json", tmp_path / "x", "T1w") == \
        ({"EchoTime": 0.003}, None)


def test_copy_sidecar(tmp_path):
    (tmp_path / "T1w.json").write_text("{}")
    actions = list(copy_sidecar(context(tmp_path), "T1w.json",
                                tmp_path / "out" / "sub-1_T1w.json",
                                tag="T1w"))
    assert [type(a) for a in actions] == [CopyJSON]
    assert actions[0].tag == "T1w"
    fail, = copy_sidecar(context(tmp_path), "site/dwi.json",
                         tmp_path / "sub-1_dwi.json", tag="dwi")
    assert isinstance(fail, Fail)
    assert fail.name == "sub-1_dwi.json"
    assert fail.message == "site/dwi.json not found (sidecar template)"
    # (sidecars not written: nothing is missing)
    assert list(copy_sidecar(context(tmp_path, accepts=False), "x.json",
                             tmp_path / "x.json")) == []


def test_repetition_time_policy():
    """The TR policy, in one function"""
    header = lambda: 2.5         # noqa: E731
    none = lambda: None          # noqa: E731
    assert repetition_time({"RepetitionTime": 3}, header) == (3, "template")
    assert repetition_time({"RepetitionTime": 3}) == (3, "template")
    assert repetition_time({}) == (None, "header")
    assert repetition_time({}, header, default=2.) == (2.5, "header")
    assert repetition_time({}, none, default=2.) == (2., "default")
    assert repetition_time({}, none) == (None, "none")


def bold_template(tmp_path, content):
    path = tmp_path / "site" / "bold.json"
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(content))
    return path


def test_bold_sidecar_copy_or_header(tmp_path):
    stem = tmp_path / "sub-1_task-rest_bold"
    full = {"RepetitionTime": 2, "TaskName": "rest"}
    action = bold_sidecar(bold_template(tmp_path, full), full, stem,
                          tmp_path / "x.nii.gz", None, "x.nii.gz")
    assert isinstance(action, CopyJSON)
    action = bold_sidecar(bold_template(tmp_path, {}), {}, stem,
                          tmp_path / "x.nii.gz", None, "x.nii.gz")
    assert isinstance(action, HeaderSidecar)
    assert action.extra == {"TaskName": "rest"}


def test_header_sidecar_tr(tmp_path):
    """The TR of the image written, else of the source; else the
    default (and its hook), else an error naming the sidecar"""
    template = bold_template(tmp_path, {"TaskName": "rest"})
    image = tmp_path / "out" / "sub-1_task-rest_bold.nii.gz"
    image.parent.mkdir()
    source = nii_bytes(shape=(2, 2, 2, 3), tr=2.5)

    def sidecar(**kwargs):
        return HeaderSidecar(template, image.with_name(
            "sub-1_task-rest_bold.json"), image,
            lambda: io.BytesIO(source), "rest.nii.gz", **kwargs)

    out = image.with_name("sub-1_task-rest_bold.json")
    assert run(sidecar())["status"] == "done"      # (from the source)
    assert json.loads(out.read_text())["RepetitionTime"] == 2.5
    image.write_bytes(nii_bytes(shape=(2, 2, 2, 3), tr=0.8))
    assert run(sidecar(ifexists="overwrite"))["status"] == "done"
    assert json.loads(out.read_text())["RepetitionTime"] == 0.8

    image.write_bytes(b"corrupt")
    source = b"corrupt too"
    used = []
    assert run(sidecar(ifexists="overwrite", default_tr=2.,
                       on_default=lambda: used.append(1)))["status"] == "done"
    assert json.loads(out.read_text())["RepetitionTime"] == 2.
    assert used == [1]
    status = run(sidecar(ifexists="overwrite"))
    assert status["status"] == "error"
    assert status["message"].startswith(
        "sub-1_task-rest_bold.json: template bold.json (site) has no "
        "RepetitionTime")


def test_template_sidecar(tmp_path):
    (tmp_path / "T1w.json").write_text("{}")
    action, = template_sidecar(tmp_path / "T1w.json", tmp_path / "sub-1_T1w",
                               "T1w")
    assert isinstance(action, CopyJSON)
    (tmp_path / "T2w.json").write_text("[]")
    fail, = template_sidecar(tmp_path / "T2w.json", tmp_path / "sub-1_T2w",
                             "T2w")
    assert isinstance(fail, Fail)
    assert fail.name == "sub-1_T2w.json"


def test_attempt():
    def read(error):
        raise error

    assert attempt(int, "3", fail_name="x.tsv", fail_tag="meta") == \
        (3, None)
    _, fail = attempt(read, OSError("disk"), fail_name="x.tsv",
                      fail_tag="meta", fail_what="x.csv")
    assert fail.message == "x.csv: cannot be read (OSError: disk)"
    assert (fail.name, fail.tag) == ("x.tsv", "meta")
    # (a `ConvertError` names its file itself)
    _, fail = attempt(read, ConvertError("x.mha: truncated data"),
                      fail_name="sub-1_T2w.nii.gz", fail_tag="subjects")
    assert fail.message == "x.mha: truncated data"
    with pytest.raises(KeyError):
        attempt(read, KeyError("x"), fail_name="x", fail_tag="x")
