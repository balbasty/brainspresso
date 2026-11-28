"""Shared roaster scaffolding: `python_api`, `named_keys`, `missing_input`,
`RoastContext.wants_any`, `freesurfer.archive`."""
import importlib
import inspect
import io
import logging
import pickle
import tarfile
from pathlib import Path, PurePosixPath
from types import SimpleNamespace

import pytest

from brainspresso.actions.action import Fail
from brainspresso.actions.archive import ExtractMembers
from brainspresso.bids.keys import KeyAliases
from brainspresso.datasets.bullitt import roaster as bullitt
from brainspresso.datasets.indi.abide.i import roaster as abide1
from brainspresso.datasets.oasis.i import roaster as oasis1
from brainspresso.datasets.oasis.ii import roaster as oasis2
from brainspresso.errors import UsageError
from brainspresso.freesurfer import archive as fsarchive
from brainspresso.roast import archives, run
from brainspresso.roast.context import RoastContext, as_list
from brainspresso.roast.helpers import missing_input, named_keys
from brainspresso.sources.base import subject_labels

#: Read by `python_api` at each call (module `TEMPLATES`)
TEMPLATES = None

#: Every ported dataset and its `roast()`
PORTED = [
    "indi.abide.i", "indi.abide.ii", "indi.cobre", "oasis.i", "oasis.ii",
    "oasis.iii", "ixi", "bullitt", "unc_wisconsin",
]


# ----------------------------------------------------------------------
#   named_keys, missing_input, wants_any
# ----------------------------------------------------------------------

@pytest.mark.parametrize("keys, named", [
    ((), []),
    (None, []),
    # the CLI default, spelled out (in any case): nothing named
    (bullitt.ROAST_SPEC.default, []),
    ([k.upper() for k in bullitt.ROAST_SPEC.default], []),
    ("T2w", ["T2w"]),
    (["RAW"], ["T2w", "angio", "flash", "mprage"]),
    (["derivatives", "meta"], ["skullstripped", "vessels"]),
])
def test_named_keys(keys, named):
    leaves = (*bullitt.RAW, *bullitt.DERIVATIVES)
    assert named_keys(bullitt.ROAST_SPEC, keys, leaves) == named


@pytest.mark.parametrize("keys, named", [
    ((), False), (["T1w"], False), (["fs"], True), (["FS-ALL"], True),
    (["derivatives"], True), (["all"], True),
])
def test_named_keys_abide1(keys, named):
    """ABIDE-I `fs_named`: `named_keys` without a default"""
    assert abide1.roast_options(keys) == {"fs_named": named}


def test_missing_input_required():
    out = list(missing_input("x.tar not found", name="x.tar", tag="discs",
                             required=True))
    assert len(out) == 1 and isinstance(out[0], Fail)
    assert (out[0].name, out[0].tag) == ("x.tar", "discs")
    assert "x.tar not found" in out[0].message


def test_missing_input_skipped(caplog):
    logger = logging.getLogger("brainspresso.datasets.test")
    with caplog.at_level(logging.INFO):
        assert list(missing_input("x.tar not found", name="x.tar",
                                  required=False, logger=logger)) == []
        assert list(missing_input("y not found", name="y", required=False,
                                  info="no y", level=logging.WARNING)) == []
    records = [(r.name, r.levelno, r.getMessage()) for r in caplog.records]
    assert records == [
        ("brainspresso.datasets.test", logging.INFO,
         "x.tar not found (skipped)"),
        ("brainspresso.roast.helpers", logging.WARNING, "no y"),
    ]


def test_wants_any():
    ctx = SimpleNamespace(wants={"fs"}.__contains__)
    assert RoastContext.wants_any(ctx, ("T1w", "fs"))
    assert not RoastContext.wants_any(ctx, ("T1w", "fs-all"))
    assert not RoastContext.wants_any(ctx, ())


# ----------------------------------------------------------------------
#   python_api
# ----------------------------------------------------------------------

KEYS = KeyAliases({"meta", "T1w", "T2w"})


def roast_a(ctx):
    """A roaster"""
    return iter(())


def roast_b(ctx):
    """Another roaster"""
    return iter(())


class Spec:
    """A `KeySpec` that records what it is given."""

    def effective(self, keys, exclude):
        return ("effective", *keys), ("excluded", *exclude)


def extra_options(keys, discs=None, *, level=1):
    """Options of a fake dataset"""
    return {"keys": list(keys), "discs": discs, "level": level}


@pytest.fixture
def calls(monkeypatch):
    """The arguments of `roast_dataset`."""
    out = []

    def fake(roaster, root, aliases, **kwargs):
        out.append({"roaster": roaster, "root": root, "aliases": aliases,
                    **kwargs})
        return "report"

    monkeypatch.setattr(run, "roast_dataset", fake)
    return out


def test_python_api_subjects(calls, tmp_path):
    """Numbers, labels with or without `sub-`, ranges and files, as on
    the command line, with the width of the dataset"""
    roast = run.python_api(roast_a, KEYS, folder="X", width=3)
    listed = tmp_path / "subs.txt"
    listed.write_text("sub-009\n10\n")
    assert roast("root", sub=[1, "sub-2", "003", "5:7", str(listed)],
                 exclude_sub="12") == "report"
    call, = calls
    assert call["sub"] == ["001", "002", "003", "005", "006", "009", "010"]
    assert call["exclude_sub"] == ["012"]
    # labels verbatim without a width
    run.python_api(roast_a, KEYS, folder="X")("root", sub=["sub-a1", "07"])
    assert calls[-1]["sub"] == ["07", "a1"]


@pytest.mark.parametrize("old,new", [
    ("subs", "sub"), ("exclude_subs", "exclude_sub"), ("sessions", "ses"),
    ("datatypes", "datatype"), ("suffixes", "suffix"), ("tasks", "task"),
])
def test_python_api_refuses_old_names(calls, old, new):
    """The selection arguments are named after the BIDS entities (`sub`,
    `exclude_sub`, `ses`...): the old plural names are unknown keywords,
    and the error names the new one"""
    roast = run.python_api(roast_a, KEYS, folder="X", width=3)
    with pytest.raises(TypeError, match=f"unexpected keyword argument "
                                        f"'{old}' \\(use '{new}'\\)"):
        roast("root", **{old: ["1"]})
    assert not calls


def test_python_api_bids_filters(calls):
    """`ses=`, `datatype=`, `suffix=`, `task=`: the `Selection` filters
    of the same names (a value or a list)"""
    roast = run.python_api(roast_a, KEYS, folder="X", width=3)
    roast("root", ses=["1"], datatype="anat", suffix=["T1w", "T2w"],
          task="rest")
    call, = calls
    assert call["ses"] == ["1"] and call["datatype"] == ["anat"]
    assert call["suffix"] == ["T1w", "T2w"] and call["task"] == ["rest"]
    # not given: not passed (the defaults of `Selection`)
    roast("root")
    assert not {"ses", "datatype", "suffix", "task"} & set(calls[-1])


def test_python_api_subject_paths(calls, tmp_path):
    """A file of subjects as an `os.PathLike`, alone or in a list"""
    listed = tmp_path / "subs.txt"
    listed.write_text("sub-009\n")
    roast = run.python_api(roast_a, KEYS, folder="X", width=3)
    roast("root", sub=listed, exclude_sub=[listed])
    assert (calls[-1]["sub"], calls[-1]["exclude_sub"]) == (["009"],
                                                              ["009"])
    run.python_api(roast_a, KEYS, folder="X")("root", sub=[listed])
    assert calls[-1]["sub"] == ["009"]
    assert as_list(listed) == [str(listed)]
    assert subject_labels(listed, 3) == subject_labels([listed], 3) == {
        "009"}


def test_python_api_refuses_options(calls):
    """`options=` with an options function: a `TypeError` (the old
    functions passed `options=` twice); without one, passed on"""
    roast = run.python_api(roast_a, KEYS, folder="X",
                           options=extra_options)
    with pytest.raises(TypeError, match="'options'.*`discs`, `level`"):
        roast("root", options={"discs": [1]})
    assert not calls
    run.python_api(roast_a, KEYS, folder="X")("root", options={"a": 1})
    assert calls[-1]["options"] == {"a": 1}


def test_python_api_keys_and_options(calls):
    roast = run.python_api(roast_a, KEYS, folder="X", spec=Spec(),
                           options=extra_options)
    roast("root", keys="T1w", exclude_keys=["T2w"], discs=[2], json="no")
    call, = calls
    assert call["roaster"] is roast_a
    assert call["keys"] == ("effective", "T1w")
    assert call["exclude_keys"] == ("excluded", "T2w")
    # (`options` sees `--keys` as given, not `spec.effective`)
    assert call["options"] == {"keys": ["T1w"], "discs": [2], "level": 1}
    assert call["json"] == "no"
    # without a spec: the keys as given (a list)
    run.python_api(roast_a, KEYS, folder="X")("root", keys="T1w")
    assert (calls[-1]["keys"], calls[-1]["exclude_keys"]) == (["T1w"], [])
    assert "options" not in calls[-1]


def test_python_api_sources(calls):
    roasters = {"one": roast_a, "two": roast_b}
    roast = run.python_api(roasters, KEYS, folder="X", default_source="one")
    roast("root")
    roast("root", source="two")
    assert [c["roaster"] for c in calls] == [roast_a, roast_b]
    with pytest.raises(ValueError, match="Unknown source 'three'"):
        roast("root", source="three")
    assert list(inspect.signature(roast).parameters)[:2] == ["root",
                                                             "source"]


def test_python_api_templates(calls, monkeypatch, tmp_path):
    """The `TEMPLATES` of the module, read at each call (patchable)"""
    roast = run.python_api(roast_a, KEYS, folder="X")
    assert roast.__module__ == __name__
    roast("root")
    monkeypatch.setitem(globals(), "TEMPLATES", tmp_path)
    roast("root")
    assert [c["templates"] for c in calls] == [None, tmp_path]
    run.python_api(roast_a, KEYS, folder="X", templates="t")("root")
    assert calls[-1]["templates"] == "t"


def test_python_api_signature_and_doc():
    roast = run.python_api(
        roast_a, KEYS, folder="X-1", width=4, example=7, options=extra_options,
        options_doc=run.doc_entry("discs : list[int]", "Only these discs"),
        keys_doc="more about keys")
    params = inspect.signature(roast).parameters
    assert list(params) == ["root", "keys", "exclude_keys", "sub",
                            "exclude_sub", "ses", "datatype", "suffix",
                            "task", "discs", "level", "options"]
    for name in ("ses", "datatype", "suffix", "task"):
        assert params[name].kind is inspect.Parameter.KEYWORD_ONLY
        assert params[name].default == ()
        assert f"{name} : str | list[str]" in roast.__doc__
    assert params["discs"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["discs"].default is None
    assert params["options"].kind is inspect.Parameter.VAR_KEYWORD
    doc = roast.__doc__
    assert doc.startswith("Roast `<root>` (the `X-1` folder).")
    for text in ("`7` is `sub-0007`", "discs : list[int]\n    Only these",
                 "more about keys", "**options\n", "`ifexists`",
                 "Returns\n-------\nRoastReport"):
        assert text in doc
    assert roast.__name__ == roast.__qualname__ == "roast"


@pytest.mark.parametrize("name", PORTED)
def test_ported_roast_is_python_api(name):
    """Every ported dataset's `roast()`: the same parameters, a docstring
    that lists them"""
    module = importlib.import_module(f"brainspresso.datasets.{name}.roaster")
    roast = module.roast
    assert roast.__module__ == module.__name__
    params = inspect.signature(roast).parameters
    assert {"root", "keys", "exclude_keys", "sub", "exclude_sub",
            "options"} <= set(params)
    for param in params:
        if param != "options":
            assert f"{param}" in roast.__doc__
    assert "**options" in roast.__doc__


def test_dataset_options_checked(tmp_path):
    """The options of a dataset are checked before anything is read"""
    with pytest.raises(UsageError, match="no disc 13"):
        oasis1.roast(tmp_path, discs=[13])
    with pytest.raises(UsageError, match="part"):
        oasis2.roast(tmp_path, parts=[3])
    assert oasis1.disc_options((), [2, 1, 2]) == {"discs": [1, 2]}
    assert oasis2.part_options((), [2, 2]) == {"parts": [2]}
    assert oasis1.disc_options(()) is None


# ----------------------------------------------------------------------
#   freesurfer.archive
# ----------------------------------------------------------------------

def make_tar(path: Path, names) -> Path:
    with tarfile.open(path, "w:gz") as tar:
        for name in names:
            info = tarfile.TarInfo(name)
            info.size = 1
            tar.addfile(info, io.BytesIO(b"x"))
    return path


def test_fixed_root():
    relpath = fsarchive.fixed_root(2)
    assert relpath("disc1/OAS1_0001_MR1/mri/aseg.mgz") == \
        PurePosixPath("mri/aseg.mgz")
    assert relpath("disc1/OAS1_0001_MR1") is None
    assert relpath("disc1/../x/mri/aseg.mgz") is None
    assert relpath("/disc1/x/mri/aseg.mgz") is None
    # (an argument of a shard)
    assert pickle.loads(pickle.dumps(relpath))("a/b/c") == PurePosixPath("c")


def test_tree_rename(tmp_path):
    relpath = fsarchive.fixed_root(1)
    rename = fsarchive.tree_rename(relpath, tmp_path, everything=False)
    assert rename("s/mri/aseg.mgz") == tmp_path / "mri" / "aseg.mgz"
    assert rename("s/tmp/x.log") is None
    assert rename("s") is None
    rename = fsarchive.tree_rename(relpath, tmp_path, everything=True)
    assert rename("s/tmp/x.log") == tmp_path / "tmp" / "x.log"


def test_tree_exists(tmp_path):
    tarball = make_tar(tmp_path / "fs.tar.gz", [
        "./s/mri/aseg.mgz", "s/surf/lh.pial", "../s/mri/norm.mgz"])
    src = tmp_path / "src"
    exists = fsarchive.tree_exists(tarball, src, fsarchive.fixed_root(1))
    assert exists(src / "mri" / "aseg.mgz")
    assert exists(src / "surf" / "lh.pial")
    assert not exists(src / "mri" / "norm.mgz")     # (`..`: dropped)
    (tmp_path / "bad.tar.gz").write_bytes(b"\x1f\x8b\x08\x00 truncated")
    with pytest.raises(archives.ArchiveError, match="cannot be read"):
        fsarchive.tree_exists(tmp_path / "bad.tar.gz", src,
                              fsarchive.fixed_root(1), label="x/bad.tar.gz")


def test_tree_actions(tmp_path):
    tarball = make_tar(tmp_path / "fs.tar.gz", ["s/mri/aseg.mgz"])
    src, dst = tmp_path / "src", tmp_path / "out"
    ctx = SimpleNamespace(json="yes", layout=SimpleNamespace(root=tmp_path))
    first = next(fsarchive.tree_actions(
        ctx, tarball, src, dst, ["bids:raw:x"],
        relpath=fsarchive.fixed_root(1), everything=False, tag="fsx"))
    assert isinstance(first, ExtractMembers)
    assert (first.staging, first.tag, first.claim.folders) == (
        True, "fsx", (src,))
    # `--sidecars only`: listed, not unpacked; unreadable: one `Fail`
    ctx.json = "only"
    (tmp_path / "bad.tar.gz").write_bytes(b"\x1f\x8b\x08\x00 truncated")
    out = list(fsarchive.tree_actions(
        ctx, tmp_path / "bad.tar.gz", src, dst, ["bids:raw:x"],
        relpath=fsarchive.fixed_root(1), everything=False))
    assert [(type(a), a.name, a.tag) for a in out] == [
        (Fail, "bad.tar.gz", "fs")]
    # (OASIS: `fs-all` unpacks, even with `--sidecars only`)
    first = next(fsarchive.tree_actions(
        ctx, tarball, src, dst, ["bids:raw:x"],
        relpath=fsarchive.fixed_root(1), everything=True, list_only=False))
    assert isinstance(first, ExtractMembers)


def test_abide1_tree_is_shared():
    """ABIDE-I's `FreeSurferTree` is `freesurfer.archive`'s"""
    assert abide1.FreeSurferTree is fsarchive.FreeSurferTree
