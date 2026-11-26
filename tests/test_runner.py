"""Roast runner (`actions.runner`), action outputs/claims, key aliases."""
import json
from pathlib import Path

import pytest

from brainspresso.actions import Action, Claim, WriteJSON, WriteText
from brainspresso.actions.archive import ExtractMembers
from brainspresso.actions.runner import (
    RoastContext, Shard, make_context, run_roast,
)
from brainspresso.bids.keys import KeyAliases
from brainspresso.bids.layout import Layout, make_layout
from roast_utils import LAYOUTS, make_tar, raw_tree, read_tsv

KEYS = KeyAliases(
    {"meta": {}, "raw": {"anat": {"T1w", "TSE"}, "dwi": {}}},
    bids={"T1w": {"suffixes": ["T1w"]},
          "TSE": {"suffixes": ["T2w"], "entities": {"acq": ["TSE"]}},
          "dwi": {"datatypes": ["dwi"]}},
)


@pytest.fixture(params=LAYOUTS)
def layout(tmp_path, request) -> Layout:
    return make_layout(request.param, tmp_path / "DS")


def ctx_of(layout, **kw) -> RoastContext:
    return make_context(layout, KEYS, **kw)


def tree(root: Path) -> set[str]:
    return {p.relative_to(root).as_posix()
            for p in Path(root).rglob("*") if p.is_file()}


def text(dst, value="x", **kw):
    return WriteText(value, dst, **kw)


def run(roaster, ctx, **kw):
    return run_roast(roaster, ctx, display=False, **kw)


# ----------------------------------------------------------------------
#   Key aliases
# ----------------------------------------------------------------------

def test_key_aliases():
    assert KEYS.leaves == {"meta", "T1w", "TSE", "dwi"}
    assert KEYS.expand(["anat"]) == {"T1w", "TSE"}
    assert KEYS.expand("RAW") == {"T1w", "TSE", "dwi"}  # case-insensitive
    assert KEYS.categories == {"meta"}
    with pytest.raises(ValueError):
        KEYS.expand(["T3w"])

    # union of heterogeneous keys: T1w or (acq-TSE and T2w)
    sel = KEYS.selection(["T1w", "TSE"])
    anat = "sub-01/anat/sub-01"
    assert sel.keeps(f"{anat}_T1w.nii.gz")
    assert sel.keeps(f"{anat}_acq-TSE_T2w.nii.gz")
    assert not sel.keeps(f"{anat}_T2w.nii.gz")
    assert not sel.keeps("sub-01/dwi/sub-01_dwi.nii.gz")
    assert sel.keeps("participants.tsv")
    assert sel.keeps("sub-01/sub-01_sessions.tsv")
    assert not sel.wants("meta")
    assert sel.select([f"{anat}_T1w.nii.gz", f"{anat}_T1w.json",
                       f"{anat}_T2w.nii.gz", f"{anat}_T2w.json"]) == [
        f"{anat}_T1w.json", f"{anat}_T1w.nii.gz"]

    # all keys: everything, even files that no key describes
    sel = KEYS.selection(subs=["01"])
    assert sel.wants("meta")
    assert sel.keeps("sub-01/func/sub-01_task-rest_bold.nii.gz")
    assert not sel.keeps("sub-02/anat/sub-02_T1w.nii.gz")
    assert not sel.keeps_subject("02")

    # exclusions remove keys, groups included
    sel = KEYS.selection(exclude_keys=["anat", "meta"])
    assert sel.keeps("sub-01/dwi/sub-01_dwi.bval")
    assert not sel.keeps(f"{anat}_T1w.nii.gz")
    assert not sel.wants("meta")
    assert sel.keys == {"dwi"}

    # categories only: no raw data, but dataset files and tables
    sel = KEYS.selection(["meta"])
    assert sel.wants("meta")
    assert not sel.keeps(f"{anat}_T1w.nii.gz")
    assert sel.keeps("README") and sel.keeps("participants.tsv")


# ----------------------------------------------------------------------
#   Selection on outputs and claims
# ----------------------------------------------------------------------

def test_selection_on_outputs(layout):
    raw = layout.raw

    def roaster(ctx):
        for sub in ("01", "02"):
            anat = raw / f"sub-{sub}" / "anat"
            yield text(anat / f"sub-{sub}_T1w.nii.gz")
            yield text(anat / f"sub-{sub}_T2w.nii.gz")
            yield text(raw / f"sub-{sub}" / "dwi" / f"sub-{sub}_dwi.nii.gz")
        drv = layout.derivative("pipe").root
        yield text(drv / "sub-02" / "anat" / "sub-02_desc-x_mask.nii.gz")
        yield text(drv / "sub-01" / "anat" / "sub-01_desc-x_mask.nii.gz")
        yield text(layout.source / "sub-02" / "unpacked.txt")

    report = run(roaster, ctx_of(layout, select_keys=["T1w", "dwi"],
                                 exclude_subs=["01"]))
    assert tree(raw) - tree(layout.source) - tree(layout.derivatives) >= {
        "sub-02/anat/sub-02_T1w.nii.gz", "sub-02/dwi/sub-02_dwi.nii.gz"}
    assert not (raw / "sub-01").exists()
    assert not (raw / "sub-02/anat/sub-02_T2w.nii.gz").exists()
    # outside the raw dataset: subject filters only
    assert tree(layout.derivatives) == {
        "pipe/sub-02/anat/sub-02_desc-x_mask.nii.gz"}
    assert (layout.source / "sub-02" / "unpacked.txt").exists()
    assert report.total("done") == 4
    assert len(report.outputs) == 4


def test_claims(layout, tmp_path):
    """Outputs only known once an archive is opened"""
    raw = layout.raw
    archive = make_tar(tmp_path / "ses.tar.gz", {
        "x/T1.nii.gz": b"t1", "x/T1.json": b"{}",
        "x/T2.nii.gz": b"t2", "x/README": b"ignored",
    })
    opened = []

    def opener(path):
        from brainspresso.utils.archive import TarArchive
        opened.append(path)
        return TarArchive(path)

    def roaster(ctx):
        for sub in ("01", "02"):
            anat = raw / f"sub-{sub}" / "ses-1" / "anat"

            def rename(name, sub=sub, anat=anat):
                stem, _, ext = Path(name).name.partition(".")
                if ext not in ("nii.gz", "json"):
                    return None
                return anat / f"sub-{sub}_ses-1_{stem}w.{ext}"

            yield ExtractMembers(archive, Claim(anat), rename,
                                 opener=opener, tag="anat")

    ctx = ctx_of(layout, select_keys=["T1w"], subs=["02"], json="no")
    report = run(roaster, ctx)
    # the claim of sub-01 cannot be selected: its archive is not opened
    assert len(opened) == 1
    assert tree(raw / "sub-02") == {"ses-1/anat/sub-02_ses-1_T1w.nii.gz"}
    assert not (raw / "sub-01").exists()
    assert report.counts["anat"]["done"] == 1
    assert report.outputs == [
        raw / "sub-02/ses-1/anat/sub-02_ses-1_T1w.nii.gz"]

    # second run: the claim still runs (outputs unknown), members exist
    report = run(roaster, ctx_of(layout, select_keys=["T1w"], subs=["02"]))
    assert report.counts["anat"] == {"skipped": 1, "done": 1}
    assert (raw / "sub-02/ses-1/anat/sub-02_ses-1_T1w.json").exists()


def test_claim_direct_writes(layout):
    """A claim action may write files itself, after asking `keeps`"""
    raw = layout.raw
    anat = raw / "sub-01" / "anat"
    action = None

    def convert():
        for suffix in ("T1w", "T2w"):
            path = anat / f"sub-01_{suffix}.nii.gz"
            if action.keeps(path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(suffix)
                action.outputs.append(path)

    def roaster(ctx):
        nonlocal action
        action = Action([], None, convert, claim=Claim(anat, "sub-01"))
        yield action

    report = run(roaster, ctx_of(layout, select_keys=["T1w"]))
    assert tree(anat) == {"sub-01_T1w.nii.gz"}
    assert action.outputs == [anat / "sub-01_T1w.nii.gz"]
    assert report.total("done") == 1


def test_multi_output_and_ifexists(layout):
    dwi = layout.raw / "sub-01" / "dwi"
    names = [dwi / f"sub-01_dwi.{ext}" for ext in ("nii.gz", "bval", "bvec")]
    calls = []

    def write(*paths):
        calls.append(len(paths))
        for path in paths:
            Path(path).write_text("new")

    def roaster(ctx):
        yield Action([], names, write, input="path", tag="dwi")

    # selected by its bval/bvec (top-level `--keys dwi` style): written all
    run(roaster, ctx_of(layout, select_keys=["dwi"]))
    assert calls == [3] and all(p.read_text() == "new" for p in names)

    # skip: all outputs exist -> not run
    report = run(roaster, ctx_of(layout, ifexists="skip"))
    assert calls == [3] and report.counts["dwi"]["skipped"] == 1

    # skip, but one output is missing -> the set is written again
    names[1].unlink()
    names[0].write_text("old")
    run(roaster, ctx_of(layout, ifexists="skip"))
    assert calls == [3, 3] and names[0].read_text() == "new"

    # error: any output exists -> error, counted
    report = run(roaster, ctx_of(layout, ifexists="error"))
    assert report.counts["dwi"]["error"] == 1 and calls == [3, 3]

    # an action's own `ifexists` wins over the runner's
    def own(ctx):
        yield Action([], names, write, input="path", ifexists="overwrite")

    run(own, ctx_of(layout, ifexists="skip"))
    assert calls == [3, 3, 3]

    # not selected at all (other subject): not run, not counted
    report = run(roaster, ctx_of(layout, subs=["02"], ifexists="overwrite"))
    assert calls == [3, 3, 3] and report.total("done") == 0


@pytest.mark.parametrize("policy", ["yes", "no", "only"])
def test_json_policy(layout, policy):
    raw = layout.raw
    anat = raw / "sub-01" / "anat"
    pheno = layout.phenotype

    def roaster(ctx):
        yield text(anat / "sub-01_T1w.nii.gz")
        yield WriteJSON({}, anat / "sub-01_T1w.json")
        yield text(raw / "participants.tsv", "participant_id\n")
        yield WriteJSON({}, raw / "participants.json")
        yield WriteJSON({}, raw / "dataset_description.json")
        yield WriteJSON({}, pheno / "test.json")
        # explicit kind
        yield WriteJSON({}, anat / "sub-01_T1w.extra", kind="sidecar")

    run(roaster, ctx_of(layout, json=policy))
    tables = {"participants.tsv", "participants.json",
              "dataset_description.json", "phenotype/test.json"}
    data = {"sub-01/anat/sub-01_T1w.nii.gz"}
    sidecars = {"sub-01/anat/sub-01_T1w.json", "sub-01/anat/sub-01_T1w.extra"}
    expected = tables | {"yes": data | sidecars, "no": data,
                         "only": sidecars}[policy]
    assert raw_tree(layout.root) == expected


def test_errors_are_counted(layout):
    raw = layout.raw

    def fail(path):
        raise RuntimeError("boom")

    def broken(ctx):
        yield text(raw / "sub-02" / "anat" / "sub-02_T1w.nii.gz", tag="b")
        raise ValueError("broken roaster")

    broken.tag = "b"        # progress group of a roaster

    def roaster(ctx):
        yield Action([], raw / "sub-01" / "anat" / "sub-01_T1w.nii.gz",
                     fail, input="path", tag="a")
        yield text(raw / "sub-01" / "anat" / "sub-01_T2w.nii.gz", tag="a")
        yield broken
        yield text(raw / "README", tag="a")

    report = run(roaster, ctx_of(layout))
    assert report.counts["a"] == {"error": 1, "done": 2}
    assert report.counts["b"] == {"done": 1, "error": 1}
    assert {e[2] for e in report.errors} == {
        "boom", "ValueError: broken roaster"}
    assert (raw / "README").exists()
    assert not (raw / "sub-01/anat/sub-01_T1w.nii.gz").exists()
    runs = json.loads(layout.manifest.read_text())["runs"]
    assert runs[-1]["stage"] == "roast"
    assert runs[-1]["counts"]["a"] == {"error": 1, "done": 2}


# Shards must be picklable: module-level functions
def shard_subject(ctx, sub):
    assert ctx.tag == "subjects"        # tag of the running roaster
    ctx.progress(total=2, tag=f"sub-{sub}")
    anat = ctx.layout.raw / f"sub-{sub}" / "anat"
    for suffix in ("T1w", "T2w"):
        yield text(anat / f"sub-{sub}_{suffix}.nii.gz", f"{sub}{suffix}",
                   tag="anat")
        ctx.progress(advance=1, tag=f"sub-{sub}")
    if sub == "03":
        yield Action([], anat / f"sub-{sub}_PDw.nii.gz", _fail,
                     input="path", tag="anat")
    ctx.progress(advance=1, tag="subjects")


def _fail(path):
    raise RuntimeError("boom")


def sharded(ctx):
    assert ctx.tag == "roast"
    ctx.progress(total=3, tag="subjects")
    for sub in ("01", "02", "03"):
        yield Shard(shard_subject, sub, tag="subjects")
    yield text(ctx.layout.raw / "README", tag="meta")


@pytest.mark.parametrize("jobs", [1, 2])
def test_shards(layout, jobs):
    report = run(sharded, ctx_of(layout, exclude_subs=["02"]), jobs=jobs)
    raw = layout.raw
    assert raw_tree(layout.root) == {
        "README", *(f"sub-{s}/anat/sub-{s}_{x}.nii.gz"
                    for s in ("01", "03") for x in ("T1w", "T2w"))}
    assert (raw / "sub-03/anat/sub-03_T2w.nii.gz").read_text() == "03T2w"
    assert report.counts["anat"] == {"done": 4, "error": 1}
    assert report.counts["meta"] == {"done": 1}
    # progress reported from the shards (also from worker processes)
    assert report.progress == {
        "subjects": (3, 3), "sub-01": (2, 2), "sub-02": (2, 2),
        "sub-03": (2, 2)}


def test_unpicklable_shard_runs_in_main_process(layout):
    raw = layout.raw

    def roaster(ctx):
        def local(ctx):     # a closure cannot be pickled
            yield text(raw / "README")
        yield local

    report = run(roaster, ctx_of(layout), jobs=2)
    assert (raw / "README").exists() and report.total("done") == 1


# ----------------------------------------------------------------------
#   Tables, finalize, resources
# ----------------------------------------------------------------------

def test_tables_trimmed_and_finalize(layout):
    raw = layout.raw
    seen = {}

    def roaster(ctx):
        rows = "participant_id\tage\nsub-01\t1\nsub-02\t2\nsub-03\t3\n"
        yield text(layout.original(raw / "participants.tsv"), rows)
        for sub in ("01", "02", "03"):
            yield text(layout.original(
                raw / f"sub-{sub}" / f"sub-{sub}_sessions.tsv"),
                "session_id\nses-1\nses-2\n")
        yield text(raw / "sub-01/ses-1/anat/sub-01_ses-1_T1w.nii.gz")
        yield text(raw / "sub-03/ses-2/anat/sub-03_ses-2_T1w.nii.gz")

    def finalize(ctx):
        # sees the finished tree, may yield more actions
        seen["subjects"] = sorted(p.name for p in raw.glob("sub-*"))
        yield text(raw / "sub-02/ses-1/anat/sub-02_ses-1_T1w.nii.gz")

    roaster.finalize = finalize
    run(roaster, ctx_of(layout, exclude_subs=["03"]))
    assert seen["subjects"] == ["sub-01"]
    assert read_tsv(raw / "participants.tsv") == [
        ["participant_id", "age"], ["sub-01", "1"], ["sub-02", "2"]]
    assert read_tsv(raw / "sub-01/sub-01_sessions.tsv") == [
        ["session_id"], ["ses-1"]]
    assert read_tsv(raw / "sub-02/sub-02_sessions.tsv") == [
        ["session_id"], ["ses-1"]]
    # excluded subject: its pristine table was not written either
    assert not layout.original(raw / "sub-03/sub-03_sessions.tsv").exists()
    assert not (raw / "sub-03").exists()
    # the pristine table is complete
    assert len(read_tsv(layout.original(raw / "participants.tsv"))) == 4

    # a later roast (other subjects) trims from the pristine tables again
    run(roaster, ctx_of(layout, subs=["03"]))
    assert [r[0] for r in read_tsv(raw / "participants.tsv")] == [
        "participant_id", "sub-01", "sub-02", "sub-03"]
    assert read_tsv(raw / "sub-03/sub-03_sessions.tsv") == [
        ["session_id"], ["ses-2"]]


def test_resource(layout, tmp_path):
    templates = tmp_path / "templates"
    (templates / "site").mkdir(parents=True)
    (templates / "README").write_text("template")
    (templates / "site" / "T1w.json").write_text("{}")
    layout.source.mkdir(parents=True)
    (layout.source / "README").write_text("harvested")
    ctx = ctx_of(layout, templates=templates)
    assert ctx.resource("README") == layout.source / "README"
    assert ctx.resource("site/T1w.json") == templates / "site" / "T1w.json"
    assert ctx.resource("missing.json") is None
    assert RoastContext(layout).resource("README") == \
        layout.source / "README"


def test_ifexists_context_is_restored():
    from brainspresso.actions import IfExists
    before = IfExists.current
    with IfExists("skip"):
        with IfExists("overwrite"):
            assert IfExists.current is IfExists.OVERWRITE
        assert IfExists.current is IfExists.SKIP
    assert IfExists.current is before


# ----------------------------------------------------------------------
#   Review fixes (2b-ii-fix)
# ----------------------------------------------------------------------

def shard_copies(ctx, sub, template):
    """Every shard reads the same template"""
    from brainspresso.actions import CopyJSON
    for k in range(40):
        yield CopyJSON(template, ctx.layout.raw / f"sub-{sub:02d}" / "anat"
                       / f"sub-{sub:02d}_run-{k}_T1w.json")


def shared_template(ctx):
    for sub in range(8):
        yield Shard(shard_copies, sub, ctx.layout.source / "T1w.json")


def test_shared_source_read_concurrently(layout):
    """Readers take no lock and create nothing next to sources"""
    layout.source.mkdir(parents=True)
    template = layout.source / "T1w.json"
    template.write_text("{}")
    report = run(shared_template, ctx_of(layout), jobs=4)
    assert not report.errors
    assert report.total("done") == 320
    assert tree(layout.source) == {"T1w.json"}


def test_reader_creates_nothing_next_to_source(layout, tmp_path):
    src = tmp_path / "pkg" / "template.json"
    src.parent.mkdir()
    src.write_text("{}")
    seen = []

    def copy(path):
        seen.append(sorted(p.name for p in src.parent.iterdir()))
        Path(path).write_text(src.read_text())

    def roaster(ctx):
        yield Action(src, layout.raw / "README", copy, input="path")

    report = run(roaster, ctx_of(layout))
    assert report.total("done") == 1
    assert seen == [["template.json"]]
    assert sorted(p.name for p in src.parent.iterdir()) == ["template.json"]
    # outputs are still written atomically (temporary file, then renamed)
    assert not list(layout.raw.glob("*.tmp"))
