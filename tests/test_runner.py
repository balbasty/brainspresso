"""Engine (`actions.engine`) and roast layer (`roast`): action outputs,
claims, kinds, shards, key aliases."""
import json
from pathlib import Path

import pytest

from brainspresso.actions import Action, Claim, WriteJSON, WriteText
from brainspresso.actions.archive import ExtractMembers
from brainspresso.roast import (
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


def test_claims_yield_actions_only(layout):
    """A claim yields sub-actions (no direct writes); one that yields
    nothing is counted as "empty"; a claim may list several folders"""
    raw = layout.raw
    ses = raw / "sub-01" / "ses-1"

    def nothing():
        return iter(())

    def writes_itself():
        (ses / "func").mkdir(parents=True, exist_ok=True)
        return [ses / "func" / "sub-01_ses-1_task-x_bold.nii.gz"]

    def both():
        yield text(ses / "func" / "sub-01_ses-1_task-x_bold.nii.gz")
        yield text(ses / "perf" / "sub-01_ses-1_asl.nii.gz")

    def roaster(ctx):
        yield Action([], None, nothing, claim=Claim(ses / "anat"), tag="a")
        yield Action([], None, writes_itself, claim=Claim(ses / "func"),
                     tag="b")
        yield Action([], None, both, tag="c",
                     claim=Claim([ses / "func", ses / "perf"]))

    report = run(roaster, ctx_of(layout))
    assert report.counts["a"] == {"empty": 1}
    assert report.counts["b"] == {"error": 1}
    assert report.counts["c"] == {"done": 2}
    assert tree(ses) == {"func/sub-01_ses-1_task-x_bold.nii.gz",
                         "perf/sub-01_ses-1_asl.nii.gz"}
    # a list of folders: selected if any of them can be
    report = run(roaster, ctx_of(layout, select_keys=["dwi"]))
    assert report.counts == {}
    assert Claim([ses / "func"]).folder == ses / "func"


def test_multi_output_and_ifexists(layout):
    dwi = layout.raw / "sub-01" / "dwi"
    names = [dwi / f"sub-01_dwi.{ext}" for ext in ("nii.gz", "bval", "bvec")]
    calls = []

    def write(*paths):
        calls.append(len(paths))
        for path in paths:
            Path(path).write_text("new")

    def roaster(ctx):
        yield Action([], names, write, input="path", tag="dwi", kind="data")

    # outputs of several kinds need an explicit `kind`
    def mixed(ctx):
        yield Action([], names, write, input="path", tag="dwi")

    report = run(mixed, ctx_of(layout))
    assert calls == [] and report.counts["dwi"] == {"error": 1}
    assert "explicit `kind`" in report.errors[0][2]

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
        yield Action([], names, write, input="path", ifexists="overwrite",
                     kind="data")

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


def test_key_classification():
    """A path has one key (first match); unclassified files follow
    their scans; category-only key sets keep no data and no claim"""
    keys = KeyAliases(
        {"meta": {}, "fs": {}, "anat": {"TSE", "T2w"}, "func": {"bold"},
         "perf": {"asl"}, "ct": {"CT"}},
        bids={"TSE": {"suffixes": ["T2w"], "entities": {"acq": ["TSE"]}},
              "T2w": {"suffixes": ["T2w"], "datatypes": ["anat"]},
              "bold": {"datatypes": ["func"], "suffixes": ["bold"]},
              "asl": {"datatypes": ["perf"], "suffixes": ["asl"]},
              "CT": {"datatypes": ["ct"], "suffixes": ["CT"]}},
    )
    b = "sub-1/ses-1/"
    tse = b + "anat/sub-1_ses-1_acq-TSE_T2w.nii.gz"
    t2w = b + "anat/sub-1_ses-1_T2w.nii.gz"
    sbref = b + "func/sub-1_ses-1_task-rest_sbref.nii.gz"
    m0 = b + "perf/sub-1_ses-1_m0scan.nii.gz"
    events = b + "func/sub-1_ses-1_task-rest_events.tsv"
    aslcontext = b + "perf/sub-1_ses-1_aslcontext.tsv"
    assert keys.key_of(tse) == "TSE" and keys.key_of(t2w) == "T2w"
    assert keys.key_of(sbref) is None

    sel = keys.selection(exclude_keys=["TSE"])
    assert not sel.keeps(tse) and sel.keeps(t2w)
    assert not sel.keeps(tse.replace(".nii.gz", ".json"))
    # a sidecar that applies to both TSE and T2w follows either
    assert sel.keeps("sub-1/sub-1_T2w.json")
    assert keys.selection(["TSE"]).keeps("sub-1/sub-1_T2w.json")

    sel = keys.selection(exclude_keys=["CT"])
    assert all(map(sel.keeps, (sbref, m0, events, aslcontext, tse)))
    sel = keys.selection(["T2w"])
    assert not any(map(sel.keeps, (sbref, m0, events, aslcontext, tse)))
    sel = keys.selection(["bold"])
    assert sel.keeps(sbref) and sel.keeps(events) and not sel.keeps(m0)

    for category in ("meta", "fs"):
        sel = keys.selection([category])
        assert sel.wants(category)
        assert not any(map(sel.keeps, (tse, sbref, events, aslcontext)))
        for claim in ("sub-1/ses-1", "sub-1/ses-1/anat", "sub-1/ses-1/func"):
            assert sel.keeps(claim, partial=True) is False
        assert sel.keeps("participants.tsv")
        assert sel.keeps("sub-1/sub-1_sessions.tsv")
    sel = keys.selection(["T2w"])
    assert sel.keeps("sub-1/ses-1/anat", partial=True) is None
    assert sel.keeps("sub-1/ses-1/func", partial=True) is False
    # all BIDS keys (categories excluded): files no key describes too
    sel = keys.selection(exclude_keys=["meta", "fs"])
    assert sel.keeps(b + "beh/sub-1_ses-1_beh.tsv")
    assert sel.keeps(b + "anat/sub-1_ses-1_PDT2.nii.gz")


# Nested shards: every shard runs in the pool, every finalize runs
def nested_leaf(ctx, site, i):
    import os
    import time
    anat = ctx.layout.raw / f"sub-{site}{i}" / "anat"
    yield WriteJSON({"pid": os.getpid()}, anat / f"sub-{site}{i}_T1w.json")
    time.sleep(ctx.options.get("sleep", 0))     # leaves run concurrently


def nested_site(ctx, site):
    for i in range(3):
        yield Shard(nested_leaf, site, i)


def nested_site_finalize(ctx, site):
    yield text(ctx.layout.raw / f"site-{site}.txt", tag="final")


nested_site.finalize = nested_site_finalize


def nested_top(ctx):
    for site in ("a", "b"):
        yield Shard(nested_site, site, tag="sites")

    def local(ctx):             # unpicklable: driven in the main process
        yield Shard(nested_leaf, "c", 0)

    def local_finalize(ctx):
        yield text(ctx.layout.raw / "site-c.txt", tag="final")

    local.finalize = local_finalize
    yield local


@pytest.mark.parametrize("jobs", [1, 6])
def test_nested_shards(layout, jobs):
    import os
    order = []

    def top(ctx):
        yield from nested_top(ctx)

    def finalize(ctx):
        order.append(sorted(p.name for p in layout.raw.glob("site-*")))
        yield from ()

    top.finalize = finalize
    report = run(top, ctx_of(layout, options={"sleep": 0.5 * (jobs > 1)}),
                 jobs=jobs)
    assert not report.errors
    leaves = list(layout.raw.rglob("*_T1w.json"))
    assert len(leaves) == 7
    pids = {json.loads(p.read_text())["pid"] for p in leaves}
    if jobs > 1:
        # leaves of a site are handed back to the pool, not run in the
        # worker of their site; shards of a main-process roaster too
        assert os.getpid() not in pids and len(pids) > 2
    # finalize of every roaster, children first, the top-level one last
    assert order == [["site-a.txt", "site-b.txt", "site-c.txt"]]
    assert report.counts["final"] == {"done": 3}


# Pool failures
def crashing(ctx, i, main_pid):
    import os
    import time
    if i == 1 and os.getpid() != main_pid:
        os._exit(1)             # a worker dies (segfault, OOM killer...)
    time.sleep(0.3)
    yield text(ctx.layout.raw / f"sub-{i:02d}" / "anat" /
               f"sub-{i:02d}_T1w.nii.gz", str(os.getpid()), tag="anat")


@pytest.mark.parametrize("pause", [0, 0.4])
def test_worker_death_does_not_abort_the_roast(layout, pause):
    import os
    import time

    def roaster(ctx):
        yield text(layout.original(layout.raw / "participants.tsv"),
                   "participant_id\nsub-00\nsub-05\nsub-99\n", tag="meta")
        for i in range(6):
            yield Shard(crashing, i, os.getpid(), id=f"roast/sub-{i:02d}")
            # (pause: the pool breaks while the roaster yields shards)
            time.sleep(pause)

    report = run(roaster, ctx_of(layout), jobs=2)
    assert report.status == "partial"
    died = [e for e in report.errors if "worker process died" in e[2]]
    assert died and len(report.errors) == len(died)
    done = {p.parts[-3]: p.read_text()
            for p in layout.raw.rglob("*_T1w.nii.gz")}
    assert "sub-01" not in done and "sub-05" in done
    assert report.counts["anat"]["done"] + len(died) == 6
    # shards that had not started ran in the main process
    assert done["sub-05"] == str(os.getpid())
    # common steps ran: tables trimmed, manifest written with a status
    rows = read_tsv(layout.raw / "participants.tsv")[1:]
    assert {r[0] for r in rows} == {"sub-00", "sub-05"} & set(done)
    run_record = json.loads(layout.manifest.read_text())["runs"][-1]
    assert run_record["status"] == "partial"


def test_unpicklable_context_runs_in_main_process(layout):
    ctx = ctx_of(layout, options={"f": lambda x: x})
    report = run(sharded, ctx, jobs=2)
    # (the error is that of `shard_subject`: no pickling error)
    assert [e[2] for e in report.errors] == ["boom"]
    assert report.counts["anat"]["done"] == 6


def test_failed_roast_still_runs_common_steps(layout):
    def roaster(ctx):
        yield text(layout.original(layout.raw / "participants.tsv"),
                   "participant_id\nsub-01\n", tag="meta")
        yield text(layout.raw / "sub-01" / "anat" / "sub-01_T1w.nii.gz")
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run(roaster, ctx_of(layout))
    run_record = json.loads(layout.manifest.read_text())["runs"][-1]
    assert run_record["status"] == "failed"
    assert (layout.raw / "participants.tsv").exists()
    report = run(lambda ctx: iter(()), ctx_of(layout))
    assert report.status == "complete"


def test_pristine_tables_follow_new_harvests(layout):
    """Tables (kind "table") use `different` under "skip": a re-roast
    after a new harvest updates them"""
    raw = layout.raw
    calls = []

    def roaster(ctx):
        n = ctx.options["n"]
        for k in range(n):
            yield text(raw / "sub-01" / f"ses-{k}" / "anat" /
                       f"sub-01_ses-{k}_T1w.nii.gz")
        rows = "session_id\n" + "".join(f"ses-{k}\n" for k in range(n))
        yield Action([], layout.original(raw / "sub-01/sub-01_sessions.tsv"),
                     lambda f: calls.append(f.write(rows)), mode="t")

    for n in (1, 2, 2):
        report = run(roaster, ctx_of(layout, options={"n": n}))
        assert read_tsv(raw / "sub-01/sub-01_sessions.tsv") == [
            ["session_id"], *([f"ses-{k}"] for k in range(n))]
    # the identical table is not replaced (counted as skipped)
    assert report.counts["roast"] == {"skipped": 3}
    # an explicit policy wins
    run(roaster, ctx_of(layout, options={"n": 3}, ifexists="refresh"))


def test_kinds_and_json_policy(layout):
    """bval/bvec are metadata written under every policy; nothing hides
    a `Fail`; tables ignore --json"""
    from brainspresso.actions import Fail
    from brainspresso.bids.kinds import default_kind
    raw = layout.raw
    assert default_kind("dwi.bval") == default_kind("x_dwi.bvec") == "meta"

    def roaster(ctx):
        yield text(raw / "dwi.bval", "0 1000")
        yield text(raw / "sub-01" / "dwi" / "sub-01_dwi.nii.gz")
        yield text(raw / "sub-01" / "dwi" / "sub-01_dwi.bvec")
        yield WriteJSON({}, raw / "sub-01" / "dwi" / "sub-01_dwi.json")
        yield text(raw / "participants.tsv", "participant_id\n")
        yield Fail("missing input", name="x", tag="fail")

    for policy, expected in [
        ("only", {"dwi.bval", "sub-01/dwi/sub-01_dwi.bvec",
                  "sub-01/dwi/sub-01_dwi.json", "participants.tsv"}),
        ("no", {"dwi.bval", "sub-01/dwi/sub-01_dwi.bvec",
                "sub-01/dwi/sub-01_dwi.nii.gz", "participants.tsv"}),
    ]:
        for path in raw.rglob("*"):
            if path.is_file() and ".brainspresso" not in path.parts:
                path.unlink()
        report = run(roaster, ctx_of(layout, json=policy))
        assert raw_tree(layout.root) == expected
        assert report.counts["fail"] == {"error": 1}


def test_staging_and_category_folders(layout):
    """The JSON policy skips `staging` actions only (no implicit
    exemption of sourcedata/); writes in the folder of an unwanted
    category are dropped by the runner"""
    keys = KeyAliases({"meta", "T1w", "fs"}, bids={"T1w": {
        "suffixes": ["T1w"]}}, folders={"fs": ["derivatives/fs"]})
    drv = layout.derivative("fs")
    unpacked = drv.source / "sub-01" / "mri" / "aseg.mgz"
    loose = layout.source / "sub-01" / "loose.nii.gz"

    def roaster(ctx):
        # forgets to check `ctx.wants("fs")`
        yield text(unpacked, staging=True)
        yield text(drv.root / "sub-01" / "anat" / "sub-01_dseg.json")
        yield text(loose)

    run(roaster, make_context(layout, keys, json="only"))
    assert unpacked.exists() and not loose.exists()
    assert (drv.root / "sub-01/anat/sub-01_dseg.json").exists()
    unpacked.unlink()
    report = run(roaster, make_context(layout, keys, select_keys=["T1w"],
                                       ifexists="overwrite"))
    assert not unpacked.exists() and loose.exists()
    assert report.total("done") == 1


# Task state
def stateful(ctx, sub, archive):
    from brainspresso.utils.archive import TarArchive
    ctx.progress(total=1, tag="units")
    with TarArchive(archive) as tar:
        for member in tar:
            yield WriteText("x", ctx.layout.raw / f"sub-{sub}" / "anat" /
                            f"sub-{sub}_{member.name}")
    (ctx.layout.root / f"opened-{sub}").touch()
    ctx.progress(advance=1, tag="units")


@pytest.mark.parametrize("jobs", [1, 2])
def test_task_state_and_skip(layout, tmp_path, jobs):
    from brainspresso.bids.harvest import read_task_states
    archive = make_tar(tmp_path / "a.tar", {"T1w.nii.gz": b"1"})
    opened = layout.root / "opened-01"

    def roaster(ctx):
        yield Shard(stateful, "01", archive, id="roast/sub-01",
                    inputs=[archive], unit={"sub": "01"}, tag="anat")

    report = run(roaster, ctx_of(layout), jobs=jobs)
    state = read_task_states(layout.state)["roast/sub-01"]
    assert state["status"] == "done" and state["unit"] == {"sub": "01"}
    assert state["outputs"] == [
        (layout.raw / "sub-01/anat/sub-01_T1w.nii.gz")
        .relative_to(layout.root).as_posix()]
    assert state["counts"] == {"anat": {"done": 1}}
    assert state["progress"] == {"units": [1, 1]}
    assert state["started"] and state["finished"]
    assert report.progress["units"] == (1, 1)

    # unchanged inputs, under "skip": the archive is not reopened
    opened.unlink()
    report = run(roaster, ctx_of(layout), jobs=jobs)
    assert not opened.exists()
    assert report.counts["anat"] == {"skipped": 1}
    assert report.progress["units"] == (1, 1)
    # another selection, policy or input: run again
    for kw in ({"exclude_subs": ["02"]}, {"ifexists": "overwrite"}):
        run(roaster, ctx_of(layout, **kw), jobs=jobs)
        assert opened.exists()
        opened.unlink()
    import os
    os.utime(archive, (1, 1))
    run(roaster, ctx_of(layout), jobs=jobs)
    assert opened.exists()
    # no stale state files of anonymous worker tasks
    assert set(read_task_states(layout.state)) == {"roast/sub-01"}


def slow_units(ctx):
    import time
    ctx.progress(total=4, tag="units")
    ctx.progress(advance=1, tag="units")
    time.sleep(1.5)
    yield text(ctx.layout.raw / "README")
    ctx.progress(advance=3, tag="units")


def test_progress_read_from_task_state(layout):
    import time
    rows = []

    def roaster(ctx):
        yield Shard(slow_units, id="roast/slow")

    start = time.monotonic()
    report = run_roast(roaster, ctx_of(layout), jobs=2,
                       display=lambda row: rows.append(
                           (time.monotonic() - start, row)))
    assert report.progress["units"] == (4, 4)
    # 25% shown while the shard was running, not only when it returned
    early = [t for t, row in rows
             if row.get("modality") == "units" and row.get("progress") == 25]
    end = max(t for t, row in rows if row.get("status") == "done")
    assert early and early[0] < end - 0.5


def test_ctx_source(layout, tmp_path):
    other = tmp_path / "other"
    other.mkdir()
    (other / "README").write_text("other")
    ctx = make_context(layout, KEYS, source=other)
    assert ctx.source == other and ctx.resource("README") == other / "README"
    assert ctx_of(layout).source == layout.source
    seen = []

    def shard(ctx):
        seen.append(ctx.source)
        yield from ()

    def parent(ctx):
        yield Shard(shard)

    def roaster(ctx):
        seen.append(ctx.source)
        yield Shard(shard, source=other)
        yield shard
        yield Shard(parent, source=other)     # nested: that of its parent

    run(roaster, ctx_of(layout))
    assert seen == [layout.source, other, layout.source, other]


def test_helpers(layout, tmp_path):
    from brainspresso.roast import copy_templates, subject_shards
    templates = tmp_path / "templates"
    templates.mkdir()
    (templates / "README").write_text("readme")
    (templates / "participants.json").write_text("{}")
    calls = []

    def subject(ctx, sub, extra):
        calls.append((sub, extra))
        yield text(ctx.layout.raw / f"sub-{sub}" / "anat" /
                   f"sub-{sub}_T1w.nii.gz", tag="anat")

    def roaster(ctx):
        yield from copy_templates(ctx, ["README", "participants.json",
                                        "missing.json"])
        yield from subject_shards(ctx, ["01", "02", "03"], subject, "x")

    report = run(roaster, ctx_of(layout, templates=templates,
                                 exclude_subs=["02"]))
    assert (layout.raw / "README").read_text() == "readme"
    assert report.counts["meta"] == {"done": 2, "error": 1}
    assert "missing.json not found" in report.errors[0][2]
    assert calls == [("01", "x"), ("03", "x")]
    assert report.progress["subjects"] == (2, 2)
    from brainspresso.bids.harvest import read_task_states
    assert set(read_task_states(layout.state)) == {
        "roast/subjects/sub-01", "roast/subjects/sub-03"}


def test_missing_resource_is_a_clear_error(layout):
    from brainspresso.actions import CopyJSON

    def roaster(ctx):
        yield CopyJSON(ctx.resource("nope.json"),
                       layout.raw / "dataset_description.json")

    report = run(roaster, ctx_of(layout))
    assert report.errors[0][2].startswith(
        "No source to copy to dataset_description.json")
