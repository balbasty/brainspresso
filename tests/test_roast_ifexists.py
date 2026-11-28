"""
`roast --if-exists`: pure `skip`, `update` (edit test), tracked files.

A toy roaster copies templates (`<DS>/sourcedata/tpl/<name>`) to a
sidecar, a `README` and a `participants.json` (tracked files), an image
(data), a pristine participants table (trimmed after the roast) and the
description of a derivative dataset (rewritten by `finish_roast`).
"""
import hashlib
import json
import os
from pathlib import Path

import pytest
from roast_utils import init_layout, layout_of

from brainspresso.actions.action import Action, Fail, IfExists
from brainspresso.actions.engine import Shard
from brainspresso.actions.writers import CopyBytes, WriteJSON, WriteText
from brainspresso.bids import harvest as harvest_mod
from brainspresso.bids.compost import compost
from brainspresso.bids.harvest import update_files
from brainspresso.bids.keys import KeyAliases
from brainspresso.bids.layout import read_manifest
from brainspresso.bids.select import Selection
from brainspresso.roast.context import RoastContext, make_context
from brainspresso.roast.run import run_roast

KEYS = KeyAliases({"T1w"})
SIDECAR = "sub-01/anat/sub-01_T1w.json"
TRACKED = {
    "sidecar": SIDECAR,
    "readme": "README",
    "table-json": "participants.json",
}
IMAGE = "sub-01/anat/sub-01_T1w.nii.gz"
DERIVATIVE = "derivatives/toy/dataset_description.json"
MODES = ("skip", "update", "different", "overwrite", "refresh", "error")
STATES = ("absent", "ours", "edited", "unknown")


def template(root: Path, rel: str) -> Path:
    return root / "sourcedata" / "tpl" / rel.replace("/", "__")


def toy(ctx):
    """The roaster: tracked files, an image, tables, a derivative."""
    root = ctx.layout.root
    for rel in (*TRACKED.values(), IMAGE):
        yield CopyBytes(template(root, rel), ctx.layout.raw / rel)
    rows = [["participant_id"]] + [
        [f"sub-{s}"] for s in ctx.options.get("subjects", ["01"])]
    yield WriteText("\n".join("\t".join(r) for r in rows) + "\n",
                    ctx.layout.original(ctx.layout.raw / "participants.tsv"))
    yield WriteJSON({"Name": "toy", "BIDSVersion": "1.8.0"},
                    ctx.layout.derivatives / "toy" / DESCRIPTION_NAME)
    yield from ctx.options.get("extra", lambda ctx: ())(ctx)


DESCRIPTION_NAME = "dataset_description.json"


def make_root(tmp_path, layout="study") -> Path:
    root = tmp_path / "TOY"
    init_layout(root, layout)
    for rel in (*TRACKED.values(), IMAGE):
        set_template(root, rel, f"old {rel}\n")
    return root


def set_template(root: Path, rel: str, text: str, later: int = 0) -> None:
    path = template(root, rel)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode())        # (no CRLF on Windows: hashes)
    if later:
        stat = path.stat()
        os.utime(path, (stat.st_atime + later, stat.st_mtime + later))


def roast(root, ifexists="skip", jobs=1, roaster=toy, **options):
    ctx = make_context(root, KEYS, ifexists=ifexists, options=options,
                       json=options.pop("json", "yes"))
    return run_roast(roaster, ctx, jobs=jobs, display=False)


def entry(root, rel) -> dict:
    lay = layout_of(root)
    key = (lay.raw / rel).relative_to(lay.root).as_posix()
    return read_manifest(lay.state)["files"].get(key) or {}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def warned(caplog, text: str) -> bool:
    """Whether a warning mentions a text (info lines may be captured)."""
    return any(text in r.getMessage() for r in caplog.records
               if r.levelname == "WARNING")


def raw(root, rel) -> Path:
    return layout_of(root).raw / rel


def forget_hash(root, rel) -> None:
    lay = layout_of(root)
    key = (lay.raw / rel).relative_to(lay.root).as_posix()
    update_files(lay, {key: {"sha256": None}})


# ----------------------------------------------------------------------
#   IfExists and the action core
# ----------------------------------------------------------------------

def test_ifexists_update_value():
    for value in ("update", "u", "UPDATE", 6, IfExists.UPDATE):
        assert IfExists.from_any(value) is IfExists.UPDATE
    assert IfExists.current is None
    with IfExists("update"):
        assert IfExists.current is IfExists.UPDATE
    assert IfExists.current is None


def test_core_update_is_different(tmp_path):
    dst = tmp_path / "x.json"
    dst.write_text("old")
    # (no compare: an existing output is identical, as under different)
    action = WriteText("new", dst, ifexists="update")
    action()
    assert dst.read_text() == "old" and not action.rendered
    action = WriteText("new", dst, ifexists="update", compare=True)
    action()
    assert dst.read_text() == "new" and action.rendered
    # an identical compare holds the render; a skip does not
    action = WriteText("new", dst, ifexists="update", compare=True)
    statuses = [s.get("status") for s in action]
    assert "skipped" in statuses and action.rendered
    action = WriteText("other", dst, ifexists="skip")
    action()
    assert not action.rendered and dst.read_text() == "new"


def test_backup_only_when_the_action_writes(tmp_path, caplog):
    dst, backup = tmp_path / "x.json", tmp_path / "replaced" / "x.json"
    dst.write_text("same")
    action = WriteText("same", dst, ifexists="different", compare=True)
    action.backup = backup
    action()
    assert not backup.exists() and not warned(caplog, "replaced")

    def fail(file):
        raise ValueError("no")

    action = Action([], dst, fail, ifexists="overwrite")
    action.backup = backup
    action()
    assert not backup.exists() and dst.read_text() == "same"
    action = WriteText("new", dst, ifexists="different", compare=True)
    action.backup = backup
    action()
    assert backup.read_text() == "same" and dst.read_text() == "new"
    assert "replaced, the old file is in" in caplog.text


# ----------------------------------------------------------------------
#   Tracked files, cell by cell
# ----------------------------------------------------------------------

def _expected(mode: str, state: str) -> tuple[str, str | None, str]:
    """(content, recorded hash of, warning) after the second roast."""
    if mode == "error":
        return ("new" if state == "absent" else
                {"ours": "old", "edited": "edit", "unknown": "old"}[state],
                {"absent": "new", "ours": "old", "edited": "old",
                 "unknown": None}[state], "")
    if mode == "skip":
        return ({"absent": "new", "ours": "old", "edited": "edit",
                 "unknown": "old"}[state],
                {"absent": "new", "ours": "old", "edited": "old",
                 "unknown": None}[state], "")
    if mode == "update":
        if state == "edited":
            return "edit", "old", "was edited"
        return "new", "new", "replaced, the old file is in" \
            if state == "unknown" else ""
    return "new", "new", ""


@pytest.mark.parametrize("state", STATES)
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("which", sorted(TRACKED))
def test_tracked_file(tmp_path, caplog, which, mode, state):
    rel = TRACKED[which]
    root = make_root(tmp_path)
    roast(root)
    path = raw(root, rel)
    assert path.read_text() == f"old {rel}\n"
    assert entry(root, rel) == {"origin": "converted",
                                "sha256": digest(path)}
    texts = {"old": f"old {rel}\n", "new": f"new {rel}\n",
             "edit": "my edit\n"}
    hashes = {k: hashlib.sha256(v.encode()).hexdigest()
              for k, v in texts.items()}
    set_template(root, rel, texts["new"], later=10)
    if state == "absent":
        path.unlink()
    elif state == "edited":
        path.write_bytes(texts["edit"].encode())
    elif state == "unknown":
        forget_hash(root, rel)
    caplog.clear()
    report = roast(root, ifexists=mode)
    content, recorded, warning = _expected(mode, state)
    assert path.read_text() == texts[content]
    assert entry(root, rel).get("sha256") == \
        (hashes[recorded] if recorded else None)
    warnings = [r for r in caplog.records if r.levelname == "WARNING"
                and path.name in r.getMessage()]
    if warning:
        assert len(warnings) == 1 and warning in warnings[0].getMessage()
    else:
        assert not warnings
    backup = layout_of(root).replaced(path)
    assert backup.exists() == (mode == "update" and state == "unknown")
    if backup.exists():
        assert backup.read_text() == texts["old"]
    errors = [e for e in report.errors if e[1] == path.name]
    assert bool(errors) == (mode == "error" and state != "absent")


def test_skip_reads_no_hash(tmp_path, monkeypatch, caplog):
    root = make_root(tmp_path)
    roast(root)
    for rel in TRACKED.values():
        set_template(root, rel, "new\n", later=10)
    raw(root, SIDECAR).write_text("edited\n")

    def recorded(self, path):
        raise AssertionError("skip reads a hash")

    monkeypatch.setattr(RoastContext, "recorded", recorded)
    caplog.clear()
    report = roast(root)
    assert not report.errors and "WARNING" not in caplog.text
    assert raw(root, SIDECAR).read_text() == "edited\n"
    assert raw(root, "README").read_text() == "old README\n"


def test_update_rewrites_only_what_changed(tmp_path):
    root = make_root(tmp_path)
    roast(root)
    paths = [raw(root, rel) for rel in TRACKED.values()]
    first = [p.stat().st_mtime_ns for p in paths]
    roast(root, ifexists="update")
    # (identical compares: nothing written)
    assert [p.stat().st_mtime_ns for p in paths] == first
    set_template(root, "README", "new\n", later=10)
    roast(root, ifexists="update")
    assert raw(root, "README").read_text() == "new\n"
    assert [p.stat().st_mtime_ns for p in paths if p.name != "README"] \
        == [t for p, t in zip(paths, first) if p.name != "README"]
    assert raw(root, "README").read_text() == "new\n"


# ----------------------------------------------------------------------
#   Other outputs
# ----------------------------------------------------------------------

def test_pristine_tables_follow_under_skip(tmp_path):
    root = make_root(tmp_path)
    roast(root)
    tsv = raw(root, "participants.tsv")
    assert tsv.read_text() == "participant_id\nsub-01\n"
    # a new source table: the pristine copy is compared and replaced
    (raw(root, "sub-02")).mkdir()
    roast(root, subjects=["01", "02", "03"])
    original = layout_of(root).original(tsv)
    assert original.read_text().splitlines()[1:] == \
        ["sub-01", "sub-02", "sub-03"]
    # ... and the visible table re-trimmed to the subjects on disk
    assert tsv.read_text().splitlines()[1:] == ["sub-01", "sub-02"]
    assert not entry(root, "participants.tsv")      # (not tracked)


def test_data_under_update(tmp_path):
    root = make_root(tmp_path)
    roast(root)
    image = raw(root, IMAGE)
    set_template(root, IMAGE, "new image\n", later=10)
    for mode in ("update", "different"):
        roast(root, ifexists=mode)
        assert image.read_text() == f"old {IMAGE}\n"
    assert not entry(root, IMAGE)
    roast(root, ifexists="overwrite")
    assert image.read_text() == "new image\n"


def test_derivative_description_is_not_an_edit(tmp_path, caplog):
    """`finish_roast` rewrites it: the hash is that of the file on disk"""
    root = make_root(tmp_path)
    roast(root)
    lay = layout_of(root)
    path = lay.derivatives / "toy" / DESCRIPTION_NAME
    assert "SourceDatasets" in json.loads(path.read_text())
    key = path.relative_to(lay.root).as_posix()
    assert read_manifest(lay.state)["files"][key]["sha256"] == digest(path)
    caplog.clear()
    roast(root, ifexists="update")
    assert "WARNING" not in caplog.text
    assert read_manifest(lay.state)["files"][key]["sha256"] == digest(path)


def _opt_outs(ctx):
    raw = ctx.layout.raw
    yield WriteText("new\n", raw / "sub-01/anat/sub-01_compare.json",
                    compare=False)
    yield WriteText("new\n", raw / "sub-01/anat/sub-01_given.json",
                    ifexists="overwrite")
    yield Action([], [raw / "sub-01/anat/sub-01_a.json",
                      raw / "sub-01/anat/sub-01_b.json"],
                 _write_two, kind="sidecar")


def _write_two(a, b):
    a.write(b"new\n")
    b.write(b"new\n")


@pytest.mark.parametrize("mode", ["skip", "update", "different"])
def test_untracked_actions(tmp_path, mode):
    """`compare=False`, a roaster's `ifexists`, several outputs"""
    root = make_root(tmp_path)
    names = ("compare", "given", "a", "b")
    paths = [raw(root, f"sub-01/anat/sub-01_{n}.json") for n in names]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("old\n")
    roast(root, ifexists=mode, extra=_opt_outs)
    assert {p.name: p.read_text() for p in paths} == {
        paths[0].name: "old\n",              # left alone when it exists
        paths[1].name: "new\n",              # the roaster's overwrite
        paths[2].name: "old\n", paths[3].name: "old\n",
    }
    # (`compare=False` is tracked, `ifexists` given and two outputs not)
    assert not entry(root, "sub-01/anat/sub-01_given.json")
    assert not entry(root, "sub-01/anat/sub-01_a.json")


def test_sidecars_no(tmp_path):
    root = make_root(tmp_path)
    roast(root, json="no")
    assert not raw(root, SIDECAR).exists()
    assert entry(root, "README")["sha256"] == digest(raw(root, "README"))
    assert not entry(root, SIDECAR)


def _in_a_shard(ctx):
    yield Shard(_shard, tag="shard", id="roast/toy/shard",
                inputs=[template(ctx.layout.root, SIDECAR)])


def _shard(ctx):
    yield CopyBytes(template(ctx.layout.root, SIDECAR),
                    ctx.layout.raw / SIDECAR)
    yield CopyBytes(template(ctx.layout.root, "README"),
                    ctx.layout.raw / "README")


def test_digests_come_back_from_workers(tmp_path):
    root = make_root(tmp_path)
    report = roast(root, jobs=2, roaster=_in_a_shard)
    assert not report.errors
    for rel in (SIDECAR, "README"):
        assert entry(root, rel)["sha256"] == digest(raw(root, rel))
    # an edit, then `update` in a worker: kept
    raw(root, SIDECAR).write_text("mine\n")
    set_template(root, SIDECAR, "new\n", later=10)
    roast(root, ifexists="update", jobs=2, roaster=_in_a_shard)
    assert raw(root, SIDECAR).read_text() == "mine\n"


def _failing(ctx):
    yield from toy(ctx)
    yield Fail("something went wrong")


def test_partial_roast_records_hashes(tmp_path):
    root = make_root(tmp_path)
    report = roast(root, roaster=_failing)
    assert report.status == "partial"
    assert entry(root, SIDECAR)["sha256"] == digest(raw(root, SIDECAR))


# ----------------------------------------------------------------------
#   Older datasets (no recorded hash)
# ----------------------------------------------------------------------

def _forget_all(root):
    lay = layout_of(root)
    files = read_manifest(lay.state)["files"]
    update_files(lay, deleted=list(files))


@pytest.mark.parametrize("rule", ["keep", "replace"])
def test_older_dataset_under_update(tmp_path, caplog, monkeypatch, rule):
    monkeypatch.setattr(harvest_mod, "MIGRATION_RULE", rule)
    root = make_root(tmp_path)
    roast(root)
    _forget_all(root)
    set_template(root, "README", "new\n", later=10)
    sidecar, readme = raw(root, SIDECAR), raw(root, "README")
    caplog.clear()
    roast(root, ifexists="update")
    backup = layout_of(root).replaced(readme)
    # identical: adopted (hash recorded), without a warning
    assert entry(root, SIDECAR)["sha256"] == digest(sidecar)
    assert not warned(caplog, sidecar.name)
    if rule == "keep":
        # (compared, then kept with a warning: the README and the
        # derivative description, rewritten by `finish_roast`)
        assert readme.read_text() == "old README\n"
        assert warned(caplog, "has no recorded hash")
        assert len([r for r in caplog.records
                    if "has no recorded hash" in r.getMessage()]) == 2
        assert not entry(root, "README") and not backup.exists()
        return
    assert readme.read_text() == "new\n"
    assert backup.read_text() == "old README\n"
    assert "replaced, the old file is in" in caplog.text
    assert entry(root, "README")["sha256"] == digest(readme)
    # a second update: a template change, no backup
    backup.unlink()
    set_template(root, "README", "newer\n", later=20)
    caplog.clear()
    roast(root, ifexists="update")
    assert readme.read_text() == "newer\n"
    assert not backup.exists() and "WARNING" not in caplog.text


def test_compost_drops_entries_then_roast_writes_again(tmp_path):
    root = make_root(tmp_path)
    roast(root)
    assert entry(root, SIDECAR)
    compost(root, selection=Selection(exclude_subs={"01"}),
            sourcedata=False, yes=True)
    assert not raw(root, SIDECAR).exists() and not entry(root, SIDECAR)
    assert entry(root, "README")
    roast(root)
    assert entry(root, SIDECAR)["sha256"] == digest(raw(root, SIDECAR))


# ----------------------------------------------------------------------
#   Review round 1
# ----------------------------------------------------------------------

def test_edited_derivative_description_survives_updates(tmp_path, caplog):
    """An edited description is left exactly as it is (no
    `SourceDatasets` added); `different` replaces it with the render"""
    root = make_root(tmp_path)
    roast(root)
    lay = layout_of(root)
    path = lay.derivatives / "toy" / DESCRIPTION_NAME
    key = path.relative_to(lay.root).as_posix()
    recorded = read_manifest(lay.state)["files"][key]["sha256"]
    for drop in (False, True):
        mine = {**json.loads(path.read_text()), "Name": "MY EDIT"}
        if drop:
            mine.pop("SourceDatasets")
        path.write_bytes(json.dumps(mine).encode())
        edit = path.read_bytes()
        for mode in ("update", "update", "skip", "error"):
            caplog.clear()
            roast(root, ifexists=mode)
            assert path.read_bytes() == edit
            assert warned(caplog, "was edited") == (mode == "update")
            # (still an edit: the hash of what roast wrote is kept)
            assert read_manifest(lay.state)["files"][key]["sha256"] == \
                recorded
    assert not lay.replaced(path).exists()
    roast(root, ifexists="different")
    desc = json.loads(path.read_text())
    assert desc["Name"] == "toy" and "SourceDatasets" in desc
    assert read_manifest(lay.state)["files"][key]["sha256"] == digest(path)


@pytest.mark.parametrize("mode", ["skip", "error", "refresh"])
def test_existing_description_is_never_completed(tmp_path, mode):
    """Pure skip: a description that exists before the roast, without a
    recorded hash nor `SourceDatasets`, is left byte for byte (also under
    `error`; `refresh` rewrites it, then completes it)"""
    root = make_root(tmp_path)
    lay = layout_of(root)
    path = lay.derivatives / "toy" / DESCRIPTION_NAME
    path.parent.mkdir(parents=True)
    theirs = b'{"Name": "theirs", "BIDSVersion": "1.8.0"}'
    path.write_bytes(theirs)
    for _ in range(2):
        roast(root, ifexists=mode)
        if mode != "refresh":
            assert path.read_bytes() == theirs
    if mode == "refresh":
        # (no mtime to compare: rewritten, so completed)
        assert "SourceDatasets" in json.loads(path.read_text())
    else:
        assert path.relative_to(lay.root).as_posix() not in \
            read_manifest(lay.state)["files"]


def test_unrecorded_derivative_description_under_update(tmp_path):
    """No recorded hash, differs: backed up, then replaced by the render,
    completed with `SourceDatasets`"""
    root = make_root(tmp_path)
    roast(root)
    lay = layout_of(root)
    path = lay.derivatives / "toy" / DESCRIPTION_NAME
    key = path.relative_to(lay.root).as_posix()
    update_files(lay, {key: {"sha256": None}})
    path.write_bytes(b'{"Name": "theirs"}')
    roast(root, ifexists="update")
    assert lay.replaced(path).read_bytes() == b'{"Name": "theirs"}'
    desc = json.loads(path.read_text())
    assert desc["Name"] == "toy" and "SourceDatasets" in desc
    assert read_manifest(lay.state)["files"][key]["sha256"] == digest(path)


def test_stale_hash_is_no_edit(tmp_path, caplog):
    """A roast killed after it wrote a file, before it recorded its hash:
    the file holds the render, so `update` adopts it"""
    root = make_root(tmp_path)
    roast(root)
    sidecar = raw(root, SIDECAR)
    set_template(root, SIDECAR, "v2\n", later=10)
    sidecar.write_bytes(b"v2\n")            # (written; hash not recorded)
    caplog.clear()
    roast(root, ifexists="update")
    assert not warned(caplog, sidecar.name)
    assert entry(root, SIDECAR)["sha256"] == digest(sidecar)
    set_template(root, SIDECAR, "v3\n", later=20)
    roast(root, ifexists="update")
    assert sidecar.read_text() == "v3\n"
    # a real edit is still kept, with a warning
    sidecar.write_bytes(b"mine\n")
    set_template(root, SIDECAR, "v4\n", later=30)
    caplog.clear()
    roast(root, ifexists="update")
    assert sidecar.read_text() == "mine\n" and warned(caplog, "was edited")


def test_backups_are_never_replaced(tmp_path):
    root = make_root(tmp_path)
    roast(root)
    readme = raw(root, "README")
    backup = layout_of(root).replaced(readme)
    for n, text in enumerate(("one\n", "two\n", "three\n")):
        readme.write_bytes(f"before {text}".encode())
        forget_hash(root, "README")
        set_template(root, "README", text, later=10 * (n + 1))
        roast(root, ifexists="update")
        assert readme.read_text() == text
    assert backup.read_text() == "before one\n"
    assert backup.with_name("README.1").read_text() == "before two\n"
    assert backup.with_name("README.2").read_text() == "before three\n"


def test_hashes_are_read_once_per_manifest_version(tmp_path, monkeypatch):
    from brainspresso.roast import context as context_mod  # noqa: PLC0415

    root = make_root(tmp_path)
    roast(root)
    reads = []
    real = context_mod.manifest_files

    def spy(layout):
        reads.append(layout)
        return real(layout)

    monkeypatch.setattr(context_mod, "manifest_files", spy)
    lay = layout_of(root)
    first = context_mod._recorded_hashes(lay)
    assert context_mod._recorded_hashes(lay) is first and len(reads) == 1
    forget_hash(root, "README")                 # (a new manifest)
    assert "rawbids/README" not in context_mod._recorded_hashes(lay)
    assert len(reads) == 2


def _shards(ctx):
    for rel in TRACKED.values():
        yield Shard(_copy_one, rel, tag="shard", id=f"roast/toy/{rel}",
                    inputs=[template(ctx.layout.root, rel)])


def _copy_one(ctx, rel):
    yield CopyBytes(template(ctx.layout.root, rel), ctx.layout.raw / rel)


def test_update_with_jobs_over_several_shards(tmp_path, caplog):
    root = make_root(tmp_path)
    roast(root, jobs=2, roaster=_shards)
    raw(root, SIDECAR).write_bytes(b"mine\n")
    for rel in TRACKED.values():
        set_template(root, rel, f"new {rel}\n", later=10)
    caplog.clear()
    report = roast(root, ifexists="update", jobs=2, roaster=_shards)
    assert not report.errors
    for rel in TRACKED.values():
        path = raw(root, rel)
        if rel == SIDECAR:
            assert path.read_text() == "mine\n"
        else:
            assert path.read_text() == f"new {rel}\n"
            assert entry(root, rel)["sha256"] == digest(path)
