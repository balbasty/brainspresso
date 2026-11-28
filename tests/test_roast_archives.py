"""Shared archive helpers (`roast.archives`, `ExtractMembers` names)."""
import gzip
import io
import json
import logging
import os
import subprocess
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace

import nibabel as nib
import numpy as np
import pytest
import test_roast_bullitt
from roast_utils import init_layout, layout_of, make_tar, nii_bytes

from brainspresso.actions.action import Claim, Fail
from brainspresso.actions.archive import ExtractMember, ExtractMembers
from brainspresso.datasets.bullitt import roaster as bullitt
from brainspresso.datasets.indi.abide.i.roaster import roast as roast_abide1
from brainspresso.datasets.indi.abide.ii import roaster as abide2
from brainspresso.datasets.indi.cobre.roaster import roast as roast_cobre
from brainspresso.datasets.oasis.i.roaster import roast as roast_oasis1
from brainspresso.datasets.oasis.ii.roaster import roast as roast_oasis2
from brainspresso.datasets.oasis.iii.roaster import roast as roast_oasis3
from brainspresso.roast import archives
from brainspresso.roast.archives import (
    ArchiveError,
    BackwardRead,
    in_order,
    member_parts,
    names,
    reading,
)
from brainspresso.roast.helpers import archive_shards
from brainspresso.roast.policy import SEEN
from brainspresso.roast.sidecars import template_config
from brainspresso.utils.archive import (
    FolderArchive,
    TarArchive,
    is_unsafe,
    member_path,
)


def payload(n=1 << 18) -> bytes:
    """Incompressible bytes (a truncation then cuts the member)."""
    return os.urandom(n)


@pytest.fixture
def tarball(tmp_path):
    return make_tar(tmp_path / "data.tar.gz", {
        "./top/a.txt": b"a",
        "top/b.bin": payload(),
        "/top/c.txt": b"c",
    }, dirs=["top"])


class Spy(TarArchive):
    """A `TarArchive` that records closes and refuses to be listed."""

    closed = []

    def __exit__(self, *args):
        Spy.closed.append(self.path.name)
        return super().__exit__(*args)


@pytest.fixture
def spy():
    Spy.closed = []
    return Spy


# ----------------------------------------------------------------------
#   Names
# ----------------------------------------------------------------------

@pytest.mark.parametrize("name, parts", [
    ("a/b", ("a", "b")),
    ("./a//b/./c", ("a", "b", "c")),
    ("/a/b", ("a", "b")),
    ("a\\b", ("a", "b")),
    ("a/../b", None),
    ("..\\b", None),
])
def test_member_parts(name, parts):
    assert member_parts(name) == parts


@pytest.mark.parametrize("name, unsafe", [
    # POSIX
    ("a/b", False), ("./a/b", False), ("a/./b", False), ("a/b.c", False),
    ("a/../b", True), ("../a", True), ("..", True), ("/a/b", True),
    ("a//b", True), ("a/ /b", True), ("a/...", True), ("a/.. ", True),
    # Windows, judged as such on every platform
    ("\\a", True), ("a\\..\\b", True), ("a\\b", True),
    ("\\Windows\\evil.mgz", True), ("C:", True), ("C:evil", True),
    ("sub/C:/evil.mgz", True), ("sub/D:x/evil.mgz", True),
    ("\\\\server\\share\\x", True),
])
def test_is_unsafe(name, unsafe):
    assert is_unsafe(name) is unsafe
    assert (member_path(name) is None) is unsafe


def test_member_path():
    assert member_path("./a//b") is None
    assert member_path("./a/./b") == "a/b"
    assert member_path(".") is None


# ----------------------------------------------------------------------
#   Reading
# ----------------------------------------------------------------------

def test_iteration_is_lazy(tarball, monkeypatch, spy):
    """Regular files, normalised, in archive order; never listed"""
    def listing(self):
        raise AssertionError("listed")

    monkeypatch.setattr(spy, "getmembers", listing)
    monkeypatch.setattr(spy, "getnames", listing)
    with reading(tarball, opener=spy) as tar:
        members = list(tar)
    assert [m.name for m in members] == ["./top/a.txt", "top/b.bin",
                                         "/top/c.txt"]
    assert [m.path for m in members] == ["top/a.txt", "top/b.bin",
                                         "top/c.txt"]
    assert members[0].parts == ("top", "a.txt")
    assert members[0].offset < members[1].offset < members[2].offset
    assert isinstance(members[0].info, tarfile.TarInfo)
    assert spy.closed == ["data.tar.gz"]


def test_member_read_and_extract(tarball, tmp_path):
    with reading(tarball, opener=TarArchive) as tar:
        for member in tar:
            if member.path == "top/a.txt":
                assert member.read() == b"a"
                with member.open() as f:
                    assert io.TextIOWrapper(f).read() == "a"
            if member.path == "top/c.txt":
                action = ExtractMember(tar.archive, member.info,
                                       tmp_path / "c.txt")
                assert action.src == [tarball]
                action.extract(io.BytesIO())


def test_listing_and_index(tarball):
    with reading(tarball, opener=TarArchive) as tar:
        listing = tar.listing()
        index = tar.index()
    assert [m.path for m in listing] == ["top/a.txt", "top/b.bin",
                                         "top/c.txt"]
    assert list(index) == ["top/a.txt", "top/b.bin", "top/c.txt"]
    assert index["top/a.txt"].name == "./top/a.txt"
    assert names(tarball, opener=TarArchive) == list(index)


def test_folder_archive(tmp_path):
    folder = tmp_path / "IXI-T1"
    folder.mkdir()
    (folder / "IXI002-Guys-0828-T1.nii.gz").write_bytes(b"x")
    (folder / "notes.txt").write_bytes(b"x")
    with reading(folder, opener=FolderArchive) as tar:
        (member,) = list(tar)
        assert member.read() == b"x" and member.offset == 0
    assert names(folder, opener=FolderArchive) == [
        "IXI002-Guys-0828-T1.nii.gz"]


def test_folder_listing_error(tmp_path):
    """A folder that cannot be listed: `ArchiveError`, not a raw
    `OSError`"""
    with pytest.raises(ArchiveError, match="IXI-T1: cannot be read "
                                           r"\(FileNotFoundError"):
        with reading(tmp_path / "IXI-T1", opener=FolderArchive) as tar:
            list(tar)
    with pytest.raises(ArchiveError):
        names(tmp_path / "IXI-T1", opener=FolderArchive)


def test_names_extractable(tmp_path):
    path = make_tar(tmp_path / "t.tar.gz", {
        "./a/x": b"", "/a/y": b"", "a/C:z": b"", "a\\w": b"", "a/v": b""})
    assert names(path, opener=TarArchive) == ["a/x", "a/y", "a/C:z",
                                              "a/w", "a/v"]
    assert names(path, opener=TarArchive, extractable=True) == ["a/x",
                                                                "a/v"]


def test_outside_members_skipped(tmp_path, caplog):
    path = make_tar(tmp_path / "evil.tar.gz", {
        "a/../../x": b"x", "../y": b"y", "ok": b"ok"})
    with caplog.at_level(logging.WARNING):
        with reading(path, opener=TarArchive) as tar:
            assert [m.path for m in tar] == ["ok"]
    assert caplog.text.count("outside the archive") == 1
    assert "evil.tar.gz: 2 member(s) outside the archive" in caplog.text


def test_in_order():
    members = [SimpleNamespace(offset=o) for o in (30, 10, 20)]
    actions = in_order([([members[0]], "c"), ([], "first"),
                        ([members[2], members[1]], "a"),
                        ([members[2]], "b"), ([], "second")])
    assert list(actions) == ["first", "second", "a", "b", "c"]


# ----------------------------------------------------------------------
#   Closing
# ----------------------------------------------------------------------

def test_closed_on_consumer_error(tarball, spy):
    with pytest.raises(ValueError, match="consumer"):
        with reading(tarball, opener=spy) as tar:
            for _ in tar:
                raise ValueError("consumer")
    assert spy.closed == ["data.tar.gz"]


def test_closed_with_the_generator(tarball, spy):
    def roaster():
        with reading(tarball, opener=spy) as tar:
            for member in tar:
                yield member

    gen = roaster()
    next(gen)
    assert spy.closed == []
    gen.close()
    assert spy.closed == ["data.tar.gz"]


def test_consumer_oserror_not_converted(tarball):
    """An `OSError` of the roaster's own code is not "cannot be read" """
    with pytest.raises(PermissionError):
        with reading(tarball, opener=TarArchive) as tar:
            for _ in tar:
                raise PermissionError("output folder")


@pytest.mark.parametrize("content", [b"not a tarball", None])
def test_error_on_open(tmp_path, content):
    path = tmp_path / "bad.tgz"
    if content is not None:
        path.write_bytes(content)
    with pytest.raises(ArchiveError) as info:
        with reading(path, opener=TarArchive):
            pass
    assert str(info.value).startswith("bad.tgz: cannot be read (")
    assert info.value.path == path


def truncate(path: Path, fraction: float) -> Path:
    data = path.read_bytes()
    path.write_bytes(data[:int(len(data) * fraction)])
    return path


def test_error_mid_iteration(tmp_path, spy):
    path = truncate(make_tar(tmp_path / "t.tar.gz", {
        "a": b"a", "b": payload(), "c": b"c"}), 0.5)
    seen = []
    with pytest.raises(ArchiveError, match="t.tar.gz: cannot be read"):
        with reading(path, opener=spy) as tar:
            for member in tar:
                seen.append(member.name)
    assert seen == ["a", "b"]
    assert spy.closed == ["t.tar.gz"]


def test_error_mid_read(tmp_path):
    path = truncate(make_tar(tmp_path / "t.tar.gz", {"b": payload()}), 0.5)
    with pytest.raises(ArchiveError) as info:
        with reading(path, opener=TarArchive) as tar:
            for member in tar:
                member.read()
    assert isinstance(info.value.cause, (EOFError, tarfile.TarError))


def test_error_listing(tarball, monkeypatch, spy):
    def listing(self):
        raise EOFError("truncated")

    monkeypatch.setattr(spy, "getmembers", listing)
    with pytest.raises(ArchiveError, match=r"\(EOFError: truncated\)"):
        with reading(tarball, opener=spy) as tar:
            tar.listing()
    assert spy.closed == ["data.tar.gz"]


def test_fail():
    error = ArchiveError(Path("/x/disc1.tar.gz"), EOFError("cut"))
    fail = error.fail(tag="discs")
    assert isinstance(fail, Fail)
    assert fail.message == "disc1.tar.gz: cannot be read (EOFError: cut)"
    assert (fail.name, fail.tag) == ("disc1.tar.gz", "discs")
    error = ArchiveError("/x/s/rest.tar.gz", OSError("no"), label="s/rest")
    fail = error.fail(tag="rest", name="n", note="; not written")
    assert fail.message == "s/rest: cannot be read (OSError: no); not " \
        "written"
    assert fail.name == "n"


# ----------------------------------------------------------------------
#   Strict (forward only)
# ----------------------------------------------------------------------

def test_strict_backward_read(tarball):
    with reading(tarball, opener=TarArchive, strict=True) as tar:
        members = list(tar)
        members[1].read(10)
        members[1].read(10)             # (the same member: allowed)
        members[2].read()
        with pytest.raises(BackwardRead):
            members[0].read()
        with pytest.raises(BackwardRead):   # (extracting, by name too)
            tar.archive.extractfile(members[0].name)


def test_strict_from_environment(tarball, monkeypatch):
    monkeypatch.setenv(archives.STRICT_ENV, "1")
    with reading(tarball, opener=TarArchive) as tar:
        members = list(tar)
        members[2].read()
        with pytest.raises(BackwardRead):
            members[0].read()
    monkeypatch.setenv(archives.STRICT_ENV, "0")
    with reading(tarball, opener=TarArchive) as tar:
        members = list(tar)
        members[2].read()
        assert members[0].read() == b"a"


# ----------------------------------------------------------------------
#   ExtractMembers: unsafe names
# ----------------------------------------------------------------------

def test_extract_members_drops_unsafe_names(tmp_path, caplog):
    path = make_tar(tmp_path / "fs.tar.gz", {
        "./s/mri/aseg.mgz": b"1",
        "s/mri/../../../../EVIL.txt": b"evil",
        "/etc/evil": b"evil",
        "s\\..\\..\\evil": b"evil",
        "s/C:/Windows/evil.mgz": b"evil",
        "s/\\Windows\\evil.mgz": b"evil",
        "s/surf/lh.white": b"2",
    })
    seen = []

    def rename(name):
        seen.append(name)
        return tmp_path.joinpath("out", *name.split("/"))

    action = ExtractMembers(path, Claim(tmp_path / "out"), rename)
    with caplog.at_level(logging.WARNING):
        dsts = [Path(a.dst).relative_to(tmp_path / "out").as_posix()
                for a in action.extract()]
    assert seen == ["s/mri/aseg.mgz", "s/surf/lh.white"]    # normalised
    assert dsts == ["s/mri/aseg.mgz", "s/surf/lh.white"]
    assert "fs.tar.gz: 5 member(s) outside the archive" in caplog.text


def test_extract_members_keeps_outputs_in_the_claim(tmp_path, caplog):
    """Second line of defence: a `rename` that leaves the claimed folder
    (whatever the name) is not obeyed"""
    path = make_tar(tmp_path / "t.tar.gz", {"a": b"", "b": b"", "c": b""})
    out = tmp_path / "out"
    targets = {"a": out / "x" / "a", "b": out / ".." / "b",
               "c": tmp_path / "elsewhere" / "c"}
    action = ExtractMembers(path, Claim(out), targets.get)
    with caplog.at_level(logging.WARNING):
        dsts = [Path(a.dst) for a in action.extract()]
    assert dsts == [out / "x" / "a"]
    assert "t.tar.gz: 2 member(s) renamed outside" in caplog.text
    # (several claimed folders: inside any of them)
    action = ExtractMembers(path, Claim([out, tmp_path / "elsewhere"]),
                            targets.get)
    assert len(list(action.extract())) == 2


def test_extract_members_bounded_by_root(tmp_path, caplog):
    """With `root` (the dataset), outputs outside the claim but inside
    the root are written; an escape outside the root is still dropped"""
    root = tmp_path / "DS"
    path = make_tar(tmp_path / "t.tar.gz", {"a": b"", "b": b"", "c": b""})
    claim = root / "rawbids" / "sub-01" / "ses-1"
    targets = {"a": claim / "func" / "a",
               "b": root / "rawbids" / "sub-01" / "ses-2" / "func" / "b",
               "c": root / ".." / "c"}
    action = ExtractMembers(path, Claim(claim), targets.get, root=root)
    with caplog.at_level(logging.WARNING):
        dsts = [Path(a.dst) for a in action.extract()]
    assert dsts == [targets["a"], targets["b"]]
    assert "t.tar.gz: 1 member(s) renamed outside" in caplog.text


def test_oasis3_session_of_the_member_name(tmp_path):
    """OASIS-III: a member named after another session than its folder
    is written to that session (outside the claim of its tarball, inside
    the dataset)"""
    root = tmp_path / "OASIS-3"
    init_layout(root, "study")
    folder = root / "sourcedata" / "OAS30001_MR_d0000"
    b = "OAS30001_MR_d0000/func1/NIFTI/sub-OAS30001_sess-d0003"
    make_tar(folder / "func1.tar.gz", {
        f"{b}_task-rest_bold.json": b"{}",
        f"{b}_task-rest_bold.nii.gz": nii_bytes((2, 2, 2, 3)),
    })
    report = roast_oasis3(root, keys={"bold"}, display=False)
    assert not report.errors, report.errors
    func = layout_of(root).raw / "sub-0001" / "ses-d0003" / "func"
    assert {p.name for p in func.iterdir()} == {
        "sub-0001_ses-d0003_task-rest_bold.json",
        "sub-0001_ses-d0003_task-rest_bold.nii.gz"}


def test_oasis1_freesurfer_unsafe_member(tmp_path, caplog):
    """OASIS-I joins the parts of a member after its subject folder: a
    `..` member is dropped before it does"""
    root = tmp_path / "OASIS-1"
    init_layout(root, "study")
    s = "disc1/OAS1_0001_MR1"
    make_tar(root / "sourcedata" / "oasis_cs_freesurfer_disc1.tar.gz", {
        f"{s}/stats/aseg.stats": b"stats",
        # (into `tmp_path`, without the check)
        f"{s}/../../../../EVIL.txt": b"evil",
    })
    with caplog.at_level(logging.WARNING):
        report = roast_oasis1(root, keys={"fs-all"}, discs=[1],
                              display=False)
    assert not report.errors
    assert "1 member(s) outside the archive" in caplog.text
    unpacked = layout_of(root).derivative("oasis-freesurfer").source
    assert (unpacked / "sub-0001/stats/aseg.stats").exists()
    assert not list(tmp_path.rglob("EVIL.txt"))


def test_oasis3_freesurfer_unsafe_member(tmp_path, caplog):
    """OASIS-III drops the first six parts of a member: a `..` member is
    dropped before it does"""
    root = tmp_path / "OASIS-3"
    init_layout(root, "study")
    lead = "a/b/c/d/e/f"
    make_tar(root / "sourcedata" / "OAS30001_MR_d0129" /
             "OAS30001_Freesurfer53_d0129.tar.gz", {
                 f"{lead}/stats/aseg.stats": b"stats",
                 # (into `tmp_path`, without the check)
                 f"{lead}/../../../../../../EVIL.txt": b"evil",
             })
    with caplog.at_level(logging.WARNING):
        report = roast_oasis3(root, keys={"fs-all"}, display=False)
    assert not report.errors
    assert "1 member(s) outside the archive" in caplog.text
    unpacked = layout_of(root).derivative("oasis-freesurfer").source
    assert (unpacked / "sub-0001/ses-d0129/stats/aseg.stats").exists()
    assert not list(tmp_path.rglob("EVIL.txt"))


# ----------------------------------------------------------------------
#   Spools
# ----------------------------------------------------------------------

def test_spool_errors(tmp_path):
    path = truncate(make_tar(tmp_path / "t.tar.gz", {"b": payload()}), 0.5)
    # writing: returned (a `Fail` of that output)
    assert archives.spool(io.BytesIO(b"x"), tmp_path / "no" / "file") \
        .startswith("FileNotFoundError")
    # reading the member: raised (the archive cannot be read)
    with pytest.raises(ArchiveError):
        with reading(path, opener=TarArchive) as tar:
            for member in tar:
                with member.open() as src:
                    archives.spool(src, tmp_path / "spooled")
    archives.place(tmp_path / "spooled", tmp_path / "placed")
    assert (tmp_path / "placed").exists()
    with pytest.raises(FileNotFoundError):
        archives.place(None, tmp_path / "x")


def test_sweep_spools(tmp_path):
    ctx = SimpleNamespace(layout=SimpleNamespace(state=tmp_path))
    tmp = tmp_path / "tmp"
    tmp.mkdir()
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    host = archives.spool_host()
    stale = tmp / f"x-{host}-{dead.pid}-abc"
    alive = tmp / f"x-{host}-{os.getpid()}-abc"
    other = tmp / f"x-elsewhere-{dead.pid}-abc"
    another = tmp / f"y-{host}-{dead.pid}-abc"
    for folder in (stale, alive, other, another):
        folder.mkdir()
    assert archives.spool_prefix("x") == f"x-{host}-{os.getpid()}-"
    assert archives.sweep_spools(ctx, "x") == [stale]
    assert alive.exists() and other.exists() and another.exists()
    assert archives.spool_folder(ctx) == tmp


# ----------------------------------------------------------------------
#   Shards and templates
# ----------------------------------------------------------------------

def test_archive_shards(tmp_path):
    progress = []
    ctx = SimpleNamespace(progress=lambda **kw: progress.append(kw))
    paths = [tmp_path / "a.tgz", tmp_path / "b.tgz"]
    shards = list(archive_shards(ctx, paths, print, tag="raw",
                                 keys=("T1w", "bold")))
    assert progress == [{"total": 2, "tag": "raw"}]
    assert [s.id for s in shards] == ["roast/raw/a.tgz", "roast/raw/b.tgz"]
    assert [s.inputs for s in shards] == [[p] for p in paths]
    assert all(s.config is None and s.tag == s.advance == "raw"
               for s in shards)
    assert shards[0].scope == {"keys": ["T1w", "bold"], "sub": SEEN}
    assert shards[0].args == (paths[0],)
    shards = list(archive_shards(
        ctx, paths, print, tag="discs", keys=None,
        id=lambda p: f"roast/discs/{p.stem}", inputs=lambda p: [p, p],
        config=lambda p: {"name": p.name}))
    assert [s.id for s in shards] == ["roast/discs/a", "roast/discs/b"]
    assert shards[1].inputs == [paths[1]] * 2
    assert shards[1].config == {"name": "b.tgz"}
    assert shards[1].scope["keys"] is None


def test_template_config(tmp_path):
    (tmp_path / "T1w.json").write_text("{}")
    (tmp_path / "bold.json").mkdir()                # (cannot be read)
    ctx = SimpleNamespace(resource=lambda name: tmp_path / name
                          if (tmp_path / name).exists() else None)
    config = template_config(ctx.resource,
                             ["T1w.json", "bold.json", "dwi.json"])
    assert config["T1w.json"] == [
        (tmp_path / "T1w.json").as_posix(),
        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"]
    assert config["bold.json"][1].startswith("unreadable: ")
    assert config["dwi.json"] is None


# ----------------------------------------------------------------------
#   Roasters: one message, narrow errors
# ----------------------------------------------------------------------

def test_cobre_corrupt_tarball(tmp_path):
    """A corrupt tarball: one `Fail` named after it, under `scans`"""
    root = tmp_path / "COBRE"
    init_layout(root, "study")
    make_tar(root / "sourcedata" / "COBRE_scan_data.tgz", {
        "COBRE/0040000/session_1/anat_1/mprage.nii.gz": payload(),
        "COBRE/0040001/session_1/anat_1/mprage.nii.gz": payload()})
    truncate(root / "sourcedata" / "COBRE_scan_data.tgz", 0.4)
    report = roast_cobre(root, keys=["T1w"], json="no", display=False)
    # (and the member that the truncation cut, an error of its output)
    assert report.counts["scans"] == {"error": 1}
    assert [e[1] for e in report.errors if "cannot be read" in e[2]] == [
        "COBRE_scan_data.tgz"]
    assert any(e[2].startswith("COBRE_scan_data.tgz: cannot be read (EOF")
               for e in report.errors)


def test_oasis_message_form(tmp_path):
    root = tmp_path / "OASIS-2"
    init_layout(root, "study")
    (root / "sourcedata").mkdir()
    (root / "sourcedata" / "OAS2_RAW_PART1.tar.gz").write_bytes(b"garbage")
    report = roast_oasis2(root, keys={"raw"}, parts=[1], display=False)
    assert report.counts["parts"] == {"error": 1}
    assert "OAS2_RAW_PART1.tar.gz: cannot be read (ReadError" in \
        str(report.errors[0])


def test_output_folder_error_is_not_an_archive_error(tmp_path, monkeypatch):
    """ABIDE-II: listing the output folder fails the shard under its own
    type, not as "cannot be read" """
    root = tmp_path / "ABIDE-2"
    init_layout(root, "study")
    make_tar(root / "sourcedata" / "ABIDEII-NYU_1.tar.gz", {
        "ABIDEII-NYU_1/29001/session_1/anat_1/anat.nii.gz": b"x"})

    def written(self, *args):
        raise PermissionError("output folder")

    monkeypatch.setattr(abide2.Session, "written", written)
    report = abide2.roast(root, keys="T1w", display=False)
    messages = " ".join(map(str, report.errors))
    assert "PermissionError" in messages and "output folder" in messages
    assert "cannot be read" not in messages


# ----------------------------------------------------------------------
#   FreeSurfer: `--sidecars only` sees what the extraction sees
# ----------------------------------------------------------------------

def mgz_bytes(value=1) -> bytes:
    img = nib.MGHImage(np.full((2, 2, 2), value, np.float32), np.eye(4))
    return gzip.compress(img.to_bytes())


def sidecars(root: Path, name: str) -> dict:
    """The sidecars (and descriptions) of a derivative, with content."""
    drv = layout_of(root).derivative(name)
    return {p.relative_to(drv.root).as_posix(): json.loads(p.read_text())
            for p in sorted(drv.root.rglob("*.json"))
            if drv.source not in p.parents}


def same_sidecars(tmp_path, build, roast, name, prefix):
    """Sidecars of a full roast and of `--sidecars only` (fresh datasets
    of the same tarball)"""
    out = []
    for json_ in ("yes", "only"):
        root = tmp_path / json_ / "DS"
        init_layout(root, "study")
        build(root, prefix)
        report = roast(root, json_)
        assert not report.errors, report.errors
        out.append({k: v for k, v in sidecars(root, name).items()
                    if not k.endswith("dataset_description.json")})
    return out


@pytest.mark.parametrize("prefix", ["", "./", "/"])
def test_oasis1_freesurfer_sidecars_only_names(tmp_path, prefix):
    """OASIS-I: the same names in the listing (`--sidecars only`) and in
    the extraction; `./disc1/...` is read (`mri/orig` runs too), a
    leading `/` is dropped by both"""
    def build(root, prefix):
        s = f"{prefix}disc1/OAS1_0001_MR1"
        make_tar(root / "sourcedata" / "oasis_cs_freesurfer_disc1.tar.gz", {
            f"{s}/mri/aseg.mgz": mgz_bytes(),
            f"{s}/mri/rawavg.mgz": mgz_bytes(),
            f"{s}/mri/orig/001.mgz": mgz_bytes(),       # (raw runs 1, 2)
            f"{s}/mri/orig/002.mgz": mgz_bytes(),
        })

    full, only = same_sidecars(
        tmp_path, build, lambda root, json_: roast_oasis1(
            root, keys={"fs"}, json=json_, discs=[1], display=False),
        "oasis-freesurfer", prefix)
    assert full == only
    orig = "sub-0001/anat/sub-0001_desc-orig_T1w.json"
    if prefix == "/":
        assert not any(k.startswith("sub-") for k in full)
        return
    assert full[orig]["Sources"] == [
        f"bids:raw:sub-0001/anat/sub-0001_run-{run}_T1w.nii.gz"
        for run in (1, 2)]


@pytest.mark.parametrize("prefix", ["", "./", "/"])
def test_abide1_freesurfer_sidecars_only_names(tmp_path, prefix):
    """ABIDE-I: the same names in the listing (`--sidecars only`) and in
    the extraction"""
    label = "Pitt_0050003"

    def build(root, prefix):
        lead = f"{prefix}{label}/assessors/fs/files/{label}"
        make_tar(root / "sourcedata" / label / f"{label}-fs.tar.gz", {
            f"{lead}/mri/aseg.mgz": mgz_bytes(),
            f"{lead}/mri/orig/001.mgz": mgz_bytes(),
        })

    full, only = same_sidecars(
        tmp_path, build, lambda root, json_: roast_abide1(
            root, source="xnat", keys={"fs"}, json=json_, display=False),
        "abide-freesurfer", prefix)
    assert full == only
    assert any(k.startswith("sub-50003/") for k in full) is (prefix != "/")


# ----------------------------------------------------------------------
#   Bullitt: the templates that a subject shard copies are signed
# ----------------------------------------------------------------------

def test_bullitt_signs_the_templates_it_copies(tmp_path, monkeypatch):
    root = tmp_path / "Bullitt"
    init_layout(root, "study")
    test_roast_bullitt.make_source(root)
    calls = []
    real = bullitt.roast_subject

    def spy(ctx, sub):
        calls.append(sub)
        return real(ctx, sub)

    monkeypatch.setattr(bullitt, "roast_subject", spy)

    # (keys as exclusions: named keys make missing scans errors)
    others = sorted({"meta", *bullitt.RAW, *bullitt.DERIVATIVES} - {"T2w"})

    def again(**options):
        del calls[:]
        report = bullitt.roast(root, exclude_keys=others, sub=[1],
                               display=False, **options)
        assert not report.errors, report.errors
        return sorted(calls)

    assert again() == ["001"]
    assert again() == []
    src = root / "sourcedata"
    # a template of a key that is not wanted: not signed
    (src / "angio.json").write_text('{"Changed": 1}')
    assert again() == []
    # the template of a wanted key: signed
    (src / "T2w.json").write_text('{"Changed": 1}')
    assert again() == ["001"]
    # no sidecars written: no template signed
    assert again(json="no") == ["001"]
    (src / "T2w.json").write_text('{"Changed": 2}')
    assert again(json="no") == []
