"""Characterization tests for the OASIS-III roast step."""
import json
import logging

import nibabel as nib
import pytest
from cli_utils import bx
from roast_utils import (
    LAYOUTS,
    check_keys_rerun,
    init_layout,
    layout_of,
    make_tar,
    nii_bytes,
    raw_file,
    raw_tree,
    read_tsv,
    scoped_records,
)

from brainspresso.bids.tables import trim_tables
from brainspresso.datasets.oasis.iii import roaster
from brainspresso.datasets.oasis.iii.roaster import roast

DEMO = (
    "OASISID,GENDER,HAND,AgeatEntry,AgeatDeath,EDUC,SES,racecode,AIAN,NHPI,"
    "ASIAN,AA,WHITE,daddem,momdem,APOE\n"
    "OAS30001,2,R,65.1,,12,2,5,0,0,0,0,1,0,1,33\n"
    "OAS30002,1,L,70.0,80.5,16,,2,0,0,1,0,0,5,0,34\n"
)
CDR = (
    "OASISID,OASIS_session_label,days_to_visit,age at visit,CDRTOT,dx1,"
    "dx1_code\n"
    "OAS30001,OAS30001_UDSb4_d0000,0,65.1,0,Cognitively normal,1\n"
    "OAS30001,OAS30001_UDSb4_d0339,339,66.0,0.5,AD Dementia,2\n"
    "OAS30002,OAS30002_UDSb4_d0000,0,70.0,,.,3\n"
)
SIDECAR = {"MagneticFieldStrength": 3}

T1 = "sub-0001/ses-d0129/anat/sub-0001_ses-d0129_T1w"
T2 = "sub-0002/ses-d0000/anat/sub-0002_ses-d0000_T2w"
META = {"README", "dataset_description.json", "participants.json",
        "participants.tsv", "sessions.json"}
PHENO = {"phenotype/UDS_b4_cdr.tsv", "phenotype/UDS_b4_cdr.json"}


def make_source(root):
    """Write the harvested files of OASIS-III."""
    src = root / "sourcedata"
    d = src / "OASIS3_data_files"
    make_tar(d / "demo.tar.gz", {"demo/demo.csv": DEMO.encode()})
    make_tar(d / "UDSb4.tar.gz", {"b4/cdr.csv": CDR.encode()})
    for sub, ses, mod in [(1, "d0129", "T1w"), (2, "d0000", "T2w")]:
        b = f"OAS3{sub:04d}_MR_{ses}/anat1/NIFTI/sub-OAS3{sub:04d}_sess-{ses}"
        make_tar(src / f"OAS3{sub:04d}_MR_{ses}" / "anat1.tar.gz", {
            f"{b}_{mod}.json": json.dumps(SIDECAR).encode(),
            f"{b}_{mod}.nii.gz": nii_bytes(),
        })
    return root


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    root = tmp_path / "OASIS-3"
    init_layout(root, request.param)
    return make_source(root)


def test_age_units():
    """`age` in years: BIDS unit "year" (not "years")"""
    for name in ('participants.json', 'sessions.json'):
        meta = json.loads((roaster.TEMPLATES / name).read_text())
        assert meta["age"]["Units"] == "year"


def test_roast_all(root):
    roast(root, keys={"meta", "pheno", "T1w", "T2w"})
    assert raw_tree(root) == META | PHENO | {
        f"{T1}.json", f"{T1}.nii.gz", f"{T2}.json", f"{T2}.nii.gz",
        "sub-0001/sub-0001_sessions.tsv", "sub-0002/sub-0002_sessions.tsv",
    }
    assert json.loads(raw_file(root, f"{T1}.json").read_text()) == SIDECAR
    assert nib.load(raw_file(root, f"{T2}.nii.gz")).shape == (2, 2, 2)

    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert rows[0] == list(roaster.PARTICIPANTS_HEADER_MAP)
    assert rows[1] == ["sub-0001", "F", "R", "65.1", "n/a", "12", "2", "W",
                       "N", "N", "N", "N", "Y", "N", "Y", "33"]
    assert rows[2][:8] == ["sub-0002", "M", "L", "70.0", "80.5", "16",
                           "n/a", "A"]
    assert rows[2][13] == "U"

    rows = read_tsv(raw_file(root, "phenotype/UDS_b4_cdr.tsv"))
    assert rows[0] == ["participant_id", "session_id", "delay", "age",
                       "CDRTOT", "DX1", "DX1_CODE"]
    # (no session of sub-0001 on day 0: session_id n/a)
    assert rows[1] == ["sub-0001", "n/a", "0", "65.1", "0",
                       "Cognitively normal", "1"]
    assert rows[3][4:6] == ["n/a", "n/a"]
    cdr = json.loads(raw_file(root, "phenotype/UDS_b4_cdr.json").read_text())
    assert {"MeasurementToolMetadata", "delay", "age", "CDRTOT"} <= set(cdr)

    # closest CDR visit to day 129 is day 0
    assert read_tsv(raw_file(root, "sub-0001/sub-0001_sessions.tsv")) == [
        ["session_id", "pathology", "age"], ["ses-d0129", "1", "65.4532"],
    ]


def test_participants_json_columns():
    """Every column of `participants.tsv` (but the id) is described
    (`daddem` was misspelled `dadem`)"""
    meta = json.loads((roaster.TEMPLATES / "participants.json").read_text())
    assert set(roaster.PARTICIPANTS_HEADER_MAP) - {"participant_id"} \
        == set(meta)


def pristine(root, name):
    """Pristine copy of a phenotype table (every visit)"""
    return read_tsv(layout_of(root).original(raw_file(root, name)))


def test_phenotype_sessions(root, caplog):
    """Every per-visit phenotype table: written complete to the state
    folder, with the visit's day as `session_id`; the visible table has
    the subjects on disk, and `session_id` n/a when the subject has no
    session that day in the raw dataset; the day is in `delay`"""
    a1 = (
        "OASISID,OASIS_session_label,days_to_visit,age at visit,EDUC\n"
        "OAS30002,OAS30002_UDSa1_d0000,0,70.0,16\n"
        "OAS30002,OAS30002_UDSa1_d0400,400,71.1,16\n"
    )
    make_tar(root / "sourcedata" / "OASIS3_data_files" / "UDSa1.tar.gz",
             {"a1/a1.csv": a1.encode()})
    with caplog.at_level(logging.INFO):
        roast(root, keys={"pheno", "T1w", "T2w"})
    assert [r[:3] for r in pristine(root, "phenotype/UDS_b4_cdr.tsv")[1:]] \
        == [["sub-0001", "ses-d0000", "0"], ["sub-0001", "ses-d0339", "339"],
            ["sub-0002", "ses-d0000", "0"]]
    cdr = read_tsv(raw_file(root, "phenotype/UDS_b4_cdr.tsv"))
    assert [r[:3] for r in cdr[1:]] == [
        ["sub-0001", "n/a", "0"], ["sub-0001", "n/a", "339"],
        ["sub-0002", "ses-d0000", "0"]]
    demo = read_tsv(raw_file(root, "phenotype/UDS_a1_demographics.tsv"))
    assert [r[:3] for r in demo[1:]] == [
        ["sub-0002", "ses-d0000", "0"], ["sub-0002", "n/a", "400"]]
    assert "UDS_b4_cdr.tsv: session_id n/a in 2 of 3 rows" in caplog.text
    for name in ("UDS_b4_cdr", "UDS_a1_demographics"):
        meta = json.loads(raw_file(root, f"phenotype/{name}.json")
                          .read_text())
        assert meta["session_id"] == roaster.SESSION_ID


def test_phenotype_sessions_follow_the_disk(root):
    """`--keys pheno` first (no image: no row), then a T1w roast under
    the default `skip`: the table follows what is on disk (sub-0001,
    no session on its visit days); then T2w: sub-0002's visit gets its
    session"""
    roast(root, keys={"pheno"})
    assert read_tsv(raw_file(root, "phenotype/UDS_b4_cdr.tsv"))[1:] == []
    roast(root, keys={"T1w"})
    cdr = read_tsv(raw_file(root, "phenotype/UDS_b4_cdr.tsv"))
    assert [r[:2] for r in cdr[1:]] == [["sub-0001", "n/a"],
                                        ["sub-0001", "n/a"]]
    roast(root, keys={"T2w"})
    cdr = read_tsv(raw_file(root, "phenotype/UDS_b4_cdr.tsv"))
    assert [r[:2] for r in cdr[1:]][-1] == ["sub-0002", "ses-d0000"]


def test_trim_phenotype_sessions(tmp_path):
    """`trim_tables(sessions=True)` (pristine copies of roasters): a
    `session_id` that names no session on disk is n/a; harvested tables
    (`sessions=False`) keep it"""
    root = tmp_path / "DS"
    (root / "sub-01" / "ses-1").mkdir(parents=True)
    orig = tmp_path / "visits.tsv"
    orig.write_text("participant_id\tsession_id\tx\nsub-01\tses-1\t1\n"
                    "sub-01\tses-2\t2\nsub-01\tn/a\t3\n")
    rel = "phenotype/visits.tsv"
    for sessions, expected in ((True, ["ses-1", "n/a", "n/a"]),
                               (False, ["ses-1", "ses-2", "n/a"])):
        trim_tables(root, {rel: orig}, sessions=sessions)
        assert [r[1] for r in read_tsv(root / rel)[1:]] == expected


def test_phenotype_sessions_of_the_raw_dataset(root):
    """`session_id` names only sessions that the raw dataset has: not a
    session of sourcedata that is not roasted (a FreeSurfer-only
    folder)"""
    make_tar(root / "sourcedata" / "OAS30001_MR_d0339" /
             "OAS30001_Freesurfer53_d0339.tar.gz",
             {"a/b/c/d/e/f/mri/aseg.mgz": b"x"})
    roast(root, keys={"pheno", "T1w"})
    cdr = read_tsv(raw_file(root, "phenotype/UDS_b4_cdr.tsv"))
    assert [r[1] for r in cdr[1:]] == ["n/a", "n/a"]


def test_phenotype_truncated_rows(root, caplog):
    """Blank rows are dropped, truncated rows (no participant or visit)
    skipped with a warning, short rows padded with n/a"""
    cdr = (CDR + "\n" + "OAS30001\n" + ",,,\n"
           + "OAS30002,OAS30002_UDSb4_d0400,400\n")
    make_tar(root / "sourcedata" / "OASIS3_data_files" / "UDSb4.tar.gz",
             {"b4/cdr.csv": cdr.encode()})
    report = roast(root, keys={"meta", "pheno"})
    assert not report.errors
    rows = pristine(root, "phenotype/UDS_b4_cdr.tsv")
    assert len(rows) == 1 + 4
    assert rows[-1] == ["sub-0002", "ses-d0400", "400", "n/a", "n/a",
                        "n/a", "n/a"]
    assert "UDS_b4_cdr.tsv: 1 row(s) skipped" in caplog.text


def test_phenotype_cells_need_no_quoting(root):
    """A `"` or a tab in a cell (the tables are read again to be
    trimmed) is replaced"""
    make_tar(root / "sourcedata" / "OASIS3_data_files" / "UDSb4.tar.gz",
             {"b4/cdr.csv": (CDR + 'OAS30001,OAS30001_UDSb4_d0500,500,'
                             '67.0,1,"say ""no""\there",1\n').encode()})
    roast(root, keys={"pheno", "T1w"})
    cdr = read_tsv(raw_file(root, "phenotype/UDS_b4_cdr.tsv"))
    assert cdr[-1][5] == "say 'no' here"


def test_phenotype_cells_with_line_breaks(root):
    """A line break in a cell is a space (a TSV table cannot hold it)"""
    make_tar(root / "sourcedata" / "OASIS3_data_files" / "UDSb4.tar.gz",
             {"b4/cdr.csv": (CDR + 'OAS30001,OAS30001_UDSb4_d0500,500,'
                             '67.0,1,"two\r\nlines",1\n').encode()})
    report = roast(root, keys={"pheno", "T1w"})
    assert not report.errors
    cdr = pristine(root, "phenotype/UDS_b4_cdr.tsv")
    assert cdr[-1][3:6] == ["67", "1", "two lines"]


@pytest.mark.parametrize("name", ["demo", "UDSb4"])
def test_corrupt_data_file(root, name):
    """A data file that cannot be read is an error of its tables only
    (the roast goes on)"""
    path = root / "sourcedata" / "OASIS3_data_files" / f"{name}.tar.gz"
    path.write_bytes(b"not a tarball")
    report = roast(root, keys={"meta", "T1w"})
    errors = sorted(name for _, name, _ in report.errors)
    if name == "demo":
        assert errors == ["participants.tsv", "sessions.tsv"]
    else:
        assert errors == ["UDS_b4_cdr.tsv", "sessions.tsv"]
    assert f"{T1}.nii.gz" in raw_tree(root)


def test_keys_all(root):
    """`--keys all` (accepted by the command line) selects every key"""
    assert roaster.KEYS.expand(["all"]) == roaster.KEYS.leaves
    add_freesurfer(root)
    report = roast(root, keys={"all"})
    assert not report.errors, report.errors
    every = raw_tree(root)
    assert f"{T1}.nii.gz" in every and PHENO <= every
    bx("oasis3", "roast", "--out", root, "--keys", "all")


def test_meta_only(root):
    # "meta" implies "pheno"
    roast(root, keys={"meta"})
    assert raw_tree(root) == META | PHENO


def test_json_no(root):
    roast(root, keys={"T1w"}, json="no")
    # sessions.tsv is only updated when sidecars are written
    assert raw_tree(root) == {f"{T1}.nii.gz"}


def test_subject_filters(root):
    roast(root, keys={"meta", "pheno", "T1w", "T2w"}, exclude_subs={2})
    assert raw_tree(root) - META - PHENO == {
        f"{T1}.json", f"{T1}.nii.gz", "sub-0001/sub-0001_sessions.tsv"}
    roast(root, keys={"T2w"}, subs={2}, json="no")
    assert f"{T2}.nii.gz" in raw_tree(root)


def test_key_selects_modality(root):
    roast(root, keys={"T2w"}, json="no")
    assert raw_tree(root) == {f"{T2}.nii.gz"}


def test_sessions_file_name(root):
    roast(root, keys={"meta", "pheno", "T1w"})
    assert "sub-0001/sub-0001_sessions.tsv" in raw_tree(root)


def test_exclude_two_keys(root):
    roast(root, keys={"T1w"}, exclude_keys={"fs", "pup"}, json="no")
    assert raw_tree(root) == {f"{T1}.nii.gz"}


def mgz_bytes(value=1) -> bytes:
    import gzip

    import numpy as np
    img = nib.MGHImage(np.full((2, 2, 2), value, np.float32), np.eye(4))
    return gzip.compress(img.to_bytes())


def add_freesurfer(root):
    lead = "a/b/c/d/e/f"        # members start with 6 folders
    make_tar(root / "sourcedata" / "OAS30001_MR_d0129" /
             "OAS30001_Freesurfer53_d0129.tar.gz", {
                 f"{lead}/mri/aseg.mgz": mgz_bytes(),
                 f"{lead}/stats/aseg.stats": b"# not bidsifiable",
             })


def test_freesurfer(root):
    """FreeSurfer trees: derivative sub-layout with its own sourcedata"""
    add_freesurfer(root)
    roast(root, keys={"fs"})
    drv = layout_of(root).derivative(roaster.FS_NAME)
    unpacked = drv.source / "sub-0001" / "ses-d0129"
    assert (unpacked / "mri" / "aseg.mgz").exists()
    assert not (unpacked / "stats").exists()        # only with fs-all
    anat = drv.root / "sub-0001/ses-d0129/anat"
    seg = "sub-0001_ses-d0129_atlas-Aseg_dseg"
    assert {p.name for p in anat.iterdir()} == {
        f"{seg}.nii.gz", f"{seg}.json"}
    assert (drv.root / "atlas-Aseg_dseg.tsv").exists()
    assert raw_tree(root) == set()                  # no raw key
    # a BIDS derivative dataset (its top-level dseg.tsv are inherited)
    desc = json.loads(drv.description.read_text())
    assert desc["DatasetType"] == "derivative"
    assert desc["GeneratedBy"][0] == {
        "Name": "FreeSurfer", "Version": "5.3",
        "Description": "FreeSurfer outputs distributed with the dataset."}
    assert desc["SourceDatasets"] == [{"URL": drv.source_url}]

    roast(root, keys={"fs-all"}, json="only")
    # unpacked files are inputs: --sidecars only does not drop them
    assert (unpacked / "stats" / "aseg.stats").exists()


def test_sessions_from_sourcedata_trimmed(root):
    # a PET session that is not roasted: in the pristine table only
    (root / "sourcedata" / "OAS30001_PIB_d0339").mkdir()
    report = roast(root, keys={"meta", "T1w"}, jobs=2, display=False)
    assert not report.errors
    table = "sub-0001/sub-0001_sessions.tsv"
    assert read_tsv(raw_file(root, table)) == [
        ["session_id", "pathology", "age"], ["ses-d0129", "1", "65.4532"]]
    original = layout_of(root).original(raw_file(root, table))
    assert read_tsv(original)[1:] == [
        ["ses-d0129", "1", "65.4532"], ["ses-d0339", "2", "66.0281"]]
    # no raw data for sub-0002: no sessions table, not in participants
    assert not raw_file(root, "sub-0002").exists()
    assert [r[0] for r in read_tsv(raw_file(root, "participants.tsv"))] \
        == ["participant_id", "sub-0001"]



def test_freesurfer_sidecars_only(root):
    """`--sidecars only --keys fs`: sidecars from the member names, nothing
    unpacked (fs-all unpacks the whole tree, see `test_freesurfer`)"""
    add_freesurfer(root)
    report = roast(root, keys={"fs"}, json="only", display=False)
    assert not report.errors
    drv = layout_of(root).derivative(roaster.FS_NAME)
    assert not drv.source.exists()
    anat = drv.root / "sub-0001/ses-d0129/anat"
    assert {p.name for p in anat.iterdir()} == {
        "sub-0001_ses-d0129_atlas-Aseg_dseg.json"}


def test_freesurfer_not_wanted_is_dropped(root, monkeypatch):
    """A roaster that forgot `ctx.wants("fs")` writes nothing there: the
    runner drops outputs in the folder of an unwanted category"""
    add_freesurfer(root)
    monkeypatch.setattr(roaster.RoastContext, "wants",
                        lambda self, key: True if key in ("fs", "fs-all")
                        else self.selection.wants(key))
    report = roast(root, keys={"T1w"}, json="no", display=False)
    assert not report.errors
    assert not layout_of(root).derivative(roaster.FS_NAME).root.exists()
    assert raw_tree(root) == {f"{T1}.nii.gz"}


@pytest.mark.parametrize("keys", [{"meta"}, {"fs"}, {"pheno"}, {"T1w"}])
def test_category_keys_open_no_image_tarball(root, monkeypatch, keys):
    from brainspresso.actions import archive
    opened = []
    real = archive.TarArchive

    def opener(path, *a, **k):
        opened.append(path)
        return real(path, *a, **k)

    monkeypatch.setattr(archive, "TarArchive", opener)
    monkeypatch.setattr(archive.ExtractMembers.__init__, "__kwdefaults__",
                        {**archive.ExtractMembers.__init__.__kwdefaults__,
                         "opener": opener})
    report = roast(root, keys=keys, display=False)
    assert not report.errors
    if keys == {"T1w"}:         # (the check sees the tarballs it opens)
        assert [p.name for p in opened] == ["anat1.tar.gz"] * 2
    else:
        assert opened == []


def test_scope_of_subject_shards(root, monkeypatch):
    """Toggling `meta`, fewer subjects: no subject is roasted again; a
    key that its shard had not written: it is (`roast.scope`)"""
    calls = []
    real = roaster.roast_subject

    def spy(ctx, sub):
        calls.append(sub)
        return real(ctx, sub)

    monkeypatch.setattr(roaster, "roast_subject", spy)
    assert not roast(root, keys={"T1w", "T2w"}, display=False).errors
    assert sorted(calls) == ["0001", "0002"]
    scope = scoped_records(root)["roast/subjects/sub-0001"]["scopes"][0]
    assert scope["fixed"]["declared"]["subs"] == ["0001"]
    assert "meta" not in scope["fixed"]["declared"]["keys"]
    del calls[:]
    for options in ({"keys": {"T1w", "T2w", "meta", "pheno"}},
                    {"keys": {"T1w"}}, {"keys": {"T2w"}, "subs": [2]}):
        assert not roast(root, display=False, **options).errors
    assert calls == []
    roast(root, keys={"T1w", "FLAIR"}, subs=[1], display=False)
    assert calls == ["0001"]


def test_keys_of_subject_shards(root):
    """Adding a key that a subject shard reads re-runs it"""
    check_keys_rerun(root, lambda **kw: roast(root, display=False, **kw),
                     {"T1w", "T2w"}, {"roast/subjects/": ("T1w", "T2w")})


def test_key_classification():
    """`KeyAliases.key_of` and the review cases of 2b-ii"""
    keys = roaster.KEYS
    b = "sub-0001/ses-d0001/"
    tse = b + "anat/sub-0001_ses-d0001_acq-TSE_T2w.nii.gz"
    t2w = b + "anat/sub-0001_ses-d0001_T2w.nii.gz"
    sbref = b + "func/sub-0001_ses-d0001_task-rest_sbref.nii.gz"
    m0 = b + "perf/sub-0001_ses-d0001_m0scan.nii.gz"
    events = b + "func/sub-0001_ses-d0001_task-rest_events.tsv"
    aslcontext = b + "perf/sub-0001_ses-d0001_aslcontext.tsv"
    assert keys.key_of(tse) == "TSE" and keys.key_of(t2w) == "T2w"
    assert "fieldmap" not in keys.leaves

    sel = keys.selection(exclude_keys=["TSE"])
    assert not sel.keeps(tse) and sel.keeps(t2w)
    sel = keys.selection(exclude_keys=["CT"])
    assert all(map(sel.keeps, (sbref, m0, events, aslcontext)))
    sel = keys.selection(["meta"])
    assert not any(map(sel.keeps, (tse, t2w, sbref, m0, events,
                                   aslcontext)))
    for category in ("meta", "fs"):
        sel = keys.selection([category])
        for claim in ("sub-0001/ses-d0001", "sub-0001/ses-d0001/anat",
                      "sub-0001/ses-d0001/pet"):
            assert sel.keeps(claim, partial=True) is False
    sel = keys.selection(["asl"])
    assert sel.keeps(m0) and sel.keeps(aslcontext) and not sel.keeps(events)
