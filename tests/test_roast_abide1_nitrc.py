"""ABIDE-I roast of the NITRC tarballs and of the bucket copy (engine)."""
import json
import logging
import tarfile

import nibabel as nib
import pytest
from cli_utils import bx
from roast_utils import (
    LAYOUTS,
    SCOPE_SCENARIOS,
    check_keys_rerun,
    check_scope,
    init_layout,
    layout_of,
    make_tar,
    nii_bytes,
    open_spy,
    raw_file,
    raw_tree,
    read_tsv,
)

from brainspresso.actions import CopyJSON
from brainspresso.bids.layout import Layout
from brainspresso.datasets.indi.abide.i import roaster, sites, tables
from brainspresso.datasets.indi.abide.i.dataset import ABIDE1
from brainspresso.datasets.indi.abide.i.roaster import roast
from brainspresso.datasets.indi.abide.i.sites import TEMPLATES
from brainspresso.roast import sidecars

PHENOS = tables.PHENOTYPES
COLS = list(dict.fromkeys(
    c for t in [tables.PARTICIPANTS, *PHENOS.values()]
    for c in t.columns.values()))


def write_csv(path, rows, cols=COLS):
    path.write_text("\n".join(
        ",".join(r.get(c, "") for c in cols) for r in [
            {c: c for c in cols}, *rows]) + "\n")


def make_source(root):
    """Write the harvested NITRC files of ABIDE-I in `<root>/sourcedata/`."""
    src = root / "sourcedata"
    src.mkdir(parents=True, exist_ok=True)
    make_tar(src / "Caltech.tgz", {
        "Caltech/0051456/session_1/anat_1/mprage.nii.gz": nii_bytes(),
        "Caltech/0051456/session_1/rest_1/rest.nii.gz":
            nii_bytes((2, 2, 2, 3)),
        "Caltech/0051457/session_1/anat_1/mprage.nii.gz": nii_bytes(),
        "Caltech/0051457/session_1/anat_1/notes.txt": b"x",
    }, dirs=["Caltech", "Caltech/0051456"])
    make_tar(src / "MaxMun_a.tgz", {
        "MaxMun_a/0051330/session_1/anat_1/mprage.nii.gz": nii_bytes(),
    })
    write_csv(src / "phenotypic_CALTECH.csv", [
        {"SUB_ID": "51456", "SITE_ID": "CALTECH", "SEX": "1",
         "AGE_AT_SCAN": "55.4", "DX_GROUP": "1", "FIQ": "-9999",
         "FIQ_TEST_TYPE": "WAIS_III", "ADI_R_SOCIAL_TOTAL_A": "20",
         "ADI_R_RSRCH_RELIABLE": "1"},
        {"SUB_ID": "51457", "SITE_ID": "CALTECH", "SEX": "2",
         "AGE_AT_SCAN": "22.9", "DX_GROUP": "2"},
    ])
    write_csv(src / "phenotypic_MAX_MUN.csv", [
        {"SUB_ID": "51330", "SITE_ID": "MAX_MUN", "SEX": "1"},
    ])
    return root


@pytest.fixture(params=LAYOUTS)
def root(tmp_path, request):
    root = tmp_path / "ABIDE-1"
    init_layout(root, request.param)
    return make_source(root)


RAW = {
    "sub-51456/anat/sub-51456_T1w.nii.gz",
    "sub-51456/anat/sub-51456_T1w.json",
    "sub-51456/func/sub-51456_task-rest_bold.nii.gz",
    "sub-51456/func/sub-51456_task-rest_bold.json",
    "sub-51457/anat/sub-51457_T1w.nii.gz",
    "sub-51457/anat/sub-51457_T1w.json",
    "sub-51330/anat/sub-51330_T1w.nii.gz",
    "sub-51330/anat/sub-51330_T1w.json",
}
META = {"README", "dataset_description.json",
        "participants.json", "participants.tsv"}
PHENO = {f"phenotype/{p}.{e}" for p in PHENOS for e in ("tsv", "json")}


def original(root, name="participants.tsv"):
    """Pristine copy of a table (all subjects, before trimming)."""
    return layout_of(root).original(raw_file(root, name))


def tpl(path):
    return json.loads((TEMPLATES / path).read_text())


def sidecar(root, path):
    return json.loads(raw_file(root, path).read_text())


def test_roast_raw(root):
    report = roast(root, keys={"T1w", "rest"}, display=False)
    assert not report.errors
    assert raw_tree(root) == RAW
    img = nib.load(raw_file(root,
                            "sub-51456/func/sub-51456_task-rest_bold.nii.gz"))
    assert img.shape == (2, 2, 2, 3)
    assert sidecar(root, "sub-51330/anat/sub-51330_T1w.json") \
        == tpl("MaxMun_a/T1w.json")


def test_roast_sample_template(root):
    """A sample-suffixed site uses its own sidecar; others fall back."""
    src = root / "sourcedata"
    make_tar(src / "MaxMun_b.tgz", {
        "MaxMun_b/0051340/session_1/rest_1/rest.nii.gz":
            nii_bytes((2, 2, 2, 3)),
    })
    make_tar(src / "CMU_a.tgz", {
        "CMU_a/0050642/session_1/anat_1/mprage.nii.gz": nii_bytes(),
    })
    roast(root, keys={"T1w", "rest"}, display=False)
    assert tpl("MaxMun_b/bold.json") != tpl("MaxMun_a/bold.json")
    assert sidecar(root, "sub-51340/func/sub-51340_task-rest_bold.json") \
        == {**tpl("MaxMun_b/bold.json"), "TaskName": "rest"}
    # CMU_a has no anatomical template: fall back to the site template
    assert not (TEMPLATES / "CMU_a/T1w.json").exists()
    assert sidecar(root, "sub-50642/anat/sub-50642_T1w.json") \
        == tpl("CMU/T1w.json")


def test_site_template_under_skip_and_update(root, caplog):
    """A per-site template that changes: left alone under `skip`, followed
    under `update` (an edit kept, with a warning)"""
    roast(root, keys={"T1w"}, display=False)
    paths = [raw_file(root, f"sub-{s}/anat/sub-{s}_T1w.json")
             for s in (51456, 51457, 51330)]
    old = [p.read_bytes() for p in paths]
    local = root / "sourcedata" / "Caltech" / "T1w.json"
    local.parent.mkdir()
    local.write_text(json.dumps({**tpl("Caltech/T1w.json"), "Local": 1}))
    caplog.clear()
    roast(root, keys={"T1w"}, display=False)
    assert [p.read_bytes() for p in paths] == old
    assert "WARNING" not in caplog.text
    paths[1].write_text('{"Mine": true}')
    roast(root, keys={"T1w"}, ifexists="update", display=False)
    assert sidecar(root, "sub-51456/anat/sub-51456_T1w.json") == \
        {**tpl("Caltech/T1w.json"), "Local": 1}
    assert json.loads(paths[1].read_text()) == {"Mine": True}
    assert paths[2].read_bytes() == old[2]       # (another site)
    assert "was edited" in caplog.text
    roast(root, keys={"T1w"}, ifexists="different", display=False)
    assert sidecar(root, "sub-51457/anat/sub-51457_T1w.json") == \
        {**tpl("Caltech/T1w.json"), "Local": 1}


def test_roast_filters(root):
    roast(root, keys={"T1w"}, exclude_subs={51457}, json="no",
          display=False)
    assert raw_tree(root) == {"sub-51456/anat/sub-51456_T1w.nii.gz",
                              "sub-51330/anat/sub-51330_T1w.nii.gz"}
    roast(root, keys={"rest"}, subs={51456}, json="only", display=False)
    assert "sub-51456/func/sub-51456_task-rest_bold.json" in raw_tree(root)
    assert "sub-51456/func/sub-51456_task-rest_bold.nii.gz" \
        not in raw_tree(root)


def test_roast_all(root):
    report = roast(root, display=False)
    assert not report.errors
    assert raw_tree(root) == RAW | META | PHENO


def test_meta(root):
    roast(root, keys={"meta"}, display=False)
    assert raw_tree(root) == META | PHENO
    rows = read_tsv(original(root))
    assert rows[0] == list(tables.PARTICIPANTS.columns)
    row = dict(zip(rows[0], rows[1]))
    assert {k: row[k] for k in ("site", "sample", "sex", "age", "group",
                                "fiq", "fiq_type")} == {
        "site": "Caltech", "sample": "Caltech", "sex": "M", "age": "55.4",
        "group": "A", "fiq": "n/a", "fiq_type": "WAIS-III"}
    # trimmed to the subjects on disk (none yet)
    assert read_tsv(raw_file(root, "participants.tsv")) == [rows[0]]

    adir = read_tsv(raw_file(root, "phenotype/adir.tsv"))
    assert adir[0] == list(PHENOS["adir"].columns)
    first = dict(zip(adir[0], adir[1]))
    assert first["participant_id"] == "sub-51456"
    assert (first["social"], first["reliable"]) == ("20", "Y")
    assert json.loads(raw_file(root, "phenotype/adir.json").read_text()) \
        == tpl("phenotype/adir.json")


def test_pheno_only(root):
    """`--keys pheno`: phenotypes, without the dataset files"""
    roast(root, keys={"pheno"}, display=False)
    assert raw_tree(root) == PHENO


def test_participants_sub_prefix(root):
    roast(root, keys={"meta"}, display=False)
    ids = [r[0] for r in read_tsv(original(root))[1:]]
    assert ids == ["sub-51456", "sub-51457", "sub-51330"]


def test_participants_maxmun(root):
    """`phenotypic_MAX_MUN.csv` is read (the legacy glob missed it)"""
    roast(root, display=False)
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert [r[0] for r in rows[1:]] == ["sub-51456", "sub-51457",
                                        "sub-51330"]
    row = dict(zip(rows[0], rows[3]))
    assert (row["site"], row["sample"]) == ("MaxMun", "MaxMun")


def test_participants_trimmed(root):
    roast(root, keys={"meta", "T1w"}, subs=[51330], display=False)
    rows = read_tsv(raw_file(root, "participants.tsv"))
    assert [r[0] for r in rows[1:]] == ["sub-51330"]
    assert len(read_tsv(original(root))) == 4


def test_participants_missing_values(root):
    roast(root, keys={"meta"}, display=False)
    rows = read_tsv(original(root))
    assert "" not in {cell for row in rows for cell in row}
    row = dict(zip(rows[0], rows[3]))
    assert row["age"] == "n/a" and row["fiq"] == "n/a"


def test_participants_samples_and_sites(tmp_path):
    root = tmp_path / "ABIDE-1"
    src = root / "sourcedata"
    src.mkdir(parents=True)
    write_csv(src / "phenotypic_UCLA_1.csv", [
        {"SUB_ID": "51201", "SITE_ID": "UCLA_1"}])
    write_csv(src / "phenotypic_OLIN.csv", [
        {"SUB_ID": "0050102", "SITE_ID": "OLIN"}])
    roast(root, keys={"meta"}, display=False)
    rows = read_tsv(original(root))
    got = {r[0]: (r[1], r[2]) for r in rows[1:]}
    assert got == {"sub-50102": ("Olin", "Olin"),
                   "sub-51201": ("UCLA", "UCLA1")}


def test_participants_json_samples():
    """Every value of `sample` (the site, or the site and a sample) is a
    level of `participants.json`; age in years (BIDS unit "year")"""
    meta = json.loads((TEMPLATES / "participants.json").read_text())
    values = {tables._sample(site) for site in sites.SITES}
    values |= {tables._sample(f"{site}_{sample}")
               for site, samples in sites.SAMPLES.items()
               for sample in samples}
    assert values == set(meta["sample"]["Levels"])
    assert {"Caltech", "CMUa", "MaxMund", "UCLA1"} <= values
    assert meta["age"]["Units"] == "year"


def test_bold_sidecar_task_name(tmp_path):
    """A bold sidecar gets the `TaskName` of its name, if its template
    has none (a copy of the template otherwise)"""
    template = tmp_path / "bold.json"
    stem = tmp_path / "sub-1_task-rest_bold"
    action = sidecars.bold_sidecar(template, {"RepetitionTime": 2}, stem,
                                tmp_path / "x.nii.gz", None, "x.nii.gz")
    assert action.extra == {"TaskName": "rest"}
    action = sidecars.bold_sidecar(template, {"RepetitionTime": 2,
                                           "TaskName": "mine"}, stem,
                                tmp_path / "x.nii.gz", None, "x.nii.gz")
    assert isinstance(action, CopyJSON)
    assert sidecars.bold_fields("sub-1_task-rest_acq-A_bold") == \
        {"TaskName": "rest"}
    assert sidecars.bold_fields("sub-1_bold") == {}


def test_participants_bad_rows(root, caplog):
    """Blank rows are skipped, rows without a subject (or listed twice)
    are skipped with a warning; files may order columns differently"""
    csv = root / "sourcedata" / "phenotypic_CALTECH.csv"
    csv.write_text(csv.read_text() + "\n" + "," * (len(COLS) - 1) + "\n"
                   + ",".join("CALTECH" if c == "SITE_ID" else ""
                              for c in COLS) + "\n"
                   + ",".join("51456" if c == "SUB_ID" else ""
                              for c in COLS) + "\n")
    write_csv(root / "sourcedata" / "phenotypic_SBL.csv",
              [{"SUB_ID": "51556", "SEX": "2"}],
              cols=["SEX", "SUB_ID", "SITE_ID"])
    with caplog.at_level(logging.WARNING):
        report = roast(root, keys={"meta"}, display=False)
    assert not report.errors
    rows = read_tsv(original(root))
    assert [r[0] for r in rows[1:]] == ["sub-51456", "sub-51457",
                                        "sub-51330", "sub-51556"]
    assert dict(zip(rows[0], rows[4]))["sex"] == "F"
    assert dict(zip(rows[0], rows[4]))["age"] == "n/a"
    assert caplog.text.count("row skipped") == 2


def test_missing_phenotypes(root):
    for path in (root / "sourcedata").glob("phenotypic_*.csv"):
        path.unlink()
    report = roast(root, keys={"meta"}, display=False)
    assert raw_tree(root) == (META - {"participants.tsv"}) | {
        p for p in PHENO if p.endswith(".json")}
    assert len(report.errors) == 2           # participants, phenotypes


def test_missing_archives(tmp_path):
    root = tmp_path / "ABIDE-1"
    (root / "sourcedata").mkdir(parents=True)
    report = roast(root, keys={"T1w"}, display=False)
    assert raw_tree(root) == set()
    assert len(report.errors) == 1
    assert "tarball" in report.errors[0][2]


def test_corrupt_archive_fails_its_shard_only(root):
    src = root / "sourcedata"
    (src / "Pitt.tgz").write_bytes(b"not a tarball")
    data = (src / "Caltech.tgz").read_bytes()
    (src / "Caltech.tgz").write_bytes(data[:len(data) // 2])
    report = roast(root, keys={"T1w", "rest"}, display=False)
    assert {"sub-51330/anat/sub-51330_T1w.nii.gz",
            "sub-51330/anat/sub-51330_T1w.json"} <= raw_tree(root)
    assert len(report.errors) == 2, report.errors
    assert any("Pitt.tgz" in str(e) for e in report.errors)
    assert any("Caltech.tgz" in str(e) for e in report.errors)


def test_hires_anatomical(root):
    """UCLA `anat_2` is the high-resolution anatomical: a spin-echo EPI,
    `acq-hires_T2w` (owner decision), key `T2w`"""
    make_tar(root / "sourcedata" / "UCLA_1.tgz", {
        "UCLA_1/0051201/session_1/anat_1/mprage.nii.gz": nii_bytes(),
        "UCLA_1/0051201/session_1/anat_2/mprage.nii.gz":
            nii_bytes(value=2),
        "UCLA_1/0051201/session_1/rest_1/rest.nii.gz":
            nii_bytes((2, 2, 2, 3)),
    })
    report = roast(root, keys={"T2w"}, subs=[51201], display=False)
    assert not report.errors
    assert raw_tree(root) == {
        "sub-51201/anat/sub-51201_acq-hires_T2w.nii.gz",
        "sub-51201/anat/sub-51201_acq-hires_T2w.json"}
    report = roast(root, keys={"anat"}, subs=[51201], display=False)
    assert not report.errors
    assert raw_tree(root) == {
        "sub-51201/anat/sub-51201_T1w.nii.gz",
        "sub-51201/anat/sub-51201_T1w.json",
        "sub-51201/anat/sub-51201_acq-hires_T2w.nii.gz",
        "sub-51201/anat/sub-51201_acq-hires_T2w.json",
    }
    assert sidecar(root, "sub-51201/anat/sub-51201_acq-hires_T2w.json") \
        == tpl("UCLA_1/acq-hires_T2w.json")
    img = nib.load(raw_file(root,
                            "sub-51201/anat/sub-51201_acq-hires_T2w.nii.gz"))
    assert img.get_fdata().max() == 2


def test_unknown_scans_skipped(root, caplog):
    """Scans without a BIDS name (another session, a second rest) are
    skipped with a warning, never written over the first one"""
    make_tar(root / "sourcedata" / "Pitt.tgz", {
        "Pitt/0050003/session_1/rest_1/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
        "Pitt/0050003/session_1/rest_2/rest.nii.gz": nii_bytes((2, 2, 2, 4)),
        "Pitt/0050003/session_2/anat_1/mprage.nii.gz": nii_bytes(),
        "Pitt/0050003/session_1/anat_2/mprage.nii.gz": nii_bytes(),
    })
    with caplog.at_level(logging.WARNING):
        roast(root, keys={"T1w", "rest"}, subs=[50003], display=False)
    assert raw_tree(root) == {
        "sub-50003/func/sub-50003_task-rest_bold.nii.gz",
        "sub-50003/func/sub-50003_task-rest_bold.json"}
    assert caplog.text.count("skipped") == 3


def test_unpadded_ids(root):
    """Tarball ids of 5 or 7 digits are the same subjects"""
    make_tar(root / "sourcedata" / "SDSU.tgz", {
        "SDSU/50182/session_1/anat_1/mprage.nii.gz": nii_bytes()})
    roast(root, keys={"T1w"}, subs=["0050182"], json="no", display=False)
    assert raw_tree(root) == {"sub-50182/anat/sub-50182_T1w.nii.gz"}


def test_missing_sidecar_template(root, monkeypatch):
    """A missing sidecar template is an error only if sidecars are
    written"""
    monkeypatch.setattr(roaster.RoastContext, "resource",
                        lambda self, name: None)
    report = roast(root, keys={"T1w"}, json="no", display=False)
    assert not report.errors
    report = roast(root, keys={"T1w"}, ifexists="overwrite", display=False)
    assert len(report.errors) == 3


def test_tarball_read_once(root, monkeypatch):
    """Members are read lazily (no listing first: one decompression)"""
    def listing(self):
        raise AssertionError("the tarball is listed")

    monkeypatch.setattr(roaster.TarArchive, "getmembers", listing)
    monkeypatch.setattr(roaster.TarArchive, "getnames", listing)
    report = roast(root, display=False)
    assert not report.errors
    assert raw_tree(root) == RAW | META | PHENO


def test_one_shard_per_tarball(root, monkeypatch):
    opened = []
    real = roaster.TarArchive

    def opener(path):
        opened.append(path.name)
        return real(path)

    monkeypatch.setattr(roaster, "TarArchive", opener)
    roast(root, keys={"T1w", "rest"}, display=False)
    assert sorted(opened) == ["Caltech.tgz", "MaxMun_a.tgz"]
    # unchanged inputs and options: skipped by signature
    roast(root, keys={"T1w", "rest"}, display=False)
    assert len(opened) == 2
    # a new template of a site re-runs the shards of that site only
    (root / "sourcedata" / "Caltech").mkdir()
    (root / "sourcedata" / "Caltech" / "T1w.json").write_text(
        '{"EchoTime": 1}')
    roast(root, keys={"T1w", "rest"}, display=False)
    assert sorted(opened) == ["Caltech.tgz", "Caltech.tgz", "MaxMun_a.tgz"]
    roast(root, keys={"T1w", "rest"}, ifexists="overwrite", display=False)
    assert sidecar(root, "sub-51456/anat/sub-51456_T1w.json") == \
        {"EchoTime": 1}


@pytest.mark.parametrize("scenario", SCOPE_SCENARIOS)
def test_scope_of_tarballs(root, monkeypatch, scenario):
    """`--subs` narrowing and `meta` skip the tarballs, a new subject
    re-reads the tarball of its site only (`roast.scope`)"""
    opened = open_spy(monkeypatch, roaster)
    check_scope(
        scenario, root, lambda **kw: roast(root, display=False, **kw),
        opened, a=51456, b=51330, label_a="51456", label_b="51330",
        archives={
            "Caltech.tgz": ({"51456", "51457"}, "roast/raw/Caltech.tgz"),
            "MaxMun_a.tgz": ({"51330"}, "roast/raw/MaxMun_a.tgz")},
        keys={"meta", "T1w", "T2w", "rest"}, key="rest", absent=50000)


def test_keys_of_tarballs(root):
    """Adding a key that a tarball shard reads re-runs it"""
    check_keys_rerun(root, lambda **kw: roast(root, display=False, **kw),
                     {"meta", "T1w", "T2w", "rest"},
                     {"roast/raw/": roaster.RAW_KEYS})


def test_subjects_pruned_before_extraction(root, monkeypatch):
    extracted = []
    real = roaster.ExtractMember

    def spy(archive, member, dst, **kwargs):
        extracted.append(member.name)
        return real(archive, member, dst, **kwargs)

    monkeypatch.setattr(roaster, "ExtractMember", spy)
    roast(root, keys={"T1w", "rest"}, subs=[51457], display=False)
    assert extracted == ["Caltech/0051457/session_1/anat_1/mprage.nii.gz"]


def test_jobs(root):
    report = roast(root, jobs=2, display=False)
    assert not report.errors
    assert raw_tree(root) == RAW | META | PHENO


def test_bucket_copy(tmp_path):
    """`RawData/<Site>/` (s3) reads as the tarballs, one shard per site"""
    root = tmp_path / "tgz"
    make_source(root)
    roast(root, keys={"T1w", "rest"}, display=False)
    rawdata = tmp_path / "s3" / "sourcedata" / "RawData"
    for tarball in sorted((root / "sourcedata").glob("*.tgz")):
        with tarfile.open(tarball) as tar:
            for member in tar:
                if member.isfile():
                    path = rawdata / member.name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(tar.extractfile(member).read())
    report = roast(tmp_path / "s3", keys={"T1w", "rest"}, display=False)
    assert not report.errors
    assert raw_tree(tmp_path / "s3") == RAW
    for path in RAW:
        assert raw_file(tmp_path / "s3", path).read_bytes() == \
            raw_file(root, path).read_bytes()


# ----------------------------------------------------------------------
#   Command line
# ----------------------------------------------------------------------

@pytest.mark.parametrize("layout", LAYOUTS)
def test_cli_layout(tmp_path, layout):
    root = make_source(tmp_path / "ABIDE-1")
    bx("abide1", "roast", "--out", root, "--layout", layout)
    assert Layout.detect(root).kind == layout
    assert raw_tree(root) == RAW | META | PHENO


@pytest.mark.parametrize("source", ["s3", "nitrc"])
def test_cli_keys_subjects_sidecars(tmp_path, source):
    root = make_source(tmp_path / "ABIDE-1")
    bx("abide1", "roast", "--out", root, "--source", source, "--keys",
       "T1w", "--subs", "51456", "0051330", "--sidecars", "no")
    assert raw_tree(root) == {"sub-51456/anat/sub-51456_T1w.nii.gz",
                              "sub-51330/anat/sub-51330_T1w.nii.gz"}
    bx("abide1", "roast", "--out", root, "--exclude-keys", "rest", "meta",
       "--exclude-subs", "sub-51456", "--sidecars", "only")
    assert "sub-51457/anat/sub-51457_T1w.json" in raw_tree(root)
    assert "sub-51457/anat/sub-51457_T1w.nii.gz" not in raw_tree(root)
    assert "participants.tsv" not in raw_tree(root)


def test_dataset_declares_the_roasters():
    abide = ABIDE1()
    sources = abide.sources()
    assert sources["s3"].roaster is roaster.roast_nitrc
    assert sources["nitrc"].roaster is roaster.roast_nitrc
    assert sources["xnat"].roaster is roaster.roast_xnat
    assert "source" in abide.groups("roast")
    assert abide.labels(["51456", "sub-0051457", "51456:51459"]) == {
        "51456", "51457", "51458"}


def test_shared_table_changes(tmp_path, caplog):
    """Changes from the tables shared with ABIDE-II (#37): `-9999.00` is
    n/a, a file that is not UTF-8 is read as Windows-1252 (logged)"""
    root = tmp_path / "ABIDE-1"
    src = root / "sourcedata"
    src.mkdir(parents=True)
    (src / "phenotypic_SBL.csv").write_bytes(
        "SUB_ID,SITE_ID,FIQ,MEDICATION_NAME\n51556,SBL,-9999.00,Fran\xe7ais\n"
        .encode("cp1252"))
    with caplog.at_level(logging.INFO):
        report = roast(root, keys={"meta"}, display=False)
    assert not report.errors
    rows = [line.split("\t") for line in
            original(root).read_text(encoding="utf-8").splitlines()]
    row = dict(zip(rows[0], rows[1]))
    assert (row["fiq"], row["med_name"]) == ("n/a", "Fran\xe7ais")
    assert "not UTF-8, read as cp1252" in caplog.text
