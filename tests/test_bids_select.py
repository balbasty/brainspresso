from pathlib import Path

import pytest

from brainspresso.bids.select import BidsName, Selection
from brainspresso.bids.tables import query_participants, trim_tables

# Layout modelled on ds000224 (top-level sidecars, sessions, scans)
LISTING = [
    ".bidsignore",
    ".datalad/config",
    ".gitattributes",
    "CHANGES",
    "README",
    "dataset_description.json",
    "participants.json",
    "participants.tsv",
    "T1w.json",
    "T2w.json",
    "magnitude1.json",
    "task-rest_bold.json",
    "task-motor_bold.json",
    "bold_group.html",
    "phenotype/NEO_FFI.tsv",
    "derivatives/surfaces/sub-01/x.gii",
    "sourcedata/raw.zip",
    "sub-01/sub-01_sessions.tsv",
    "sub-01/ses-1/sub-01_ses-1_scans.tsv",
    "sub-01/ses-1/anat/sub-01_ses-1_T1w.nii.gz",
    "sub-01/ses-1/anat/sub-01_ses-1_T2w.nii.gz",
    "sub-01/ses-1/anat/sub-01_ses-1_T2w.json",
    "sub-01/ses-1/fmap/sub-01_ses-1_magnitude1.nii.gz",
    "sub-01/ses-2/sub-01_ses-2_scans.tsv",
    "sub-01/ses-2/func/sub-01_ses-2_task-rest_bold.nii.gz",
    "sub-01/ses-2/func/sub-01_ses-2_task-motor_bold.nii.gz",
    "sub-01/ses-2/func/sub-01_ses-2_task-motor_events.tsv",
    "sub-02/sub-02_sessions.tsv",
    "sub-02/ses-1/anat/sub-02_ses-1_T1w.nii.gz",
    "sub-02/ses-1/dwi/sub-02_ses-1_dwi.nii.gz",
    "sub-02/ses-1/dwi/sub-02_ses-1_dwi.bval",
    "sub-02/ses-1/dwi/sub-02_ses-1_dwi.bvec",
]
ALWAYS = {
    ".bidsignore", "CHANGES", "README", "dataset_description.json",
    "participants.json", "participants.tsv", "bold_group.html",
    "phenotype/NEO_FFI.tsv",
}


def test_parse_name():
    name = BidsName.parse(
        "sub-01/ses-2/func/sub-01_ses-2_task-rest_bold.nii.gz"
    )
    assert name.entities == {"sub": "01", "ses": "2", "task": "rest"}
    assert name.suffix == "bold"
    assert name.extension == ".nii.gz"
    assert name.datatype == "func"
    assert not name.is_metadata
    top = BidsName.parse("T1w.json")
    assert top.entities == {} and top.suffix == "T1w" and top.is_metadata


def test_select_all_skips_hidden_and_optional_folders():
    kept = set(Selection().select(LISTING))
    assert ".datalad/config" not in kept
    assert ".gitattributes" not in kept
    assert not any(p.startswith(("derivatives/", "sourcedata/")) for p in kept)
    assert ALWAYS <= kept
    # every sidecar applies to some data file
    assert {"T1w.json", "T2w.json", "magnitude1.json",
            "task-rest_bold.json", "task-motor_bold.json"} <= kept


def test_select_optional_folders():
    kept = set(Selection(folders=["derivatives"]).select(LISTING))
    assert "derivatives/surfaces/sub-01/x.gii" in kept
    assert "sourcedata/raw.zip" not in kept


def test_select_suffix_with_inheritance():
    kept = set(Selection(suffix=["T1w"]).select(LISTING))
    assert kept == ALWAYS | {
        "T1w.json",
        "sub-01/sub-01_sessions.tsv",
        "sub-01/ses-1/sub-01_ses-1_scans.tsv",
        "sub-01/ses-1/anat/sub-01_ses-1_T1w.nii.gz",
        "sub-02/sub-02_sessions.tsv",
        "sub-02/ses-1/anat/sub-02_ses-1_T1w.nii.gz",
    }


def test_select_task_includes_matching_sidecars_and_events():
    kept = set(Selection(task=["motor"]).select(LISTING))
    assert "sub-01/ses-2/func/sub-01_ses-2_task-motor_bold.nii.gz" in kept
    assert "sub-01/ses-2/func/sub-01_ses-2_task-motor_events.tsv" in kept
    assert "task-motor_bold.json" in kept
    assert "task-rest_bold.json" not in kept
    assert "sub-01/ses-2/func/sub-01_ses-2_task-rest_bold.nii.gz" not in kept
    assert "T1w.json" not in kept
    assert "sub-01/ses-1/sub-01_ses-1_scans.tsv" not in kept


def test_select_subjects_sessions_datatypes():
    kept = set(Selection(sub=["sub-02"], datatype=["dwi"]).select(LISTING))
    assert {
        "sub-02/ses-1/dwi/sub-02_ses-1_dwi.nii.gz",
        "sub-02/ses-1/dwi/sub-02_ses-1_dwi.bval",
        "sub-02/ses-1/dwi/sub-02_ses-1_dwi.bvec",
        "sub-02/sub-02_sessions.tsv",
    } <= kept
    assert not any(p.startswith("sub-01") for p in kept)

    kept = set(Selection(exclude_sub=["02"], ses=["2"]).select(LISTING))
    data = {p for p in kept if p.endswith(".nii.gz")}
    assert data == {
        "sub-01/ses-2/func/sub-01_ses-2_task-rest_bold.nii.gz",
        "sub-01/ses-2/func/sub-01_ses-2_task-motor_bold.nii.gz",
    }


def test_select_globs():
    kept = set(Selection(
        include=["sub-*/ses-1/anat/*"], exclude=["*.html", "README"]
    ).select(LISTING))
    data = {p for p in kept if p.endswith(".nii.gz")}
    assert data == {
        "sub-01/ses-1/anat/sub-01_ses-1_T1w.nii.gz",
        "sub-01/ses-1/anat/sub-01_ses-1_T2w.nii.gz",
        "sub-02/ses-1/anat/sub-02_ses-1_T1w.nii.gz",
    }
    assert "bold_group.html" not in kept and "README" not in kept
    assert "dataset_description.json" in kept
    # same-name sidecar next to the data file
    assert "sub-01/ses-1/anat/sub-01_ses-1_T2w.json" in kept


def test_unknown_folder():
    with pytest.raises(ValueError):
        Selection(folders=["models"])


PARTICIPANTS = (
    "participant_id\tsex\tage\tgroup\n"
    "sub-01\tF\t34\tcontrol\n"
    "sub-02\tM\t61\tpatient\n"
    "sub-03\tM\tn/a\tcontrol\n"
    "sub-04\tf\t70\tcontrol\n"
)


def test_query_participants():
    assert query_participants(PARTICIPANTS, ["age>=60"]) == {"02", "04"}
    assert query_participants(PARTICIPANTS, ["sex==F"]) == {"01", "04"}
    assert query_participants(
        PARTICIPANTS, ["sex=f", "age > 50"]
    ) == {"04"}
    assert query_participants(PARTICIPANTS, ["group != control"]) == {"02"}
    with pytest.raises(ValueError):
        query_participants(PARTICIPANTS, ["age ~ 3"])
    with pytest.raises(KeyError):
        query_participants(PARTICIPANTS, ["height>3"])


def test_trim_tables(tmp_path: Path):
    orig = tmp_path / ".brainspresso" / "orig"
    (orig / "sub-01" / "ses-1").mkdir(parents=True)
    (orig / "participants.tsv").write_text(PARTICIPANTS)
    (orig / "sub-01" / "sub-01_sessions.tsv").write_text(
        "session_id\tacq_time\nses-1\tx\nses-2\ty\n"
    )
    (orig / "sub-01" / "ses-1" / "sub-01_ses-1_scans.tsv").write_text(
        "filename\tacq_time\nanat/sub-01_ses-1_T1w.nii.gz\tx\n"
        "anat/sub-01_ses-1_T2w.nii.gz\ty\n"
    )
    anat = tmp_path / "sub-01" / "ses-1" / "anat"
    anat.mkdir(parents=True)
    (anat / "sub-01_ses-1_T1w.nii.gz").write_bytes(b"")
    (tmp_path / "sub-04").mkdir()

    originals = {
        "participants.tsv": orig / "participants.tsv",
        "sub-01/sub-01_sessions.tsv": orig / "sub-01/sub-01_sessions.tsv",
        "sub-01/ses-1/sub-01_ses-1_scans.tsv":
            orig / "sub-01/ses-1/sub-01_ses-1_scans.tsv",
    }
    written = trim_tables(tmp_path, originals)
    assert sorted(written) == sorted(originals)
    lines = (tmp_path / "participants.tsv").read_text().splitlines()
    assert [ln.split("\t")[0] for ln in lines] == [
        "participant_id", "sub-01", "sub-04"
    ]
    lines = (tmp_path / "sub-01/sub-01_sessions.tsv").read_text().splitlines()
    assert lines == ["session_id\tacq_time", "ses-1\tx"]
    lines = (
        tmp_path / "sub-01/ses-1/sub-01_ses-1_scans.tsv"
    ).read_text().splitlines()
    assert lines == ["filename\tacq_time", "anat/sub-01_ses-1_T1w.nii.gz\tx"]
