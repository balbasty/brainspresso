"""Modality-agnostic selection: companions, recordings, entities, keeps"""
import pytest

from brainspresso.bids.compost import _bids_candidates
from brainspresso.bids.select import (
    COMPANIONS,
    DATATYPES,
    BidsName,
    Selection,
)

ZARR = "sub-01/micr/sub-01_sample-A_SEM.ome.zarr"
CTF = "sub-01/meg/sub-01_task-rest_meg.ds"

# One subject per modality (sub-01), plus a second EEG/MEG subject
LISTING = [
    # dataset files
    ".bidsignore",
    "dataset_description.json",
    "participants.tsv",
    "samples.tsv",
    "README.md",
    "LICENSE",
    "phenotype/moca.tsv",
    "phenotype/moca.json",
    "task-rest_bold.json",
    "task-rest_eeg.json",
    # MRI
    "sub-01/sub-01_sessions.tsv",
    "sub-01/anat/sub-01_acq-mprage_T1w.nii.gz",
    "sub-01/anat/sub-01_acq-mprage_T1w.json",
    "sub-01/anat/sub-01_acq-space_T2w.nii.gz",
    "sub-01/func/sub-01_task-rest_run-01_bold.nii.gz",
    "sub-01/func/sub-01_task-rest_run-02_bold.nii.gz",
    "sub-01/func/sub-01_task-rest_run-01_recording-cardiac_physio.tsv.gz",
    "sub-01/func/sub-01_task-rest_run-01_recording-cardiac_physio.json",
    "sub-01/func/sub-01_task-rest_events.tsv",
    "sub-01/dwi/sub-01_dwi.nii.gz",
    "sub-01/dwi/sub-01_dwi.bval",
    "sub-01/dwi/sub-01_dwi.bvec",
    "sub-01/fmap/sub-01_phasediff.nii.gz",
    "sub-01/fmap/sub-01_magnitude1.nii.gz",
    "sub-01/perf/sub-01_asl.nii.gz",
    "sub-01/perf/sub-01_asl.json",
    "sub-01/perf/sub-01_aslcontext.tsv",
    "sub-01/perf/sub-01_asllabeling.jpg",
    "sub-01/perf/sub-01_m0scan.nii.gz",
    # PET
    "sub-01/pet/sub-01_trc-FDG_pet.nii.gz",
    "sub-01/pet/sub-01_trc-FDG_pet.json",
    "sub-01/pet/sub-01_trc-FDG_recording-manual_blood.tsv",
    "sub-01/pet/sub-01_trc-FDG_recording-manual_blood.json",
    "sub-01/pet/sub-01_trc-FDG_recording-autosampler_blood.tsv",
    # EEG
    "sub-01/eeg/sub-01_task-rest_eeg.edf",
    "sub-01/eeg/sub-01_task-rest_channels.tsv",
    "sub-01/eeg/sub-01_task-rest_events.tsv",
    "sub-01/eeg/sub-01_space-CapTrak_electrodes.tsv",
    "sub-01/eeg/sub-01_space-CapTrak_coordsystem.json",
    "sub-01/eeg/sub-01_photo.jpg",
    # iEEG (MEF3 directory)
    "sub-01/ieeg/sub-01_task-rest_ieeg.mefd/ch1.timd/ch1-000000.segd/x.tdat",
    "sub-01/ieeg/sub-01_task-rest_ieeg.json",
    "sub-01/ieeg/sub-01_task-rest_channels.tsv",
    "sub-01/ieeg/sub-01_space-ACPC_electrodes.tsv",
    "sub-01/ieeg/sub-01_space-ACPC_coordsystem.json",
    # MEG (CTF directory, FIF, BTi directory)
    f"{CTF}/sub-01_task-rest_meg.meg4",
    f"{CTF}/sub-01_task-rest_meg.res4",
    f"{CTF}/BadChannels",
    "sub-01/meg/sub-01_task-rest_meg.json",
    "sub-01/meg/sub-01_task-rest_channels.tsv",
    "sub-01/meg/sub-01_task-noise_meg.fif",
    "sub-01/meg/sub-01_task-noise_channels.tsv",
    "sub-01/meg/sub-01_task-bti_meg/c,rfDC",
    "sub-01/meg/sub-01_task-bti_meg/config",
    "sub-01/meg/sub-01_headshape.pos",
    "sub-01/meg/sub-01_coordsystem.json",
    "sub-01/meg/sub-01_acq-crosstalk_meg.fif",
    "sub-01/meg/sub-01_acq-calibration_meg.dat",
    # NIRS
    "sub-01/nirs/sub-01_task-tap_nirs.snirf",
    "sub-01/nirs/sub-01_task-tap_nirs.json",
    "sub-01/nirs/sub-01_task-tap_channels.tsv",
    "sub-01/nirs/sub-01_task-tap_events.tsv",
    "sub-01/nirs/sub-01_optodes.tsv",
    "sub-01/nirs/sub-01_coordsystem.json",
    # motion
    "sub-01/motion/sub-01_task-walk_tracksys-imu_motion.tsv",
    "sub-01/motion/sub-01_task-walk_tracksys-imu_motion.json",
    "sub-01/motion/sub-01_task-walk_tracksys-imu_channels.tsv",
    # microscopy (OME-Zarr directory with hidden files, OME-TIFF)
    f"{ZARR}/.zattrs",
    f"{ZARR}/.zgroup",
    f"{ZARR}/0/.zarray",
    f"{ZARR}/0/0/0/0",
    "sub-01/micr/sub-01_sample-A_SEM.json",
    "sub-01/micr/sub-01_sample-A_photo.jpg",
    "sub-01/micr/sub-01_sample-B_BF.ome.tif",
    "sub-01/micr/sub-01_sample-B_BF.json",
    # behaviour
    "sub-01/beh/sub-01_task-stroop_beh.tsv",
    "sub-01/beh/sub-01_task-stroop_beh.json",
    "sub-01/beh/sub-01_task-stroop_events.tsv",
    # second subject
    "sub-02/eeg/sub-02_task-rest_eeg.edf",
    "sub-02/eeg/sub-02_task-rest_channels.tsv",
    "sub-02/eeg/sub-02_space-CapTrak_electrodes.tsv",
    "sub-02/eeg/sub-02_space-CapTrak_coordsystem.json",
    "sub-02/meg/sub-02_task-rest_meg.ds/sub-02_task-rest_meg.meg4",
    "sub-02/meg/sub-02_task-rest_meg.json",
]
SKIPPED = [
    ".datalad/config",
    "sub-01/.git/HEAD",
    "derivatives/fs/sub-01/mri/aseg.mgz",
    "models/model.json",
]
DATASET = {
    ".bidsignore", "dataset_description.json", "participants.tsv",
    "samples.tsv", "README.md", "LICENSE", "phenotype/moca.tsv",
    "phenotype/moca.json",
}
SESSIONS = {"sub-01/sub-01_sessions.tsv"}


def select(**kwargs) -> set:
    return set(Selection(**kwargs).select(LISTING + SKIPPED))


def under(prefix: str) -> set:
    return {p for p in LISTING if p.startswith(prefix)}


# ----------------------------------------------------------------------
#   Empty selection
# ----------------------------------------------------------------------

def test_empty_selection_keeps_every_file():
    assert select() == set(LISTING)


@pytest.mark.parametrize("path", [
    "sub-01/pet/sub-01_trc-FDG_recording-manual_blood.tsv",
    "sub-01/pet/sub-01_trc-FDG_recording-manual_blood.json",
    "sub-01/eeg/sub-01_space-CapTrak_electrodes.tsv",
    "sub-01/meg/sub-01_task-rest_meg.json",
    "sub-01/meg/sub-01_task-rest_channels.tsv",
    f"{CTF}/sub-01_task-rest_meg.meg4",
    f"{CTF}/BadChannels",
])
def test_empty_selection_keeps_previously_dropped(path):
    # each file alone with what it describes
    listing = [p for p in LISTING if p.startswith(path.rsplit("/", 1)[0])]
    assert path in Selection().select(listing)
    assert Selection().keeps(path) is True


def test_orphan_metadata_is_kept():
    # metadata that describes nothing of the listing is judged on its own
    listing = ["sub-01/eeg/sub-01_space-CapTrak_electrodes.tsv",
               "sub-02/eeg/sub-02_space-CapTrak_electrodes.tsv"]
    assert Selection().select(listing) == listing
    assert Selection(sub=["01"]).select(listing) == listing[:1]


# ----------------------------------------------------------------------
#   Companions, with filters
# ----------------------------------------------------------------------

def test_eeg_companions():
    kept = select(datatype=["eeg"], sub=["01"])
    assert kept == DATASET | SESSIONS | {"task-rest_eeg.json"} \
        | under("sub-01/eeg/")


def test_ieeg_companions():
    kept = select(datatype=["ieeg"])
    assert kept == DATASET | SESSIONS | under("sub-01/ieeg/")


def test_subject_filter_drops_other_companions():
    kept = select(sub=["02"])
    assert kept == DATASET | {"task-rest_eeg.json"} | under("sub-02/")


def test_pet_blood_companions():
    pet = SESSIONS | under("sub-01/pet/")
    assert select(suffix=["pet"]) == DATASET | pet
    assert select(datatype=["pet"], sub=["01"]) == DATASET | pet
    # blood requested explicitly
    assert select(suffix=["blood"]) == DATASET | SESSIONS | {
        p for p in pet if "_blood." in p
    }
    assert select(suffix=["pet"], sub=["02"]) == DATASET


def test_meg_companions():
    kept = select(suffix=["meg"], sub=["01"])
    assert kept == DATASET | SESSIONS | under("sub-01/meg/")
    # session-level files (headshape, coordsystem, crosstalk) serve
    # every task; channels follow their recording
    kept = select(datatype=["meg"], task=["rest"])
    assert kept == DATASET | SESSIONS | under("sub-02/meg/") | under(CTF) | {
        "sub-01/meg/sub-01_task-rest_meg.json",
        "sub-01/meg/sub-01_task-rest_channels.tsv",
        "sub-01/meg/sub-01_headshape.pos",
        "sub-01/meg/sub-01_coordsystem.json",
        "sub-01/meg/sub-01_acq-crosstalk_meg.fif",
        "sub-01/meg/sub-01_acq-calibration_meg.dat",
    }


def test_nirs_motion_beh_companions():
    for kwargs, folder in [
        ({"datatype": ["nirs"]}, "nirs"),
        # `_motion.tsv` and `_beh.tsv` are data files
        ({"suffix": ["motion"]}, "motion"),
        ({"suffix": ["beh"]}, "beh"),
    ]:
        assert select(**kwargs) == \
            DATASET | SESSIONS | under(f"sub-01/{folder}/")


def test_mri_companions():
    kept = select(datatype=["func"], entities={"run": ["1"]})
    assert kept == DATASET | {
        "task-rest_bold.json",
        "sub-01/sub-01_sessions.tsv",
        "sub-01/func/sub-01_task-rest_run-01_bold.nii.gz",
        "sub-01/func/sub-01_task-rest_run-01_recording-cardiac_physio.tsv.gz",
        "sub-01/func/sub-01_task-rest_run-01_recording-cardiac_physio.json",
        "sub-01/func/sub-01_task-rest_events.tsv",
    }
    kept = select(suffix=["asl"])
    assert kept == DATASET | {
        "sub-01/sub-01_sessions.tsv",
        "sub-01/perf/sub-01_asl.nii.gz",
        "sub-01/perf/sub-01_asl.json",
        "sub-01/perf/sub-01_aslcontext.tsv",
        "sub-01/perf/sub-01_asllabeling.jpg",
    }


def test_companion_requested_explicitly():
    kept = select(suffix=["events"], datatype=["func"])
    assert kept == DATASET | SESSIONS | {
        "sub-01/func/sub-01_task-rest_events.tsv",
    }


# ----------------------------------------------------------------------
#   Directory-format recordings
# ----------------------------------------------------------------------

def test_parse_recording():
    name = BidsName.parse(f"{ZARR}/0/0/0/0")
    assert name.unit == ZARR
    assert name.entities == {"sub": "01", "sample": "A"}
    assert (name.suffix, name.extension, name.datatype) == \
        ("SEM", ".ome.zarr", "micr")
    assert not name.is_metadata
    assert str(name.directory) == "sub-01/micr"
    bti = BidsName.parse("sub-01/meg/sub-01_task-bti_meg/config")
    assert bti.unit == "sub-01/meg/sub-01_task-bti_meg"
    assert (bti.suffix, bti.extension) == ("meg", "")


def test_recordings_are_units():
    zarr, ctf = under(ZARR), under(CTF)
    kept = select(entities={"sample": ["A"]})
    assert zarr <= kept               # hidden files included
    assert "sub-01/micr/sub-01_sample-A_photo.jpg" in kept
    assert "sub-01/micr/sub-01_sample-B_BF.ome.tif" not in kept
    # globs see the recording, never its inner files
    kept = select(include=["*_meg.ds"])
    assert kept & under("sub-01/") == SESSIONS | ctf | {
        "sub-01/meg/sub-01_task-rest_meg.json",
        "sub-01/meg/sub-01_task-rest_channels.tsv",
        "sub-01/meg/sub-01_headshape.pos",
        "sub-01/meg/sub-01_coordsystem.json",
        "sub-01/meg/sub-01_acq-crosstalk_meg.fif",
        "sub-01/meg/sub-01_acq-calibration_meg.dat",
    }
    assert ctf <= select(exclude=["*.meg4"])
    assert not ctf & select(exclude=[CTF])
    assert not ctf & select(include=["*.meg4"])


def test_compost_candidates_include_recording_files():
    candidates = set(_bids_candidates(LISTING + SKIPPED))
    assert under(ZARR) <= candidates
    assert "sub-01/.git/HEAD" not in candidates


# ----------------------------------------------------------------------
#   Entities
# ----------------------------------------------------------------------

def test_entities_filter():
    kept = select(entities={"acq": ["mprage"]})
    data = {p for p in kept if p.startswith("sub-")}
    assert data == {
        "sub-01/sub-01_sessions.tsv",
        "sub-01/anat/sub-01_acq-mprage_T1w.nii.gz",
        "sub-01/anat/sub-01_acq-mprage_T1w.json",
        # crosstalk files: `acq-` only qualifies them, they follow the
        # MEG recordings, none of which has `acq-mprage`
    }
    kept = select(entities={"run": ["1", "02"]}, suffix=["bold"])
    assert {p for p in kept if p.endswith("_bold.nii.gz")} == {
        "sub-01/func/sub-01_task-rest_run-01_bold.nii.gz",
        "sub-01/func/sub-01_task-rest_run-02_bold.nii.gz",
    }


def test_entities_shortcuts():
    sel = Selection(entities={"sub": ["sub-01"], "ses": "1", "acq": []})
    assert sel.sub == {"01"} and sel.ses == {"1"}
    assert sel.entities == {}
    assert Selection(entities={"acq": ["acq-x"]}).entities == {"acq": {"x"}}
    with pytest.raises(ValueError):
        Selection(sub=["01"], entities={"sub": ["02"]})
    # shortcuts may still be changed after construction
    sel = Selection()
    sel.sub = {"02"}
    assert sel.select(LISTING) == sorted(select(sub=["02"]))


# ----------------------------------------------------------------------
#   Per-path predicate and claims
# ----------------------------------------------------------------------

@pytest.mark.parametrize("kwargs", [
    {}, {"sub": ["01"]}, {"datatype": ["meg"], "task": ["rest"]},
    {"suffix": ["pet"]}, {"entities": {"sample": ["A"]}},
    {"include": ["*_meg.ds"]}, {"exclude": ["*.json"]},
    {"folders": ["derivatives"], "sub": ["01"]},
])
def test_keeps_agrees_with_select(kwargs):
    sel = Selection(**kwargs)
    kept = set(sel.select(LISTING + SKIPPED))
    for path in LISTING + SKIPPED:
        name = BidsName.parse(path)
        keeps = sel.keeps(path)
        if path in kept:
            assert keeps is True, path
        elif not name.is_metadata:
            # exact for data and dataset files
            assert keeps is False, path


def test_keeps():
    sel = Selection(sub=["01"], task=["rest"])
    assert sel.keeps("sub-01/func/sub-01_task-rest_run-01_bold.nii.gz")
    assert not sel.keeps("sub-02/eeg/sub-02_task-rest_eeg.edf")
    assert not sel.keeps("sub-01/meg/sub-01_task-noise_meg.fif")
    # metadata: True when it may apply
    assert sel.keeps("task-rest_bold.json")
    assert sel.keeps("sub-01/eeg/sub-01_space-CapTrak_electrodes.tsv")
    assert sel.keeps("sub-01/meg/sub-01_acq-crosstalk_meg.fif")
    assert not sel.keeps("task-motor_bold.json")
    assert not sel.keeps("sub-02/eeg/sub-02_task-rest_channels.tsv")
    assert not sel.keeps("sub-01/meg/sub-01_task-noise_channels.tsv")
    # recordings: the directory or any of its files
    assert sel.keeps(CTF) and sel.keeps(f"{CTF}/BadChannels")
    assert not sel.keeps("sub-02/meg/sub-02_task-rest_meg.ds/x.meg4")
    # dataset files, hidden files, optional folders
    assert sel.keeps("README.md") and sel.keeps("phenotype/moca.tsv")
    assert not sel.keeps(".datalad/config")
    assert not sel.keeps("derivatives/fs/sub-01/mri/aseg.mgz")
    assert Selection(folders=["derivatives"]).keeps(
        "derivatives/fs/sub-01/mri/aseg.mgz"
    )
    assert not Selection(exclude=["*.edf"]).keeps(
        "sub-01/eeg/sub-01_task-rest_eeg.edf"
    )


def test_keeps_subject():
    sel = Selection(sub=["01", "02"], exclude_sub=["02"])
    assert sel.keeps_subject("01") and sel.keeps_subject("sub-01")
    assert not sel.keeps_subject("02") and not sel.keeps_subject("03")
    assert sel.keep_subject("01")       # former name


def test_keeps_claims():
    sel = Selection(sub=["01"])
    assert sel.keeps("sub-02", partial=True) is False
    assert sel.keeps("sub-01", partial=True) is True
    assert sel.keeps("sub-01/ses-1/anat", partial=True) is True

    sel = Selection(sub=["01"], ses=["1"], datatype=["anat"])
    assert sel.keeps("sub-01", partial=True) is None
    assert sel.keeps("sub-01/ses-2", partial=True) is False
    assert sel.keeps("sub-01/ses-1/func", partial=True) is False
    assert sel.keeps("sub-01/ses-1/anat", partial=True) is True

    sel = Selection(entities={"acq": ["mprage"]}, suffix=["T1w"])
    claim = "sub-01/anat/sub-01_acq-{}"
    assert sel.keeps(claim.format("mprage"), partial=True) is None
    assert sel.keeps(claim.format("mprage_T1w"), partial=True) is True
    assert sel.keeps(claim.format("mprage_T2w"), partial=True) is False
    assert sel.keeps(claim.format("space"), partial=True) is False
    # unknown: what include/exclude globs would say
    sel = Selection(sub=["01"], exclude=["*.json"])
    assert sel.keeps("sub-01/anat", partial=True) is None
    assert sel.keeps("sub-02/anat", partial=True) is False
    # optional folders
    sel = Selection(sub=["01"], folders=["derivatives"])
    assert sel.keeps("derivatives/fs/sub-02", partial=True) is False
    assert sel.keeps("derivatives/fs/sub-01", partial=True) is True
    assert sel.keeps("derivatives/fs", partial=True) is None
    assert Selection().keeps("derivatives/fs", partial=True) is False
    assert Selection().keeps("", partial=True) is True


# ----------------------------------------------------------------------
#   Built-in tables against the BIDS schema
# ----------------------------------------------------------------------

def test_tables_match_bids_schema():
    schema = pytest.importorskip("bidsschematools.schema").load_schema()
    keys = {k: v["name"] for k, v in schema["objects"]["entities"].items()}
    datatypes = {v["value"] for v in schema["objects"]["datatypes"].values()}
    assert datatypes - {"phenotype"} <= DATATYPES

    # entities that companions have and the data files of their
    # datatype do not are declared as their own entities
    primary, companion = {}, {}
    for rules in schema["rules"]["files"]["raw"].values():
        for rule in rules.values():
            entities = {keys[e] for e in rule["entities"]}
            for datatype in rule["datatypes"]:
                for suffix in rule["suffixes"]:
                    table = companion if suffix in COMPANIONS else primary
                    table.setdefault((datatype, suffix), set())
                    table[(datatype, suffix)] |= entities
    by_datatype = {}
    for (datatype, _), entities in primary.items():
        by_datatype.setdefault(datatype, set()).update(entities)
    for (datatype, suffix), entities in companion.items():
        extra = entities - by_datatype.get(datatype, set())
        assert extra <= COMPANIONS[suffix], (datatype, suffix)

    # every directory format is a recording unit
    for ext in schema["objects"]["extensions"].values():
        ext = ext["value"]
        if ext.endswith("/"):
            path = f"sub-01/meg/sub-01_task-x_meg{ext.rstrip('/')}/file"
            assert BidsName.parse(path).unit is not None, ext
