"""
Curated INDI projects (`bx fcon1000`, `bx acpi`, `bx slim`...) against a
local fake of s3://fcp-indi, and the generic pieces they rely on (BIDS
entity order, metadata repair, inline entries, derivative filters).
"""
import asyncio
import hashlib
import json
import socket
import threading
from collections import Counter
from pathlib import Path
from xml.sax.saxutils import escape

import pytest
from aiohttp import web

from brainspresso.bids.repair import repair, to_number
from brainspresso.bids.select import Selection
from brainspresso.cli import app
from brainspresso.datasets.INDI.engine import (
    Site, complete_participants, relabel, reorder_entities,
)
from brainspresso.sources import fcp
from brainspresso.sources.s3 import S3Bucket

P = "data/Projects"


def blob(key: str) -> bytes:
    return hashlib.sha256(key.encode()).digest() * 4


def tsv(*rows) -> bytes:
    return "".join("\t".join(r) + "\n" for r in rows).encode()


F = f"{P}/FCON1000"
A = f"{P}/ACPI"
S = f"{P}/INDI/SLIM"
OBJECTS = {
    # --- FCON1000: two sites with a colliding label ------------------
    f"{F}/ICBM/participants.tsv": tsv(
        ["participant_id", "number", "age", "sex", "handedness"],
        ["sub-00448", "1", "25", " m ", "R"],
        ["sub-11111", "2", "40", "f", "L"],
    ),
    f"{F}/ICBM/task-rest_bold.json": b'{"TaskName": "Resting State"}',
    f"{F}/ICBM/README.txt": b"Three rest scans available for each subject",
    f"{F}/ICBM/sourcedata/sub-00448/anat/sub-00448_T1w.nii.gz": None,
    f"{F}/ICBM/sourcedata/sub-00448/func_1/sub-00448_task-rest_bold.nii.gz":
        None,
    f"{F}/ICBM/sourcedata/sub-00448/func_2/sub-00448_task-rest_bold.nii.gz":
        None,
    f"{F}/ICBM/sourcedata/sub-11111/anat/sub-11111_T1wnii.gz": None,
    f"{F}/ICBM/sourcedata/sub-11111/func_1/sub-11111_tast-rest_bold.nii.gz":
        None,
    f"{F}/ICBM/derivatives/sub-00448_T1w_brain.nii.gz": None,
    f"{F}/Atlanta/participants.tsv": tsv(
        ["participant_id", "number", "age", "sex", "handedness"],
        ["sub-00448", "1", "30", "f", "9999"],
    ),
    f"{F}/Atlanta/sourcedata/sub-00448/anat/no_T1w": b"",
    f"{F}/Atlanta/sourcedata/sub-00448/func/sub-00448_task-rest_bold.nii.gz":
        None,
    # its TR differs from the site's (templates/fcon1000/_repetition_times)
    f"{F}/Atlanta/sourcedata/sub-06870/func/sub-06870_task-rest_bold.nii.gz":
        None,
    # --- ACPI: multi-site BIDS with site-level sidecars ---------------
    f"{A}/RawDataBIDS/du_1/T1w.json":
        b'{"MagneticFieldStrength": "3T", "EchoTime": "Min Full"}',
    f"{A}/RawDataBIDS/du_1/task-rest_bold.json":
        b'{"RepetitionTime": 2, "AcquisitionDuration": "6:00",'
        b' "SliceTiming": [0.0, 1000.0, 500.0]}',
    f"{A}/RawDataBIDS/du_1/sub-0028631/ses-1/anat/"
    f"sub-0028631_ses-1_run-1_T1w.nii.gz": None,
    f"{A}/RawDataBIDS/du_1/sub-0028631/ses-1/func/"
    f"sub-0028631_ses-1_task-rest_run-1_bold.nii.gz": None,
    f"{A}/RawDataBIDS/du_1/sub-0028631/sub-0028631_sessions.tsv": tsv(
        ["participant_id", "session", "scannum", "scandesc", "age"],
        ["0028631", "1", "1", "Pre", "22"],
        ["0028631", "1", "2", "Post", "22"],
    ),
    f"{A}/RawDataBIDS/nyu_1/participants.tsv": tsv(
        ["participant_id", "age_at_scan_1", "sex", "handedness"],
        ["0028160", "37.77", "Male", "Right"],
    ),
    f"{A}/RawDataBIDS/nyu_1/task-rest_bold.json":
        b'{"RepetitionTime": 2, "PartialFourier": "8-Jul"}',
    f"{A}/RawDataBIDS/nyu_1/sub-0028160/ses-1/func/"
    f"sub-0028160_ses-1_task-rest_run-1_bold.nii.gz": None,
    f"{A}/PhenotypicData/du_1_phenotypic_data.csv": (
        b'SUB_ID,SESSION,SCANNUM,AGE_AT_SCAN_1,SEX,HANDEDNESS,WEIGHT\n'
        b'"0028631","1","1","22","2","2","61.2"\n'
        b'"0028631","1","2","22","2","2","61.2"\n'
    ),
    f"{A}/PhenotypicData/du_1_phenotypic_key.csv": b"COLUMN LABEL\n",
    # --- SLIM: tables outside the BIDS folder --------------------------
    f"{S}/swu_slim_phenodata_time1.tsv": tsv(
        ["Match_ID", "Sex", "age_at_Scan1", "Scan_Time", "Session"],
        ["25629", "female", "18", "'19-Nov-2011'", "1"],
    ),
    f"{S}/swu_slim_phenodata_time2.tsv": tsv(
        ["Match_ID", "Sex", "Scan_Time", "Session"],
        ["25629", "female", "'04-May-2013'", "2"],
    ),
    f"{S}/RawData/sub-25629/ses-1/anat/sub-25629_ses-1_T1w.nii.gz": None,
    f"{S}/RawData/sub-25629/ses-2/anat/sub-25629_ses-2_T1w.nii.gz": None,
    f"{S}/RawData/sub-25629/ses-1/func/sub-25629_ses-1_task-rest_bold.nii.gz":
        None,
    f"{S}/RawData/sub-30699/ses-1/anat/sub-30699_ses-1_T1w.nii.gz": None,
}
OBJECTS = {k: blob(k) if v is None else v for k, v in OBJECTS.items()}
REQUESTS = Counter()        # listing requests, by prefix


def _list(request):
    prefix = request.query.get("prefix", "")
    delimiter = request.query.get("delimiter")
    REQUESTS[prefix] += 1
    contents, prefixes = [], set()
    for key, value in sorted(OBJECTS.items()):
        if not key.startswith(prefix):
            continue
        rest = key[len(prefix):]
        if delimiter and delimiter in rest:
            prefixes.add(prefix + rest.split(delimiter)[0] + delimiter)
            continue
        contents.append(
            f"<Contents><Key>{escape(key)}</Key>"
            f"<LastModified>2020-01-01T00:00:00.000Z</LastModified>"
            f"<ETag>&quot;{hashlib.md5(value).hexdigest()}&quot;</ETag>"
            f"<Size>{len(value)}</Size></Contents>"
        )
    common = "".join(
        f"<CommonPrefixes><Prefix>{escape(p)}</Prefix></CommonPrefixes>"
        for p in sorted(prefixes)
    )
    xml = (
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f'<IsTruncated>false</IsTruncated>{"".join(contents)}{common}'
        "</ListBucketResult>"
    )
    return web.Response(text=xml, content_type="application/xml")


@pytest.fixture(scope="module")
def bucket_url():
    async def list_objects(request):
        return _list(request)

    async def get_object(request):
        key = request.match_info["key"]
        if key not in OBJECTS:
            raise web.HTTPNotFound()
        return web.Response(body=OBJECTS[key])

    application = web.Application()
    application.router.add_get("/fcp-indi", list_objects)
    application.router.add_get("/fcp-indi/{key:.+}", get_object)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    loop = asyncio.new_event_loop()
    runner = web.AppRunner(application)
    started = threading.Event()

    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(runner.setup())
        loop.run_until_complete(web.SockSite(runner, sock).start())
        started.set()
        loop.run_forever()

    threading.Thread(target=run, daemon=True).start()
    started.wait(10)
    yield f"http://127.0.0.1:{port}"
    loop.call_soon_threadsafe(loop.stop)


@pytest.fixture(autouse=True)
def fake_bucket(bucket_url, monkeypatch):
    monkeypatch.setattr(
        fcp, "get_bucket", lambda: S3Bucket("fcp-indi", endpoint=bucket_url)
    )
    REQUESTS.clear()


def bx(*args):
    try:
        app([str(a) for a in args])
    except SystemExit as e:
        assert not e.code, e.code


def files(root: Path) -> set[str]:
    return {p.relative_to(root).as_posix() for p in root.rglob("*")
            if p.is_file() and ".brainspresso" not in p.parts}


def table(path: Path) -> list[list[str]]:
    return [line.split("\t") for line in path.read_text().splitlines()]


# ----------------------------------------------------------------------
#   FCON1000
# ----------------------------------------------------------------------

def test_fcon1000_harvest(tmp_path):
    bx("fcon1000", "harvest", tmp_path, "--sites", "ICBM", "Atlanta",
       "--folders", "derivatives")
    root = tmp_path / "FCON1000"
    data = {p for p in files(root) if p.startswith("sub-")}
    assert data == {
        # labels prefixed with site codes (sub-00448 exists at both)
        "sub-ICBM00448/anat/sub-ICBM00448_T1w.nii.gz",
        # func_<n> folders -> runs
        "sub-ICBM00448/func/sub-ICBM00448_task-rest_run-1_bold.nii.gz",
        "sub-ICBM00448/func/sub-ICBM00448_task-rest_run-2_bold.nii.gz",
        "sub-ICBM00448/sub-ICBM00448_task-rest_bold.json",
        # typos absorbed (`T1wnii.gz`, `tast-rest`)
        "sub-ICBM11111/anat/sub-ICBM11111_T1w.nii.gz",
        "sub-ICBM11111/func/sub-ICBM11111_task-rest_run-1_bold.nii.gz",
        "sub-ICBM11111/sub-ICBM11111_task-rest_bold.json",
        # `no_T1w` marker skipped
        "sub-Atlanta00448/func/sub-Atlanta00448_task-rest_bold.nii.gz",
        "sub-Atlanta00448/sub-Atlanta00448_task-rest_bold.json",
        "sub-Atlanta06870/func/sub-Atlanta06870_task-rest_bold.nii.gz",
        "sub-Atlanta06870/sub-Atlanta06870_task-rest_bold.json",
    }
    src = root / "sub-ICBM11111/anat/sub-ICBM11111_T1w.nii.gz"
    assert src.read_bytes() == OBJECTS[
        f"{F}/ICBM/sourcedata/sub-11111/anat/sub-11111_T1wnii.gz"
    ]
    # Repetition times from the committed site table and exceptions
    sidecar = json.loads(
        (root / "sub-ICBM00448/sub-ICBM00448_task-rest_bold.json").read_text()
    )
    assert sidecar == {"TaskName": "rest", "RepetitionTime": 2.0}
    sidecar = json.loads((
        root / "sub-Atlanta00448/sub-Atlanta00448_task-rest_bold.json"
    ).read_text())
    assert sidecar["RepetitionTime"] == 2.02
    sidecar = json.loads((
        root / "sub-Atlanta06870/sub-Atlanta06870_task-rest_bold.json"
    ).read_text())
    assert sidecar["RepetitionTime"] == 2.0
    # derivatives
    assert "derivatives/fcon1000/sub-ICBM00448/anat/" \
        "sub-ICBM00448_desc-brain_T1w.nii.gz" in files(root)
    assert "derivatives/fcon1000/desc-brain_T1w.json" in files(root)
    # site notes
    assert "sourcedata/ICBM/README.txt" not in files(root)   # optional
    # participants (committed table), trimmed to what was downloaded
    rows = table(root / "participants.tsv")
    assert rows[0][:6] == ["participant_id", "site", "site_participant_id",
                           "age", "sex", "handedness"]
    assert sorted(r[0] for r in rows[1:]) == [
        "sub-Atlanta00448", "sub-Atlanta06870", "sub-ICBM00448",
        "sub-ICBM11111",
    ]
    for name in ("README", "dataset_description.json", "participants.json"):
        assert name in files(root)


def test_fcon1000_layout_sites(tmp_path):
    bx("fcon1000", "harvest", tmp_path, "--layout", "sites",
       "--sites", "ICBM", "Atlanta", "--folders", "derivatives")
    root = tmp_path / "FCON1000"
    # one BIDS dataset per site, with the labels of the bucket
    assert {p for p in files(root / "ICBM") if p.startswith("sub-")} == {
        "sub-00448/anat/sub-00448_T1w.nii.gz",
        "sub-00448/func/sub-00448_task-rest_run-1_bold.nii.gz",
        "sub-00448/func/sub-00448_task-rest_run-2_bold.nii.gz",
        "sub-11111/anat/sub-11111_T1w.nii.gz",
        "sub-11111/func/sub-11111_task-rest_run-1_bold.nii.gz",
    }
    assert "derivatives/fcon1000/sub-00448/anat/" \
        "sub-00448_desc-brain_T1w.nii.gz" in files(root / "ICBM")
    # site-level sidecar, and a subject sidecar where the TR differs
    atlanta = root / "Atlanta"
    assert json.loads((atlanta / "task-rest_bold.json").read_text()) == {
        "TaskName": "rest", "RepetitionTime": 2.02,
    }
    assert json.loads(
        (atlanta / "sub-06870/sub-06870_task-rest_bold.json").read_text()
    )["RepetitionTime"] == 2.0
    assert not (atlanta / "sub-00448/sub-00448_task-rest_bold.json").exists()
    # site tables: labels of the site, no site columns
    rows = table(atlanta / "participants.tsv")
    assert rows[0] == ["participant_id", "age", "sex", "handedness"]
    assert {r[0] for r in rows[1:]} == {"sub-00448", "sub-06870"}
    columns = json.loads((atlanta / "participants.json").read_text())
    assert set(columns) == {"age", "sex", "handedness"}
    desc = json.loads((atlanta / "dataset_description.json").read_text())
    assert desc["Name"].endswith(": Atlanta")
    assert "../README" in (atlanta / "README").read_text()
    # parent folder: README and merged table of what was downloaded
    assert {"README", "participants.tsv", "participants.json"} <= {
        p.name for p in root.iterdir() if p.is_file()
    }
    assert not (root / "dataset_description.json").exists()
    rows = table(root / "participants.tsv")
    assert sorted(r[0] for r in rows[1:]) == [
        "sub-Atlanta00448", "sub-Atlanta06870", "sub-ICBM00448",
        "sub-ICBM11111",
    ]


def test_fcon1000_layout_sites_labels(tmp_path):
    # labels of the site datasets and merged labels are both accepted
    bx("fcon1000", "harvest", tmp_path, "--layout", "sites",
       "--sites", "ICBM", "Atlanta", "--subs", "00448", "ICBM11111")
    root = tmp_path / "FCON1000"
    assert sorted(p.name for p in root.glob("*/sub-*")) == [
        "sub-00448", "sub-00448", "sub-11111",
    ]
    assert not (root / "Atlanta" / "sub-06870").exists()


def test_fcon1000_prunes_listing(tmp_path):
    bx("fcon1000", "harvest", tmp_path, "--sites", "ICBM",
       "--subs", "ICBM00448")
    root = tmp_path / "FCON1000"
    assert not any("ICBM11111" in p for p in files(root))
    # the unselected subject is never listed, nor other sites
    assert not any("sub-11111" in p for p in REQUESTS)
    assert not any("/Atlanta/" in p for p in REQUESTS)
    # derivatives are not listed unless requested
    assert not any("derivatives" in p for p in REQUESTS)


def test_fcon1000_where(tmp_path):
    bx("fcon1000", "harvest", tmp_path, "--sites", "ICBM",
       "--where", "site==ICBM", "age>=30", "--dry-run")
    # nothing written on a dry run
    assert not (tmp_path / "FCON1000").exists()


def test_fcon1000_unknown_site(tmp_path, capsys):
    with pytest.raises(SystemExit):
        app(["fcon1000", "harvest", str(tmp_path), "--sites", "Nowhere"])
    assert "Unknown sites" in capsys.readouterr().err


# ----------------------------------------------------------------------
#   ACPI (multi-site BIDS)
# ----------------------------------------------------------------------

def test_acpi_harvest(tmp_path):
    bx("acpi", "harvest", tmp_path)
    root = tmp_path / "ACPI"
    out = files(root)
    # site-level sidecars are moved to the subjects of the site
    assert "sub-0028631/sub-0028631_T1w.json" in out
    assert "sub-0028631/sub-0028631_task-rest_bold.json" in out
    assert "sub-0028160/sub-0028160_task-rest_bold.json" in out
    assert not any(p.endswith("_T1w.json") and "0028160" in p for p in out)
    # repaired sidecars
    t1w = json.loads((root / "sub-0028631/sub-0028631_T1w.json").read_text())
    assert t1w == {"MagneticFieldStrength": 3, "EchoTimeText": "Min Full"}
    bold = json.loads(
        (root / "sub-0028631/sub-0028631_task-rest_bold.json").read_text()
    )
    assert bold["AcquisitionDuration"] == 360
    assert bold["SliceTiming"] == [0.0, 1.0, 0.5]
    bold = json.loads(
        (root / "sub-0028160/sub-0028160_task-rest_bold.json").read_text()
    )
    assert bold["PartialFourier"] == 0.875
    # sessions: session_id first, one row per session (the columns
    # that differ between the scans of a session are dropped)
    assert table(root / "sub-0028631/sub-0028631_sessions.tsv") == [
        ["session_id", "participant_id", "age"],
        ["ses-1", "0028631", "22"],
    ]
    # participants: merged, harmonised, du_1 decoded from its key
    rows = table(root / "participants.tsv")
    assert rows[0][:5] == ["participant_id", "site", "age", "sex",
                           "handedness"]
    by_id = {r[0]: r for r in rows[1:]}
    assert by_id["sub-0028631"][1:5] == ["du_1", "22", "M", "R"]
    assert by_id["sub-0028160"][1:5] == ["nyu_1", "37.77", "M", "R"]


def test_acpi_layout_sites(tmp_path):
    bx("acpi", "harvest", tmp_path, "--layout", "sites")
    root = tmp_path / "ACPI"
    # site-level sidecars stay at the site level
    assert (root / "du_1" / "T1w.json").exists()
    assert not (root / "du_1" / "sub-0028631" / "sub-0028631_T1w.json"
                ).exists()
    assert table(root / "du_1" / "participants.tsv")[1][:4] == [
        "sub-0028631", "22", "M", "R",
    ]
    assert (root / "nyu_1" / "sub-0028160").is_dir()
    rows = table(root / "participants.tsv")
    assert rows[0][:2] == ["participant_id", "site"]


# ----------------------------------------------------------------------
#   SLIM (tables outside the BIDS folder)
# ----------------------------------------------------------------------

def test_slim_harvest(tmp_path):
    bx("slim", "harvest", tmp_path)
    root = tmp_path / "SLIM"
    assert table(root / "participants.tsv") == [
        ["participant_id", "sex", "age"],
        ["sub-25629", "F", "18"],
        # in the bucket but not in the tables
        ["sub-30699", "n/a", "n/a"],
    ]
    assert table(root / "sub-25629/sub-25629_sessions.tsv") == [
        ["session_id", "scan_date", "age"],
        ["ses-1", "2011-11-19", "18"],
        ["ses-2", "2013-05-04", "n/a"],
    ]
    assert json.loads((root / "task-rest_bold.json").read_text()) == {
        "TaskName": "rest", "RepetitionTime": 2.0,
    }


def test_slim_sessions_trimmed(tmp_path):
    bx("slim", "harvest", tmp_path, "--sessions", "1")
    root = tmp_path / "SLIM"
    assert [r[0] for r in table(
        root / "sub-25629/sub-25629_sessions.tsv"
    )] == ["session_id", "ses-1"]


# ----------------------------------------------------------------------
#   Building blocks
# ----------------------------------------------------------------------

def test_reorder_entities():
    assert reorder_entities("sub-1/fmap/sub-1_dir-PA_acq-dwi_epi.nii.gz") \
        == "sub-1/fmap/sub-1_acq-dwi_dir-PA_epi.nii.gz"
    assert reorder_entities("sub-1/func/sub-1_run-1_task-rest_bold.json") \
        == "sub-1/func/sub-1_task-rest_run-1_bold.json"
    for path in ("participants.tsv", "task-rest_bold.json",
                 "sub-1/anat/sub-1_foo-1_T1w.nii.gz",     # unknown entity
                 "sourcedata/README.txt"):
        assert reorder_entities(path) == path


def test_relabel():
    site = Site("AnnArbor_a", "FCON1000/AnnArbor_a", "AnnArborA")
    assert relabel("sub-04111/anat/sub-04111_T1w.nii.gz", site) == \
        "sub-AnnArborA04111/anat/sub-AnnArborA04111_T1w.nii.gz"
    assert relabel("sub-1/x.nii", Site("", "X")) == "sub-1/x.nii"


def test_complete_participants():
    text = "participant_id\tage\nsub-1\t20\n"
    assert complete_participants(text, {"sub-1", "sub-2"}) == \
        "participant_id\tage\nsub-1\t20\nsub-2\tn/a\n"
    assert complete_participants(text, {"sub-1"}) == text
    text = "participant_id\tsite\nsub-1\tA\n"
    assert complete_participants(text, {"sub-2"}, {"sub-2": "B"}) == \
        "participant_id\tsite\nsub-1\tA\nsub-2\tB\n"


def test_selection_filters_derivatives():
    sel = Selection(subs=["01"], sessions=["a"], folders=["derivatives"])
    paths = [
        "derivatives/x/dataset_description.json",
        "derivatives/x/sub-01/ses-a/anat/sub-01_ses-a_desc-brain_T1w.nii.gz",
        "derivatives/x/sub-01/ses-b/anat/sub-01_ses-b_desc-brain_T1w.nii.gz",
        "derivatives/x/sub-02/anat/sub-02_desc-brain_T1w.nii.gz",
        "derivatives/sub-02_T1w_brain.nii.gz",
    ]
    assert sel.select(paths) == sorted(paths[:2])


@pytest.mark.parametrize("key,value,expected", [
    ("MagneticFieldStrength", "3T", 3),
    ("MagneticFieldStrength", "1.5 T", 1.5),
    ("PartialFourier", "6/8", 0.75),
    ("PartialFourier", "8-Jul", 0.875),
    ("AcquisitionDuration", "6:00", 360),
    ("EchoTime", "0.03", 0.03),
    ("EchoTime", "Min Full", None),
])
def test_to_number(key, value, expected):
    assert to_number(value, key) == expected


def test_repair_json(tmp_path):
    bad = tmp_path / "acq-MEMPRAGE_T1w.json"
    bad.write_text('{\n "EchoTime" : 0.00164,\n "FlipAngle" :, 7.0,\n'
                   ' "RepetitionTime" : 2.73,\n}\n')
    desc = tmp_path / "dataset_description.json"
    desc.write_text('{"Name": "X",\xa0"BIDSVersion": "1.0.1",'
                    ' "Authors": ["A\tB"], "Funding": "grant",'
                    ' "Acknowledgements": ["C", "D"]}')
    mp2rage = tmp_path / "sub-1_inv-1_MP2RAGE.json"
    mp2rage.write_text('{"RepetitionTime": 5}')
    magnitude = tmp_path / "sub-1_magnitude1.json"
    magnitude.write_text('{"EchoTime": 0.00492}')
    phasediff = tmp_path / "sub-1_phasediff.json"
    phasediff.write_text('{"EchoTime": 0.00738}')
    mton = tmp_path / "sub-1_mt-on_MTR.json"
    mton.write_text('{"FlipAngle": 15}')
    good = tmp_path / "T2w.json"
    good.write_text('{"EchoTime": 0.1}')
    changed = repair([bad, desc, mp2rage, phasediff, magnitude, mton, good])
    assert json.loads(mton.read_text()) == {"FlipAngle": 15, "MTState": True}
    assert good not in changed and magnitude not in changed
    assert json.loads(bad.read_text()) == {
        "EchoTime": 0.00164, "FlipAngle": 7.0, "RepetitionTime": 2.73,
    }
    assert json.loads(desc.read_text()) == {
        "Name": "X", "BIDSVersion": "1.0.1", "Authors": ["A B"],
        "Funding": ["grant"], "Acknowledgements": "C; D",
    }
    assert json.loads(mp2rage.read_text()) == {
        "RepetitionTimePreparation": 5,
    }
    assert json.loads(phasediff.read_text()) == {
        "EchoTime": 0.00738, "EchoTime1": 0.00492, "EchoTime2": 0.00738,
    }


def test_intended_for(tmp_path):
    from brainspresso.bids.repair import fix_intended_for
    sub = tmp_path / "sub-1"
    for name in ("func/sub-1_task-rest_bold.nii.gz",
                 "dwi/sub-1_dwi.nii.gz", "dwi/sub-1_acq-TRACEW_dwi.nii.gz",
                 "fmap/sub-1_dir-AP_epi.nii.gz"):
        (sub / name).parent.mkdir(parents=True, exist_ok=True)
        (sub / name).write_bytes(b"")
    (tmp_path / ".bidsignore").write_text("*TRACEW_dwi.nii.gz\n")
    fmap = sub / "fmap" / "sub-1_dir-AP_epi.json"
    # free-text tags -> local images of that datatype (not ignored ones)
    fmap.write_text('{"IntendedFor": "dwi"}')
    assert fix_intended_for(fmap, tmp_path)
    assert json.loads(fmap.read_text()) == {
        "IntendedFor": ["dwi/sub-1_dwi.nii.gz"]
    }
    fmap.write_text('{"IntendedFor": "fMRI"}')
    fix_intended_for(fmap, tmp_path)
    assert json.loads(fmap.read_text())["IntendedFor"] == [
        "func/sub-1_task-rest_bold.nii.gz"
    ]
    fmap.write_text('{"IntendedFor": "asl", "EchoTime": 0.03}')
    fix_intended_for(fmap, tmp_path)
    assert json.loads(fmap.read_text()) == {"EchoTime": 0.03}
    # trim: targets that are not present are dropped
    fmap.write_text('{"IntendedFor": ["func/sub-1_task-rest_bold.nii.gz",'
                    ' "func/sub-1_task-other_bold.nii.gz",'
                    ' "bids::sub-1/dwi/sub-1_dwi.nii.gz"]}')
    assert not fix_intended_for(fmap, tmp_path, trim=False)
    assert fix_intended_for(fmap, tmp_path, trim=True)
    assert json.loads(fmap.read_text())["IntendedFor"] == [
        "func/sub-1_task-rest_bold.nii.gz",
        "bids::sub-1/dwi/sub-1_dwi.nii.gz",
    ]


def test_repair_tsv(tmp_path):
    pheno = tmp_path / "nycq.tsv"
    pheno.write_text("participant_id  session_id\tQ1\r\n"
                     "sub-01\tses-01\tN/A\r\nsub-02\tses-01\t3\r\n")
    participants = tmp_path / "participants.tsv"
    participants.write_text("participant_id\tage\n001\t\n")
    sessions = tmp_path / "sub-1_sessions.tsv"
    sessions.write_text("id\tsession\tage\nA\tBAS1\t59\nA\tBAS2\t64\n")
    repair([pheno, participants, sessions])
    assert pheno.read_text() == (
        "participant_id\tsession_id\tQ1\nsub-01\tses-01\tn/a\n"
        "sub-02\tses-01\t3\n"
    )
    assert participants.read_text() == "participant_id\tage\nsub-001\tn/a\n"
    assert sessions.read_text() == (
        "session_id\tid\tage\nses-BAS1\tA\t59\nses-BAS2\tA\t64\n"
    )
