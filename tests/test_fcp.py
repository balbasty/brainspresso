"""
Harvesting from s3://fcp-indi (ABIDE-I, CoRR, ADHD-200) and roasting the
result, against a local fake bucket.
"""
import asyncio
import hashlib
import json
import socket
import threading
from pathlib import Path
from xml.sax.saxutils import escape

import pytest
from aiohttp import web

from brainspresso.sources import fcp
from brainspresso.sources.s3 import S3Bucket

P = "data/Projects"


def blob(key: str) -> bytes:
    return hashlib.sha256(key.encode()).digest() * 8


KEYS = [
    # ABIDE-I
    f"{P}/ABIDE/RawData/Caltech/0051456/session_1/anat_1/mprage.nii.gz",
    f"{P}/ABIDE/RawData/Caltech/0051456/session_1/rest_1/rest.nii.gz",
    f"{P}/ABIDE/RawData/Caltech/0051456/session_1/rest_1/QA_rest.xml",
    f"{P}/ABIDE/RawData/Caltech/0051457/session_1/anat_1/mprage.nii.gz",
    f"{P}/ABIDE/RawData/MaxMun_a/0051330/session_1/anat_1/mprage.nii.gz",
    f"{P}/ABIDE/RawData/.DS_Store",
    f"{P}/ABIDE/PhenotypicData/phenotypic_CALTECH.csv",
    f"{P}/ABIDE/PhenotypicData/phenotypic_MAX_MUN.csv",
    # CoRR
    f"{P}/CORR/RawData/CoRR_AggregatedPhenotypicData.csv",
    f"{P}/CORR/RawData/BMB_1/0003001/session_1/anat_1/anat.nii.gz",
    f"{P}/CORR/RawData/BMB_1/0003001/session_1/rest_2/rest.nii.gz",
    f"{P}/CORR/RawData/BMB_1/0003002/session_1/anat_1/anat.nii.gz",
    f"{P}/CORR/RawData/IBA_TRT/0027223/session_1/anat_1/anat.nii.gz",
    # ADHD-200
    f"{P}/ADHD200/RawData/Brown_phenotypic.csv",
    f"{P}/ADHD200/RawData/Brown/0026001/session_1/anat_1/mprage.nii.gz",
    f"{P}/ADHD200/RawData/Brown/0026001/session_1/rest_1/rest.nii.gz",
    f"{P}/ADHD200/RawData/KKI/1018959/session_1/anat_1/mprage.nii.gz",
]
OBJECTS = {k: blob(k) for k in KEYS}


@pytest.fixture(scope="module")
def bucket_url():
    async def list_objects(request):
        prefix = request.query.get("prefix", "")
        contents = "".join(
            f"<Contents><Key>{escape(k)}</Key>"
            f"<LastModified>2020-01-01T00:00:00.000Z</LastModified>"
            f"<ETag>&quot;{hashlib.md5(v).hexdigest()}&quot;</ETag>"
            f"<Size>{len(v)}</Size></Contents>"
            for k, v in sorted(OBJECTS.items()) if k.startswith(prefix)
        )
        xml = (
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/'
            '2006-03-01/"><IsTruncated>false</IsTruncated>'
            f"{contents}</ListBucketResult>"
        )
        return web.Response(text=xml, content_type="application/xml")

    async def get_object(request):
        key = request.match_info["key"]
        if key not in OBJECTS:
            raise web.HTTPNotFound()
        return web.Response(body=OBJECTS[key])

    app = web.Application()
    app.router.add_get("/fcp-indi", list_objects)
    app.router.add_get("/fcp-indi/{key:.+}", get_object)

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    loop = asyncio.new_event_loop()
    runner = web.AppRunner(app)
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


def files(root: Path) -> set[str]:
    return {
        p.relative_to(root).as_posix()
        for p in root.rglob("*") if p.is_file()
    }


def same(path: Path, key: str) -> bool:
    return path.read_bytes() == OBJECTS[f"{P}/{key}"]


# ----------------------------------------------------------------------
#   ABIDE-I
# ----------------------------------------------------------------------

def test_abide1_harvest_s3(tmp_path):
    from brainspresso.datasets.ABIDE.I.download_s3 import download_s3
    download_s3(tmp_path, sites=["Caltech"], subs=[51456])
    src = tmp_path / "ABIDE-1" / "sourcedata"
    assert files(src) == {
        "phenotypic_CALTECH.csv",
        "RawData/Caltech/0051456/session_1/anat_1/mprage.nii.gz",
        "RawData/Caltech/0051456/session_1/rest_1/rest.nii.gz",
    }
    assert same(
        src / "RawData/Caltech/0051456/session_1/anat_1/mprage.nii.gz",
        "ABIDE/RawData/Caltech/0051456/session_1/anat_1/mprage.nii.gz",
    )


def test_abide1_harvest_s3_keys_and_samples(tmp_path):
    from brainspresso.datasets.ABIDE.I.download_s3 import download_s3
    # "MaxMun" selects the "MaxMun_a" sample and "MAX_MUN" phenotypes;
    # a single (non-list) key is accepted.
    download_s3(tmp_path, sites=["MaxMun"], keys="T1w")
    src = tmp_path / "ABIDE-1" / "sourcedata"
    assert files(src) == {
        "RawData/MaxMun_a/0051330/session_1/anat_1/mprage.nii.gz",
    }
    download_s3(tmp_path, sites=["MaxMun"], keys=["meta"])
    assert "phenotypic_MAX_MUN.csv" in files(src)


@pytest.mark.parametrize("layout", ["study", "flat"])
def test_abide1_roast_from_s3_folder(tmp_path, layout):
    from brainspresso.bids.layout import Layout
    from brainspresso.datasets.ABIDE.I.download_s3 import download_s3
    from brainspresso.datasets.ABIDE.I.bidsifier_nitrc import Bidsifier
    download_s3(tmp_path, sites=["Caltech"], keys=["raw"], layout=layout)
    root = tmp_path / "ABIDE-1"
    Bidsifier(root, keys={"T1w", "rest"}).run()
    found = Layout.detect(root)
    assert found.kind == layout
    raw = found.raw
    assert {p for p in files(raw) if not p.startswith((
        "sourcedata/", ".brainspresso/"))} == {
        "sub-51456/anat/sub-51456_T1w.nii.gz",
        "sub-51456/anat/sub-51456_T1w.json",
        "sub-51456/func/sub-51456_task-rest_bold.nii.gz",
        "sub-51456/func/sub-51456_task-rest_bold.json",
        "sub-51457/anat/sub-51457_T1w.nii.gz",
        "sub-51457/anat/sub-51457_T1w.json",
    }
    assert same(
        raw / "sub-51456/anat/sub-51456_T1w.nii.gz",
        "ABIDE/RawData/Caltech/0051456/session_1/anat_1/mprage.nii.gz",
    )
    sidecar = json.loads(
        (raw / "sub-51456/anat/sub-51456_T1w.json").read_text()
    )
    assert "MagneticFieldStrength" in sidecar


# ----------------------------------------------------------------------
#   CoRR
# ----------------------------------------------------------------------

def test_corr_harvest_s3(tmp_path):
    from brainspresso.datasets.CoRR.download import download_s3
    src = tmp_path / "CoRR" / "sourcedata"
    download_s3(
        src, keys={"raw", "meta"}, sites={"BMB", "IBA"}, subs=set(),
        exclude_subs={3002}, if_exists="skip", packet="1MB", jobs=2,
    )
    assert files(src) == {
        "CoRR_AggregatedPhenotypicData.csv",
        "RawData/BMB_1/0003001/session_1/anat_1/anat.nii.gz",
        "RawData/BMB_1/0003001/session_1/rest_2/rest.nii.gz",
        "RawData/IBA_TRT/0027223/session_1/anat_1/anat.nii.gz",
    }


def test_corr_harvest_s3_raw_only(tmp_path):
    from brainspresso.datasets.CoRR.download import download_s3
    src = tmp_path / "CoRR" / "sourcedata"
    download_s3(
        src, keys={"raw"}, sites={"BMB"}, subs={3002}, exclude_subs=set(),
        if_exists="skip", packet="1MB", jobs=None,
    )
    assert files(src) == {
        "RawData/BMB_1/0003002/session_1/anat_1/anat.nii.gz",
    }


# ----------------------------------------------------------------------
#   ADHD-200
# ----------------------------------------------------------------------

@pytest.mark.parametrize("layout", ["study", "flat"])
def test_adhd200_harvest_s3(tmp_path, layout):
    from brainspresso.bids.layout import Layout
    from brainspresso.datasets.ADHD200.download import download
    download(tmp_path, keys=["raw", "meta"], sites=["Brown"], jobs=2,
             layout=layout)
    root = tmp_path / "ADHD-200"
    raw = Layout.detect(root).raw
    assert raw == {"study": root / "rawbids", "flat": root}[layout]
    assert {p for p in files(raw) if not p.startswith((
        "sourcedata/", ".brainspresso/"))} == {
        "sub-0026001/ses-1/anat/sub-0026001_ses-1_T1w.nii.gz",
        "sub-0026001/ses-1/func/sub-0026001_ses-1_task-rest_run-1_bold.nii.gz",
    }
    assert same(
        raw / "sub-0026001/ses-1/anat/sub-0026001_ses-1_T1w.nii.gz",
        "ADHD200/RawData/Brown/0026001/session_1/anat_1/mprage.nii.gz",
    )
    assert "Brown_phenotypic.csv" in files(root / "sourcedata")
    sub2site = json.loads((root / "sourcedata/sub2site.json").read_text())
    assert sub2site == {"0026001": "Brown", "1018959": "KKI"}
