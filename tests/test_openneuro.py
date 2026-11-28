"""
Tests for the S3 lister and the OpenNeuro client, against local mock
servers (no network access needed).
"""
import asyncio
import socket
import threading
from xml.sax.saxutils import escape

import pytest
from aiohttp import web

from brainspresso.sources.openneuro import (
    APIUnavailable,
    OpenNeuroAPI,
    OpenNeuroError,
    digests_from_annex_key,
    list_snapshot,
)
from brainspresso.sources.s3 import S3Bucket, S3Object, parse_s3_url

# ----------------------------------------------------------------------
#   Mock servers
# ----------------------------------------------------------------------

def serve(app):
    """Run an aiohttp app in a thread; return (base_url, stop)"""
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

    def stop():
        loop.call_soon_threadsafe(loop.stop)

    return f"http://127.0.0.1:{port}", stop


KEYS = {
    "ds000001/CHANGES": b"1.0.2 2024-01-01\n  - fix\n1.0.1 2023-01-01\n",
    "ds000001/dataset_description.json": b'{"Name": "x"}',
    "ds000001/participants.tsv": b"participant_id\tage\nsub-01\t30\n",
    "ds000001/sub-01/anat/sub-01_T1w.nii.gz": b"\x00" * 100,
    "ds000001/sub-01/anat/sub-01_T1w.json": b"{}",
}


@pytest.fixture(scope="module")
def s3():
    """Fake S3: ListObjectsV2 with pagination (2 keys per page)"""
    state = {"requests": 0}

    async def list_objects(request):
        state["requests"] += 1
        bucket = request.match_info["bucket"]
        if bucket == "elsewhere" and not request.path.startswith("/eu/"):
            return web.Response(
                status=301, headers={"x-amz-bucket-region": "eu-west-1"}
            )
        if bucket == "private":
            return web.Response(status=403)
        prefix = request.query.get("prefix", "")
        keys = sorted(k for k in KEYS if k.startswith(prefix))
        start = int(request.query.get("continuation-token", 0))
        page = keys[start:start + 2]
        truncated = start + 2 < len(keys)
        contents = "".join(
            f"<Contents><Key>{escape(k)}</Key>"
            f"<LastModified>2024-01-19T03:59:10.000Z</LastModified>"
            f"<ETag>&quot;{'a' * 32 if i % 2 == 0 else 'b' * 32 + '-3'}"
            f"&quot;</ETag><Size>{len(KEYS[k])}</Size></Contents>"
            for i, k in enumerate(page)
        )
        token = (
            f"<NextContinuationToken>{start + 2}</NextContinuationToken>"
            if truncated else ""
        )
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/'
            '2006-03-01/">'
            f"<Name>{bucket}</Name><IsTruncated>{str(truncated).lower()}"
            f"</IsTruncated>{token}{contents}</ListBucketResult>"
        )
        return web.Response(text=xml, content_type="application/xml")

    async def get_object(request):
        key = request.match_info["key"]
        if key not in KEYS:
            raise web.HTTPNotFound()
        return web.Response(body=KEYS[key])

    app = web.Application()
    app.router.add_get("/eu/{bucket}", list_objects)
    app.router.add_get("/{bucket}", list_objects)
    app.router.add_get("/{bucket}/{key:.+}", get_object)
    url, stop = serve(app)
    yield url, state
    stop()


@pytest.fixture(scope="module")
def graphql():
    """Fake OpenNeuro GraphQL API"""
    state = {"queries": []}

    async def handler(request):
        query = (await request.json())["query"]
        state["queries"].append(query)
        if "ds999999" in query:
            return web.json_response({"data": {"dataset": None}})
        if "latestSnapshot" in query and "snapshots" in query:
            return web.json_response({"data": {"dataset": {
                "latestSnapshot": {"tag": "1.0.2"},
                "snapshots": [{"tag": "1.0.1", "created": "2023"},
                              {"tag": "1.0.2", "created": "2024"}],
            }}})
        if "latestSnapshot" in query:
            return web.json_response({"data": {"dataset": {
                "latestSnapshot": {"tag": "1.0.2"}}}})
        if "snapshot(" in query:
            if '"9.9.9"' in query:
                return web.json_response({
                    "data": {"snapshot": None},
                    "errors": [{"message": "Snapshot not found"}],
                })
            return web.json_response({"data": {"snapshot": {
                "id": "ds000001:1.0.1", "tag": "1.0.1", "files": [
                    {"id": "abc", "filename": "dataset_description.json",
                     "size": 13, "directory": False, "annexed": False,
                     "urls": ["https://s3/openneuro.org/ds000001/"
                              "dataset_description.json?versionId=v1"]},
                    {"id": "dir1", "filename": "sub-01", "size": 0,
                     "directory": True, "annexed": False, "urls": []},
                    {"id": "SHA256E-s100--" + "c" * 64 + ".nii.gz",
                     "filename": "sub-01/anat/sub-01_T1w.nii.gz",
                     "size": "100", "directory": False, "annexed": True,
                     "urls": ["https://s3/x?versionId=v2"]},
                    {"id": "MD5E-s5--" + "d" * 32 + ".json",
                     "filename": "sub-01/anat/sub-01_T1w.json",
                     "size": 5, "directory": False, "annexed": True,
                     "urls": []},
                ]}}})
        return web.json_response({"errors": [{"message": "bad query"}]})

    app = web.Application()
    app.router.add_post("/crn/graphql", handler)
    url, stop = serve(app)
    yield f"{url}/crn/graphql", state
    stop()


# ----------------------------------------------------------------------
#   S3
# ----------------------------------------------------------------------

def test_parse_s3_url():
    assert parse_s3_url("s3://fcp-indi/data/Projects/ABIDE/") == (
        "fcp-indi", "data/Projects/ABIDE/"
    )
    assert parse_s3_url("https://s3.amazonaws.com/openneuro.org/ds1/x") == (
        "openneuro.org", "ds1/x"
    )
    with pytest.raises(ValueError):
        parse_s3_url("ftp://x/y")


def test_s3_object_md5():
    assert S3Object("k", 1, etag='"' + "a" * 32 + '"').md5 == "a" * 32
    assert S3Object("k", 1, etag='"' + "a" * 32 + '-4"').md5 is None
    assert S3Object("k", 1).md5 is None


def test_s3_list_paginates(s3):
    url, state = s3
    state["requests"] = 0
    bucket = S3Bucket("openneuro.org", endpoint=url)
    objs = bucket.list_all("ds000001/")
    assert [o.key for o in objs] == sorted(KEYS)
    assert state["requests"] == 3       # 5 keys, 2 per page
    assert objs[0].size == len(KEYS[objs[0].key])
    assert objs[0].mtime.year == 2024
    assert bucket.url("ds000001/a b.nii") == (
        f"{url}/openneuro.org/ds000001/a%20b.nii"
    )


def test_s3_region_redirect(s3):
    url, _ = s3
    # emulate AWS: redirect, then the regional endpoint answers
    seen = []

    class Regional(S3Bucket):
        @property
        def base_url(self):
            seen.append(self.region)
            prefix = "/eu" if self.region == "eu-west-1" else ""
            return f"{url}{prefix}/{self.bucket}"

    bucket = Regional("elsewhere")
    objs = bucket.list_all("ds000001/")
    assert len(objs) == len(KEYS)
    assert bucket.region == "eu-west-1"
    assert seen[0] is None and seen[-1] == "eu-west-1"


def test_s3_private_bucket(s3):
    url, _ = s3
    with pytest.raises(PermissionError):
        S3Bucket("private", endpoint=url).list_all("")


# ----------------------------------------------------------------------
#   OpenNeuro
# ----------------------------------------------------------------------

def test_digests_from_annex_key():
    sha = "c" * 64
    assert digests_from_annex_key(f"SHA256E-s100--{sha}.nii.gz") == {
        "sha256": sha
    }
    assert digests_from_annex_key(f"SHA256-s100--{sha}") == {"sha256": sha}
    assert digests_from_annex_key("MD5E-s5--" + "D" * 32 + ".json") == {
        "md5": "d" * 32
    }
    assert digests_from_annex_key("not-a-key") == {}
    assert digests_from_annex_key(None) == {}


def test_graphql_snapshot_files(graphql):
    url, state = graphql
    api = OpenNeuroAPI(url=url)
    snap = asyncio.run(list_snapshot("ds000001", "1.0.1", api=api))
    assert snap.source == "graphql" and snap.tag == "1.0.1"
    files = {f.path: f for f in snap.files}
    assert set(files) == {
        "dataset_description.json",
        "sub-01/anat/sub-01_T1w.nii.gz",
        "sub-01/anat/sub-01_T1w.json",
    }
    t1 = files["sub-01/anat/sub-01_T1w.nii.gz"]
    assert t1.size == 100 and t1.digests == {"sha256": "c" * 64}
    assert t1.url.endswith("versionId=v2")
    assert files["dataset_description.json"].digests == {}
    # no URL: fall back to the snapshot file endpoint
    assert files["sub-01/anat/sub-01_T1w.json"].url == (
        "https://openneuro.org/crn/datasets/ds000001/snapshots/1.0.1/"
        "files/sub-01%3Aanat%3Asub-01_T1w.json"
    )


def test_graphql_latest_tag(graphql):
    url, _ = graphql
    api = OpenNeuroAPI(url=url)
    snap = asyncio.run(list_snapshot("ds000001", api=api))
    assert snap.tag == "1.0.2"      # resolved, then files listed
    tags = [s["tag"] for s in asyncio.run(api.snapshots("ds000001"))]
    assert tags == ["1.0.1", "1.0.2"]


def test_graphql_errors(graphql):
    url, _ = graphql
    api = OpenNeuroAPI(url=url)
    with pytest.raises(OpenNeuroError, match="not found"):
        asyncio.run(list_snapshot("ds000001", "9.9.9", api=api))
    with pytest.raises(OpenNeuroError, match="not found"):
        asyncio.run(api.latest_tag("ds999999"))
    with pytest.raises(ValueError):
        asyncio.run(list_snapshot("ds1", api=api))


def unreachable_api():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()        # nothing listens on this port
    return OpenNeuroAPI(url=f"http://127.0.0.1:{port}/crn/graphql",
                        max_attempts=1)


def test_fallback_to_s3(s3):
    url, _ = s3
    bucket = S3Bucket("openneuro.org", endpoint=url)
    snap = asyncio.run(list_snapshot(
        "ds000001", api=unreachable_api(), bucket=bucket
    ))
    assert snap.source == "s3"
    assert snap.tag == "1.0.2"          # read from CHANGES
    assert {f.path for f in snap.files} == {
        k[len("ds000001/"):] for k in KEYS
    }
    md5s = [f.digests for f in snap.files]
    assert {"md5": "a" * 32} in md5s and {} in md5s     # multipart: none


def test_no_fallback_for_specific_tag(s3):
    url, _ = s3
    bucket = S3Bucket("openneuro.org", endpoint=url)
    with pytest.raises(OpenNeuroError, match="only be listed"):
        asyncio.run(list_snapshot(
            "ds000001", "1.0.1", api=unreachable_api(), bucket=bucket
        ))
    with pytest.raises(APIUnavailable):
        asyncio.run(list_snapshot(
            "ds000001", source="graphql", api=unreachable_api()
        ))
    # S3 explicitly, with the tag it holds: fine; with another: error
    snap = asyncio.run(list_snapshot(
        "ds000001", "1.0.2", source="s3", bucket=bucket
    ))
    assert snap.tag == "1.0.2"
    with pytest.raises(OpenNeuroError, match="holds snapshot 1.0.2"):
        asyncio.run(list_snapshot(
            "ds000001", "1.0.1", source="s3", bucket=bucket
        ))


def test_graphql_query_is_escaped(graphql):
    url, state = graphql
    api = OpenNeuroAPI(url=url)
    asyncio.run(api.files("ds000001", '1" } evil { "'))
    assert 'tag: "1\\" } evil { \\""' in state["queries"][-1]
