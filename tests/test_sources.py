"""
Sources (`brainspresso.sources`): shared mirror, per-key source choice
with fallback, `LocalSource`, label mapping, credentials, `insecure`
downloads, NITRC and XNAT authentication (fake servers).
"""
import asyncio
import datetime
import hashlib
import io
import json
import os
import socket
import ssl
import sys
import tarfile
import threading
from pathlib import Path

import pytest
from aiohttp import web
from cli_utils import bx

from brainspresso.actions import File
from brainspresso.actions.locks import (
    LOCAL_DIR_ENV,
    TIMEOUT_ENV,
    LockTimeout,
)
from brainspresso.bids.layout import Layout
from brainspresso.bids.select import Selection
from brainspresso.download import Downloader, DownloadManager
from brainspresso.download.manager import DownloadTable
from brainspresso.sources import credentials
from brainspresso.sources.base import (
    DownloadOptions,
    LabelMap,
    LocalSource,
    RemoteEntry,
    Source,
    SourceUnavailable,
    StaticURLs,
    Wants,
    harvest_sources,
    mirror,
    numeric_labels,
    plan_sources,
    select_roaster,
    subject_labels,
    write_content,
)
from brainspresso.sources.credentials import (
    CredentialsError,
    NitrcAuth,
    get_credentials,
    get_secret,
)


def serve(app, ssl_context=None):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    loop = asyncio.new_event_loop()
    runner = web.AppRunner(app)
    started = threading.Event()

    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(runner.setup())
        site = web.SockSite(runner, sock, ssl_context=ssl_context)
        loop.run_until_complete(site.start())
        started.set()
        loop.run_forever()

    threading.Thread(target=run, daemon=True).start()
    started.wait(10)
    scheme = "https" if ssl_context else "http"
    return f"{scheme}://127.0.0.1:{port}", \
        lambda: loop.call_soon_threadsafe(loop.stop)


def files(root: Path) -> set[str]:
    return {p.relative_to(root).as_posix() for p in root.rglob("*")
            if p.is_file() and ".brainspresso" not in p.parts}


# ----------------------------------------------------------------------
#   Fake hosts: static files, NITRC login, XNAT
# ----------------------------------------------------------------------

FILES = {f"/data/{name}": name.encode() * 10 for name in (
    "a.txt", "b.txt", "dir/c.txt",
)}
USER, PASSWORD = "jdoe", "s3cret"


def tarball(name: str) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo(name)
        info.size = len(name)
        tar.addfile(info, io.BytesIO(name.encode()))
    return buf.getvalue()


# XNAT project "ABIDE": subject -> scans, assessors
XNAT_SUBJECTS = {
    "Caltech_0051456": {"scans": ["anat", "rest"],
                        "assessors": ["Caltech_0051456-man_qa"]},
    "Caltech_0051457": {"scans": ["anat"], "assessors": []},
}


@pytest.fixture(scope="module")
def host():
    state = {"logins": [], "jsessions": 0, "requests": []}

    def logged_in(request):
        return request.cookies.get("session") == "ok"

    async def static(request):
        state["requests"].append(request.path)
        if request.path not in FILES:
            raise web.HTTPNotFound()
        return web.Response(body=FILES[request.path])

    async def protected(request):
        # NITRC project downloads: login cookie required
        state["requests"].append(request.path)
        if not logged_in(request):
            raise web.HTTPUnauthorized()
        name = request.match_info["name"]
        return web.Response(body=f"protected:{name}".encode())

    async def login(request):
        form = await request.post()
        state["logins"].append(dict(form))
        response = web.Response(text="welcome")
        if form.get("form_loginname") == USER and \
                form.get("form_pw") == PASSWORD:
            response.set_cookie("session", "ok")
        return response

    async def jsession(request):
        auth = request.headers.get("Authorization", "")
        import base64
        expected = "Basic " + base64.b64encode(
            f"{USER}:{PASSWORD}".encode()).decode()
        if auth != expected:
            raise web.HTTPUnauthorized()
        state["jsessions"] += 1
        response = web.Response(text="JSESSIONID")
        response.set_cookie("session", "ok")
        return response

    def results(rows):
        return web.json_response({"ResultSet": {"Result": rows}})

    async def xnat(request):
        if not logged_in(request):
            raise web.HTTPUnauthorized()
        parts = request.match_info["path"].strip("/").split("/")
        # archive/projects/ABIDE[/subjects/<s>[/experiments/<e>]]/<what>
        if parts[-1] == "subjects":
            return results([{"label": s} for s in XNAT_SUBJECTS])
        sub = parts[parts.index("subjects") + 1]
        if parts[-1] == "experiments":
            return results([{"label": sub, "ID": f"E_{sub}"}])
        if parts[-1] == "scans":
            return results([{"ID": s, "type": s}
                            for s in XNAT_SUBJECTS[sub]["scans"]])
        if parts[-1] == "assessors":
            return results([{"label": a, "ID": a}
                            for a in XNAT_SUBJECTS[sub]["assessors"]])
        if parts[-1] == "files" and request.query.get("format") == "tar.gz":
            return web.Response(body=tarball("/".join(parts[-4:-1])))
        raise web.HTTPNotFound()

    app = web.Application()
    app.router.add_get("/data/{path:.+}", static)
    app.router.add_get("/downloads/{name:.+}", protected)
    app.router.add_post("/account/login.php", login)
    app.router.add_post("/ir/data/JSESSION", jsession)
    async def logout(request):
        return web.Response()

    app.router.add_delete("/ir/data/JSESSION", logout)
    app.router.add_get("/ir/data/{path:.+}", xnat)
    url, stop = serve(app)
    # aiohttp does not keep cookies of IP addresses: use a host name
    state["url"] = url.replace("127.0.0.1", "localhost")
    yield state
    stop()


@pytest.fixture
def nitrc_env(monkeypatch):
    monkeypatch.setenv("NITRC_USER", USER)
    monkeypatch.setenv("NITRC_PASS", PASSWORD)
    monkeypatch.setattr(credentials, "interactive", lambda: False)


# ----------------------------------------------------------------------
#   RemoteEntry, mirror
# ----------------------------------------------------------------------

def test_remote_entry_defaults():
    e = RemoteEntry("sub-01/anat/x.nii.gz")
    assert (e.url, e.size, e.mtime, e.digests, e.meta) == \
        (None, None, None, {}, {})
    assert e.name == "x.nii.gz" and e.relpath.parts[0] == "sub-01"
    # mutable defaults are not shared
    e.meta["k"] = 1
    assert RemoteEntry("y").meta == {}


def test_mirror_downloaders(tmp_path):
    mtime = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
    entries = [
        RemoteEntry("a.txt", "http://h/a", size=3, mtime=mtime,
                    digests={"md5": "0" * 32}),
        RemoteEntry("dir/", "http://h/dir/"),          # folder: skipped
        RemoteEntry("b.txt", "http://h/b"),
        RemoteEntry("c.txt", "http://h/c"),
    ]
    dls = list(mirror(
        entries, tmp_path,
        keep=lambda e: e.name != "c.txt",
        rename=lambda e: f"x/{e.path}",
        ifexists="overwrite", insecure=True,
    ))
    assert [d.dst for d in dls] == [tmp_path / "x/a.txt",
                                    tmp_path / "x/b.txt"]
    assert dls[0]._size == 3 and dls[0]._mtime == mtime
    assert dls[0].digests == {"md5": "0" * 32}
    assert dls[1].digests is None
    assert all(d.insecure for d in dls)
    with pytest.raises(ValueError, match="no URL"):
        list(mirror([RemoteEntry("no-url")], tmp_path))


def test_mirror_filters_before_requiring_a_url(tmp_path):
    entries = [RemoteEntry("a.txt", "http://h/a"), RemoteEntry("dropped"),
               RemoteEntry("renamed-away")]
    dls = list(mirror(
        entries, tmp_path,
        keep=lambda e: e.path != "dropped",
        rename=lambda e: None if e.path == "renamed-away" else e.path,
    ))
    assert [d.dst for d in dls] == [tmp_path / "a.txt"]


def test_mirror_downloads_and_generated_files(host, tmp_path):
    url = host["url"]
    md5 = hashlib.md5(FILES["/data/a.txt"]).hexdigest()
    source = StaticURLs({
        "raw": [RemoteEntry("a.txt", f"{url}/data/a.txt", digests={
            "md5": md5})],
        "meta": [StaticURLs.entry(f"{url}/data/dir/c.txt", "meta/c.txt")],
    })
    entries = source.list(keys=Wants(["raw", "meta"]))
    entries.append(RemoteEntry("generated.json", meta={"content": "{}"}))
    layout = Layout.detect(tmp_path) or __import__(
        "brainspresso.bids.layout", fromlist=["x"]).make_layout(
            "study", tmp_path)
    source.fetch(entries, layout, DownloadOptions(on_error="raise"))
    assert files(tmp_path / "sourcedata") == {
        "a.txt", "meta/c.txt", "generated.json"}
    assert (tmp_path / "sourcedata/a.txt").read_bytes() == \
        FILES["/data/a.txt"]
    # keys filter the static lists
    assert [e.path for e in source.list(keys=["meta"])] == ["meta/c.txt"]


@pytest.mark.parametrize("on_error", ["yield", "raise"])
def test_generated_file_in_use_does_not_stop_the_harvest(
        host, tmp_path, monkeypatch, on_error):
    monkeypatch.setenv(LOCAL_DIR_ENV, str(tmp_path / "locks"))
    monkeypatch.setenv(TIMEOUT_ENV, "0.3")
    source = StaticURLs()
    entries = [
        RemoteEntry("first.json", meta={"content": "new"}),
        RemoteEntry("a.txt", f"{host['url']}/data/a.txt"),
        RemoteEntry("second.json", meta={"content": "new"}),
    ]
    first = tmp_path / "first.json"
    first.write_text("old")
    with File(first, "r"):      # a roast reads it for longer than the wait
        if on_error == "raise":
            with pytest.raises(LockTimeout):
                source.fetch(entries, tmp_path,
                             DownloadOptions(on_error=on_error))
            return
        done = source.fetch(entries, tmp_path,
                            DownloadOptions(on_error=on_error))
    assert first.read_text() == "old"
    # the other entries were fetched, and written
    assert (tmp_path / "a.txt").read_bytes() == FILES["/data/a.txt"]
    assert (tmp_path / "second.json").read_text() == "new"
    assert {d.dst.name for d in done} == {"first.json", "a.txt"}


def test_unwritten_generated_file_fails_as_a_download(host, tmp_path,
                                                      monkeypatch):
    monkeypatch.setenv(LOCAL_DIR_ENV, str(tmp_path / "locks"))
    monkeypatch.setenv(TIMEOUT_ENV, "0.3")
    rows = []
    monkeypatch.setattr(DownloadTable, "__call__",
                        lambda self, row: rows.append(row))
    entries = [
        RemoteEntry("first.json", meta={"content": "new"}),
        RemoteEntry("missing.txt", f"{host['url']}/data/missing.txt"),
    ]
    first = tmp_path / "first.json"
    first.write_text("old")
    with File(first, "r"):
        StaticURLs().fetch(entries, tmp_path, DownloadOptions(path="name"))
    # the same final row as a failed download (there is no other count)
    final = {row["path"]: row for row in rows if row.get("status")}
    assert final["first.json"]["status"] == "error"
    assert final["first.json"]["message"].startswith("[LockTimeout]")
    assert final["missing.txt"]["status"] == "error"


def test_dry_run_downloads_nothing(host, tmp_path, capsys):
    source = StaticURLs({"raw": [f"{host['url']}/data/b.txt"]})
    before = len(host["requests"])
    source.harvest(tmp_path, options=DownloadOptions(dry_run=True))
    assert not (tmp_path / "b.txt").exists()
    assert len(host["requests"]) == before
    assert "b.txt" in capsys.readouterr().out


# ----------------------------------------------------------------------
#   Keys, labels
# ----------------------------------------------------------------------

TREE = {"all": {"meta": {"pheno"}, "raw": {"anat": {"T1w"}, "rest": {}},
                "derivatives": {"fs": {}}}}


def test_wants_tree():
    wants = Wants(["T1w"], TREE)
    assert wants("T1w") and wants("anat") and wants("raw") and wants("all")
    assert not wants("rest") and not wants("meta")
    raw = Wants(["all"], TREE).restrict(["raw"])
    assert raw("T1w") and raw("rest") and not raw("meta")
    assert not raw("fs")
    assert Wants()("anything")
    assert not Wants(["all"], TREE, exclude=["raw"])("T1w")


def test_numeric_labels():
    labels = numeric_labels(4, "OAS3")
    assert labels.to_bids("OAS30001") == "0001"
    assert labels.to_bids("0AS_data_files") is None
    assert labels.to_host("sub-0001") == "OAS30001"
    selection = Selection(subs=["0001", "0003"], exclude_subs=["0003"])
    assert labels.keeps(selection, "OAS30001")
    assert not labels.keeps(selection, "OAS30002")
    assert not labels.keeps(selection, "OAS30003")
    assert labels.hosts(selection) == ["OAS30001"]
    assert numeric_labels().to_bids("0051456") == "51456"
    assert LabelMap().keeps(None, "anything")


def test_subject_labels(tmp_path):
    ids = tmp_path / "ids.txt"
    ids.write_text("7\n8\n")
    assert subject_labels([1, "sub-02", "3:6:2", str(ids)], width=3) == {
        "001", "002", "003", "005", "007", "008"}
    with pytest.raises(ValueError):
        subject_labels(["3:"])
    # a list saved with a byte order mark (Windows Notepad)
    ids.write_bytes(b"\xef\xbb\xbf9\r\n10\r\n")
    assert subject_labels([str(ids)], width=3) == {"009", "010"}


def test_url_list_with_a_byte_order_mark(tmp_path):
    from brainspresso.datasets.adni.i.dataset import URLOptions
    urls = tmp_path / "urls.txt"
    urls.write_bytes(b"\xef\xbb\xbfhttps://a/1\r\nhttps://a/2\n")
    options = URLOptions(urls=(str(urls), "https://b/3"))
    assert options.all_urls() == ["https://a/1", "https://a/2",
                                  "https://b/3"]


# ----------------------------------------------------------------------
#   Per-key choice with fallback
# ----------------------------------------------------------------------

class Fake(Source):
    def __init__(self, name, provides, fail=False):
        super().__init__(name=name, provides=provides)
        self.fail = fail
        self.calls = []

    def harvest(self, layout, selection=None, keys=None, options=None):
        if self.fail:
            raise SourceUnavailable("offline")
        self.calls.append(sorted(k for k in ("raw", "meta", "fs")
                                 if keys(k)))
        return []


def fakes(**fail):
    return {
        "s3": Fake("s3", {"raw", "meta"}, fail.get("s3", False)),
        "nitrc": Fake("nitrc", {"raw", "meta"}, fail.get("nitrc", False)),
        "xnat": Fake("xnat", {"raw", "fs"}, fail.get("xnat", False)),
    }


DEFAULTS = {"raw": ["s3", "nitrc"], "meta": ["nitrc"]}


def test_plan_sources():
    sources = fakes()
    assert plan_sources(sources, DEFAULTS, Wants(["all"], TREE)) == [
        (("raw",), ["s3", "nitrc"]), (("meta",), ["nitrc"]),
    ]
    assert plan_sources(sources, DEFAULTS, Wants(["T1w"], TREE)) == [
        (("raw",), ["s3", "nitrc"]),
    ]
    # --source: the keys that source provides, nothing else
    assert plan_sources(sources, DEFAULTS, Wants(["all"], TREE),
                        override="xnat") == [(("raw", "fs"), ["xnat"])]
    # same candidates: one group
    assert plan_sources(sources, {"raw": ["s3"], "meta": ["s3"]}) == [
        (("raw", "meta"), ["s3"])]
    with pytest.raises(ValueError, match="Unknown sources"):
        plan_sources(sources, DEFAULTS, override="ftp")


def test_harvest_sources_fallback(tmp_path):
    sources = fakes(s3=True)
    used = harvest_sources(sources, DEFAULTS, tmp_path,
                           keys=Wants(["all"], TREE))
    assert used == {"raw": "nitrc", "meta": "nitrc"}
    # each call only sees the keys of its group
    assert sources["nitrc"].calls == [["raw"], ["meta"]]
    sources = fakes(s3=True, nitrc=True)
    with pytest.raises(SourceUnavailable, match="No available source"):
        harvest_sources(sources, DEFAULTS, tmp_path, keys=["raw"])


def test_local_source(tmp_path):
    local = LocalSource(provides={"raw", "meta"}, folder="ADNI")
    assert list(local.list(Selection(), ["raw"])) == []
    assert local.fetch([], tmp_path) == []
    assert local.harvest(tmp_path) == []
    assert not local.present(tmp_path)
    (tmp_path / "ADNI").mkdir()
    (tmp_path / "ADNI" / "scan.zip").write_bytes(b"")
    assert local.present(tmp_path)
    # a user-provided folder satisfies a key without any request
    sources = {"local": local}
    assert harvest_sources(sources, {"raw": ["local"]}, tmp_path,
                           keys=["raw"]) == {"raw": "local"}


def test_select_roaster():
    def roast_a(ctx):
        yield from ()

    def roast_b(ctx):
        yield from ()
    roast_b.detect = lambda ctx: ctx == "b"

    class WithRoaster(Source):
        roaster = roast_a           # a function, not a method

    a = WithRoaster(name="a")
    b = Source(name="b", roaster=roast_b)
    assert a.roaster is roast_a
    assert select_roaster([a, b], "b") is b
    assert select_roaster([a, b], "x") is a
    assert select_roaster([Source(name="c")], "x") is None


def test_configure_ignores_unknown_options():
    s = StaticURLs({}, name="s")
    assert s.configure(user="x", jobs=None) is not s
    with pytest.raises(TypeError, match="unknown option"):
        s.with_options(user="x")
    with pytest.raises(TypeError):
        s.with_options(list=1)          # methods are not options


# ----------------------------------------------------------------------
#   Credentials
# ----------------------------------------------------------------------

def test_secret_from_value_and_env(monkeypatch):
    monkeypatch.setenv("B_TOKEN", "from-env")
    monkeypatch.setattr(credentials, "interactive", lambda: False)
    assert get_secret("Token", "given", ["A_TOKEN", "B_TOKEN"]) == "given"
    assert get_secret("Token", None, ["A_TOKEN", "B_TOKEN"]) == "from-env"


def test_secret_from_prompt(monkeypatch):
    monkeypatch.delenv("A_TOKEN", raising=False)
    asked = []
    monkeypatch.setattr(credentials, "interactive", lambda: True)
    monkeypatch.setattr(credentials, "prompt",
                        lambda msg, hidden: asked.append((msg, hidden))
                        or "typed")
    assert get_secret("Token", None, ["A_TOKEN"], hidden=True) == "typed"
    assert asked == [("Token: ", True)]
    assert get_credentials("NITRC", env_user=["A_TOKEN"]) == \
        ("typed", "typed")


def test_secret_error(monkeypatch):
    monkeypatch.delenv("A_TOKEN", raising=False)
    monkeypatch.setattr(credentials, "interactive", lambda: False)
    with pytest.raises(CredentialsError) as e:
        get_secret("NITRC password", None, ["A_TOKEN"], option="--password")
    assert "`A_TOKEN`" in str(e.value) and "--password" in str(e.value)
    # an empty answer is an error too
    monkeypatch.setattr(credentials, "interactive", lambda: True)
    monkeypatch.setattr(credentials, "prompt", lambda msg, hidden: "")
    with pytest.raises(CredentialsError):
        get_secret("NITRC password", None, ["A_TOKEN"])


def test_nitrc_auth_is_picklable(nitrc_env):
    import pickle
    auth = pickle.loads(pickle.dumps(NitrcAuth()))
    assert (auth.user, auth.password) == (USER, PASSWORD)


# ----------------------------------------------------------------------
#   NITRC login (async), regression: ABIDE-II and ABIDE-I xnat used the
#   sync authentifier with aiohttp sessions
# ----------------------------------------------------------------------

def test_nitrc_auth_logs_in_on_401(host, nitrc_env, tmp_path):
    url = host["url"]
    auth = NitrcAuth(url=f"{url}/account/login.php")
    DownloadManager([Downloader(f"{url}/downloads/x.csv", tmp_path / "x.csv",
                                auth=auth)], on_error="raise").run()
    assert (tmp_path / "x.csv").read_bytes() == b"protected:x.csv"
    assert host["logins"][-1] == {"form_loginname": USER,
                                  "form_pw": PASSWORD}


def test_abide2_harvest_nitrc_login(host, nitrc_env, tmp_path, monkeypatch):
    from brainspresso.datasets.indi.abide.ii import harvest as abide2
    url = host["url"]
    monkeypatch.setattr(abide2.NITRCTarballs, "login_url",
                        f"{url}/account/login.php")
    monkeypatch.setattr(abide2.NITRCTarballs, "urls", {
        "meta": [abide2.NITRC.entry(f"{url}/downloads/ABIDEII-BNI_1.csv",
                                    site="BNI")],
        "raw": [abide2.NITRC.entry(f"{url}/downloads/ABIDEII-BNI_1.tar.gz",
                                   site="BNI")],
    })
    before = len(host["logins"])
    bx("abide2", "harvest", tmp_path, "--keys", "meta", "--site", "BNI")
    src = tmp_path / "ABIDE-2" / "sourcedata"
    assert files(src) == {"ABIDEII-BNI_1.csv"}
    assert (src / "ABIDEII-BNI_1.csv").read_bytes() == \
        b"protected:ABIDEII-BNI_1.csv"
    assert len(host["logins"]) > before


def test_abide1_harvest_xnat(host, nitrc_env, tmp_path, monkeypatch):
    from brainspresso.datasets.indi.abide.i import sources as abide1_sources
    url = host["url"]
    monkeypatch.setattr(abide1_sources.XNATImages, "server", f"{url}/ir")
    monkeypatch.setattr(abide1_sources.XNATMeta, "login_url",
                        f"{url}/account/login.php")
    monkeypatch.setattr(abide1_sources.XNATMeta, "urls", {"meta": [
        abide1_sources.NITRC.entry(f"{url}/downloads/Phenotypic_V1_0b.csv"),
        abide1_sources.NITRC.entry(f"{url}/data/a.txt", "Caltech/anat.pdf"),
    ]})
    bx("abide1", "harvest", tmp_path, "--source", "xnat", "--keys", "meta",
       "anat", "qa-man", "--sub", "51456")
    src = tmp_path / "ABIDE-1" / "sourcedata"
    assert files(src) == {
        "Phenotypic_V1_0b.csv", "Caltech/anat.pdf",
        "Caltech_0051456/anat.tar.gz",
        "Caltech_0051456/Caltech_0051456-man_qa.tar.gz",
    }
    assert (src / "Phenotypic_V1_0b.csv").read_bytes() == \
        b"protected:Phenotypic_V1_0b.csv"
    with tarfile.open(src / "Caltech_0051456/anat.tar.gz") as tar:
        assert tar.getnames() == ["Caltech_0051456/scans/anat"]
    assert host["jsessions"] >= 1


# ----------------------------------------------------------------------
#   insecure: TLS verification
# ----------------------------------------------------------------------

@pytest.fixture(scope="module")
def https(tmp_path_factory):
    x509 = pytest.importorskip("cryptography.x509")
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(1)
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=1))
            .sign(key, hashes.SHA256()))
    folder = tmp_path_factory.mktemp("tls")
    (folder / "cert.pem").write_bytes(
        cert.public_bytes(serialization.Encoding.PEM))
    (folder / "key.pem").write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()))
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(folder / "cert.pem", folder / "key.pem")

    async def handler(request):
        return web.Response(body=b"secret bytes")

    app = web.Application()
    app.router.add_get("/{name}", handler)
    url, stop = serve(app, context)
    yield url
    stop()


def test_insecure_download(https, tmp_path):
    dl = Downloader(f"{https}/f", tmp_path / "f", max_attempts=1)
    DownloadManager([dl]).run()
    assert not (tmp_path / "f").exists()        # self-signed: refused
    DownloadManager([Downloader(f"{https}/f", tmp_path / "f",
                                insecure=True)], on_error="raise").run()
    assert (tmp_path / "f").read_bytes() == b"secret bytes"
    # through a source and the download options
    source = StaticURLs({"raw": [f"{https}/g"]})
    source.harvest(tmp_path, options=DownloadOptions(insecure=True,
                                                     on_error="raise"))
    assert (tmp_path / "g").read_bytes() == b"secret bytes"


def test_tls_request_options_are_refused(tmp_path):
    for key in ("ssl", "verify", "verify_ssl"):
        with pytest.raises(TypeError, match="insecure=True"):
            Downloader("https://h/f", tmp_path / "f", get_opt={key: False})


def test_nitrc_sources_are_insecure_by_default():
    from brainspresso.sources.nitrc import NITRC
    source = NITRC({"raw": ["https://h/f"]}, login=False)
    opts = source.download_options(DownloadOptions())
    assert opts["insecure"] is True and "auth" not in opts
    opts = source.download_options(DownloadOptions(insecure=False))
    assert "insecure" not in opts


def test_unc_folders_json_merged(tmp_path):
    from brainspresso.datasets.unc_wisconsin.harvest import Girder
    src = tmp_path / "sourcedata"
    src.mkdir()
    (src / "folders.json").write_text(json.dumps({"001": {"sex": "F"}}))
    Girder().fetch([RemoteEntry("folders.json", meta={
        "generated": True, "content": {"002": {"sex": "M"}}})], src)
    assert json.loads((src / "folders.json").read_text()) == {
        "001": {"sex": "F"}, "002": {"sex": "M"}}


def test_unreachable_s3_is_unavailable():
    from brainspresso.sources.s3 import S3Bucket, S3Prefix
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()                    # nothing listens on this port
    bucket = S3Bucket("b", endpoint=f"http://127.0.0.1:{port}",
                      max_attempts=1)
    with pytest.raises(SourceUnavailable):
        S3Prefix(bucket, "data").list()


# ----------------------------------------------------------------------
#   Review fixes
# ----------------------------------------------------------------------

@pytest.fixture
def no_credentials(monkeypatch):
    for name in ("NITRC_USER", "NITRC_PASS", "XNAT_USER", "XNAT_PASS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(credentials, "interactive", lambda: False)


def test_ixi_user_without_password(no_credentials):
    from brainspresso.datasets.ixi.harvest import NitrcIR
    auth = NitrcIR().configure(user="anon").auth()
    assert (auth.user, auth.password) == ("anon", "")


def test_ixi_listing_access_denied():
    from brainspresso.datasets.ixi.harvest import NitrcIR

    async def deny(request):
        return web.Response(status=403)

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", deny)
    url, stop = serve(app)
    try:
        with pytest.raises(PermissionError, match="--user"):
            NitrcIR(server=url).list()
    finally:
        stop()


def test_wants_exclude_key_aliases_group():
    from brainspresso.bids.keys import KeyAliases
    tree = KeyAliases({"raw": {"anat": {"T1w", "T2w"}, "dwi": {"dwi"}}})
    wants = Wants(tree=tree, exclude=["anat"])
    assert not wants("T1w") and not wants("T2w") and wants("dwi")
    assert Wants(tree=tree, exclude=["unknown"])("T1w")


def test_write_content_with_stale_tmp_dir(tmp_path):
    (tmp_path / "a.json.tmp").mkdir()
    assert write_content(tmp_path / "a.json", "{}")
    assert (tmp_path / "a.json").read_text() == "{}"
    assert sorted(p.name for p in tmp_path.iterdir()) == \
        ["a.json", "a.json.tmp"]


@pytest.fixture
def umask_002():
    old = os.umask(0o002)
    try:
        yield
    finally:
        os.umask(old)


@pytest.mark.skipif(sys.platform == "win32", reason=(
    "POSIX permission bits: Windows has no umask or group/other modes"))
def test_write_content_mode_follows_the_umask(tmp_path, umask_002):
    path = tmp_path / "a.json"
    assert write_content(path, "{}")
    assert path.stat().st_mode & 0o777 == 0o664
    assert write_content(path, "[]", "overwrite")
    assert path.stat().st_mode & 0o777 == 0o664
    assert sorted(p.name for p in tmp_path.iterdir()) == ["a.json"]


def test_write_content_of_a_long_name(tmp_path):
    path = tmp_path / ("x" * 250)       # near the limit of file names
    assert write_content(path, "{}")
    assert sorted(p.name for p in tmp_path.iterdir()) == [path.name]


def test_write_content_waits_for_a_reader(tmp_path, monkeypatch):
    monkeypatch.setenv(LOCAL_DIR_ENV, str(tmp_path / "locks"))
    path = tmp_path / "data" / "a.json"
    path.parent.mkdir()
    path.write_text("old")
    written = threading.Event()

    def writer():
        write_content(path, "new", "overwrite")
        written.set()

    with File(path, "r"):
        thread = threading.Thread(target=writer)
        thread.start()
        # a roast reads the file: it is not replaced under it
        assert not written.wait(0.5)
        assert path.read_text() == "old"
    thread.join(60)
    assert written.is_set() and path.read_text() == "new"
    assert sorted(p.name for p in path.parent.iterdir()) == ["a.json"]


def test_dry_run_nitrc_without_credentials(host, no_credentials, tmp_path):
    from brainspresso.sources.nitrc import NITRC
    source = NITRC({"raw": [f"{host['url']}/downloads/ABIDEII-BNI_1.csv"]})
    source.harvest(tmp_path, options=DownloadOptions(dry_run=True))
    assert not any(tmp_path.iterdir())


def test_abide2_s3_sidecars_filtered_by_site():
    from types import SimpleNamespace

    from brainspresso.datasets.indi.abide.ii.harvest import S3
    s3 = S3()
    s3.sites = frozenset({"ETH"})

    def keep(path):
        entry = SimpleNamespace(relpath=Path(path))
        return s3.keep(entry, None, None)

    assert keep("sidecards/ABIDEII-ETHZ_1/task-rest_bold.json")
    assert keep("sidecards/ABIDEII-ETHZ_1_task-rest_bold.json")
    assert not keep("sidecards/ABIDEII-BNI_1/task-rest_bold.json")
    assert not keep("sidecards/ABIDEII-BNI_1_T1w.json")
    assert keep("sidecards/task-rest_bold.json")      # site unknown


def test_xnat_reauth_is_not_an_attempt():
    from brainspresso.sources.credentials import XnatAuth
    from brainspresso.sources.xnat import XNATClient
    state = {"logged": False}

    async def login(request):
        state["logged"] = True
        return web.Response(text="session")

    async def data(request):
        if not state["logged"]:
            return web.Response(status=401)
        return web.json_response({"ok": 1})

    app = web.Application()
    app.router.add_post("/data/JSESSION", login)
    app.router.add_get("/data/thing", data)
    url, stop = serve(app)

    async def run():
        client = XNATClient(url, max_attempts=1)
        async with client:
            client.auth = XnatAuth(url, "u", "p")   # log in on the 401
            return await client.get_json("thing")
    try:
        assert asyncio.run(run()) == {"ok": 1}
    finally:
        stop()


def test_dataverse_verifies_tls(monkeypatch):
    from brainspresso.sources.dataverse import Dataverse
    monkeypatch.setenv("DATAVERSE_TOKEN", "t")
    source = Dataverse({"raw": {"a.txt": "doi:10.0/X"}})
    assert "insecure" not in source.download_options(DownloadOptions())
