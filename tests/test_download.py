"""
Tests for the download engine, against a local HTTP server that can
inject faults (dropped connections, 5xx, ignored Range, auth...).
"""
import asyncio
import errno
import hashlib
import socket
import threading
import time
from collections import Counter

import aiohttp
import pytest
from aiohttp import web

from brainspresso.actions import locks
from brainspresso.actions.locks import FileLock
from brainspresso.download import Downloader, DownloadManager, incomplete
from brainspresso.download.constants import TIMEOUT
from brainspresso.download.incomplete import IncompleteFile

SIZE = 3 * 2**20 + 123          # not a multiple of the chunk size


def content(name: str, version: int = 0, size: int = SIZE) -> bytes:
    """Deterministic, non-trivial content for a file"""
    seed = hashlib.sha256(f"{name}:{version}".encode()).digest()
    return (seed * (size // len(seed) + 1))[:size]


# ----------------------------------------------------------------------
#   Fault-injecting server
# ----------------------------------------------------------------------

class Server:
    """
    Behaviour of each file is controlled through `self.rules[name]`:
    - "drop_after": int      close the connection after N body bytes
                             (only for the first `drop_times` GETs)
    - "drop_when": Event     ... once this event is set (`appended`: the
                             client wrote a chunk), then clear it
    - "fail_status": int     answer this status for the first
                             `fail_times` GETs
    - "retry_after": str     Retry-After header sent with failures
    - "no_range": bool       ignore Range headers
    - "version": int         content version (changes ETag)
    - "auth": bool           require cookie `auth=ok` (401 otherwise)
    - "rate": float          bytes per second
    """

    def __init__(self):
        self.rules: dict[str, dict] = {}
        self.counts: Counter = Counter()
        self.gets: Counter = Counter()
        self.ranges: list[tuple[str, str]] = []
        self.active = 0
        self.max_active = 0

    def reset(self):
        self.rules.clear()
        self.counts.clear()
        self.gets.clear()
        self.ranges.clear()
        self.active = self.max_active = 0

    async def login(self, request):
        resp = web.Response(text="ok")
        resp.set_cookie("auth", "ok")
        return resp

    async def handler(self, request):
        name = request.match_info["name"]
        rule = self.rules.get(name, {})
        self.counts[request.method] += 1
        if rule.get("missing"):
            raise web.HTTPNotFound()
        if rule.get("auth") and request.cookies.get("auth") != "ok":
            raise web.HTTPUnauthorized()
        if request.method == "GET":
            self.gets[name] += 1
        version = rule.get("version", 0)
        data = content(name, version)
        etag = f'"{name}-v{version}"'
        headers = {
            "Accept-Ranges": "bytes",
            "ETag": etag,
            "Last-Modified": "Wed, 01 Jan 2020 00:00:00 GMT",
        }
        start, status = 0, 200
        rng = request.headers.get("Range")
        if_range = request.headers.get("If-Range")
        if rng and not rule.get("no_range") and (
            if_range is None or if_range == etag
        ):
            self.ranges.append((name, rng))
            start = int(rng.split("=")[1].split("-")[0])
            if start >= len(data):
                return web.Response(
                    status=416,
                    headers={"Content-Range": f"bytes */{len(data)}"},
                )
            status = 206
            headers["Content-Range"] = (
                f"bytes {start}-{len(data)-1}/{len(data)}"
            )
        body = data[start:]
        headers["Content-Length"] = str(len(body))
        if request.method == "HEAD":
            return web.Response(status=status, headers=headers)

        resp = web.StreamResponse(status=status, headers=headers)
        await resp.prepare(request)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            drop = rule.get("drop_after")
            if (
                drop is not None and
                self.gets[name] > rule.get("drop_times", 1)
            ):
                drop = None
            rate = rule.get("rate")
            sent, block = 0, 64 * 1024
            t0 = time.monotonic()
            while sent < len(body):
                k = min(block, len(body) - sent)
                if drop is not None and sent + k > drop:
                    await resp.write(body[sent:drop])
                    event = rule.get("drop_when")
                    if event is not None:
                        # aiohttp raises the error of a dropped
                        # connection before it hands over the bytes it
                        # has buffered: without this, a client that is
                        # slow to start reading gets none of them
                        await asyncio.get_running_loop().run_in_executor(
                            None, event.wait, 30)
                        event.clear()
                    request.transport.close()
                    return resp
                await resp.write(body[sent:sent+k])
                sent += k
                if rate:
                    delay = t0 + sent / rate - time.monotonic()
                    if delay > 0:
                        await asyncio.sleep(delay)
        finally:
            self.active -= 1
        return resp


async def _failing(server: Server, request):
    """Wrap handler to inject HTTP failures"""
    name = request.match_info["name"]
    rule = server.rules.get(name, {})
    if request.method == "GET":
        n = server.gets[name] + 1
        if n <= rule.get("fail_times", 0):
            server.gets[name] += 1
            server.counts["GET"] += 1
            headers = {}
            if "retry_after" in rule:
                headers["Retry-After"] = rule["retry_after"]
            return web.Response(
                status=rule.get("fail_status", 503), headers=headers,
                text="<html>error page</html>",
            )
    return await server.handler(request)


@pytest.fixture(scope="module")
def server():
    srv = Server()
    app = web.Application()
    app.router.add_post("/login", srv.login)

    async def route(request):
        return await _failing(srv, request)

    app.router.add_route("*", "/{name}", route)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    loop = asyncio.new_event_loop()
    runner = web.AppRunner(app)
    started = threading.Event()

    def serve():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(runner.setup())
        site = web.SockSite(runner, sock)
        loop.run_until_complete(site.start())
        started.set()
        loop.run_forever()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    started.wait(10)
    # (aiohttp's cookie jar ignores cookies from bare IP addresses)
    srv.url = f"http://localhost:{port}"
    yield srv
    loop.call_soon_threadsafe(loop.stop)
    thread.join(5)


@pytest.fixture
def srv(server):
    server.reset()
    return server


def run(downloader, **kwargs):
    kwargs.setdefault("ifexists", "overwrite")
    DownloadManager([downloader], on_error="raise", **kwargs).run()


async def collect(downloader):
    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
        downloader.session = session
        return [status async for status in downloader]


def appended(monkeypatch) -> threading.Event:
    """An event set each time a download writes a chunk."""
    from brainspresso.download.incomplete import IncompleteFile
    event, append = threading.Event(), IncompleteFile.append

    async def spy(self, blob):
        out = await append(self, blob)
        event.set()
        return out

    monkeypatch.setattr(IncompleteFile, "append", spy)
    return event


def fast_retries(monkeypatch):
    import brainspresso.download.downloader as dl
    monkeypatch.setattr(dl, "_retry_delay", lambda exc, attempt: 0.01)


# ----------------------------------------------------------------------
#   Tests
# ----------------------------------------------------------------------

def test_no_total_timeout():
    # aiohttp's default (total=300s) kills any download > 5 minutes
    assert TIMEOUT.total is None
    assert TIMEOUT.sock_read


def test_downloads_run_concurrently(srv, tmp_path):
    for i in range(6):
        srv.rules[f"f{i}"] = {"rate": 4 * 2**20}
    DownloadManager(
        [Downloader(f"{srv.url}/f{i}", tmp_path / f"f{i}") for i in range(6)],
        jobs=6, ifexists="overwrite", on_error="raise",
    ).run()
    assert srv.max_active == 6
    assert srv.counts["HEAD"] == 0      # no pre-flight HEAD requests
    for i in range(6):
        assert (tmp_path / f"f{i}").read_bytes() == content(f"f{i}")


def test_jobs_limit(srv, tmp_path):
    for i in range(6):
        srv.rules[f"g{i}"] = {"rate": 8 * 2**20}
    DownloadManager(
        (Downloader(f"{srv.url}/g{i}", tmp_path / f"g{i}") for i in range(6)),
        jobs=2, ifexists="overwrite", on_error="raise",
    ).run()
    assert srv.max_active == 2
    assert len(list(tmp_path.iterdir())) == 6


def test_resume_after_dropped_connection(srv, tmp_path, monkeypatch):
    fast_retries(monkeypatch)
    srv.rules["a"] = {
        "drop_after": 2**20, "drop_times": 2, "rate": 16 * 2**20,
        "drop_when": appended(monkeypatch),
    }
    statuses = asyncio.run(collect(
        Downloader(f"{srv.url}/a", tmp_path / "a", ifexists="overwrite")
    ))
    assert statuses[-1]["status"] == "done"
    assert (tmp_path / "a").read_bytes() == content("a")
    # second and third GETs resumed from where the previous one stopped
    # (bytes in flight when the connection drops may be lost, so offsets
    # are slightly below the drop points)
    offsets = [
        int(r.split("=")[1].rstrip("-")) for n, r in srv.ranges if n == "a"
    ]
    assert offsets and offsets == sorted(offsets)
    assert 0 < offsets[0] <= 2**20 and offsets[-1] <= 2**21
    assert not (tmp_path / "a.download").exists()
    assert not (tmp_path / "a.lock").exists()


def test_restart_when_range_ignored(srv, tmp_path, monkeypatch):
    fast_retries(monkeypatch)
    srv.rules["b"] = {"drop_after": 2**20, "no_range": True}
    statuses = asyncio.run(collect(
        Downloader(f"{srv.url}/b", tmp_path / "b", ifexists="overwrite")
    ))
    assert statuses[-1]["status"] == "done"
    assert (tmp_path / "b").read_bytes() == content("b")


def test_restart_when_remote_changed(srv, tmp_path, monkeypatch):
    fast_retries(monkeypatch)
    # first attempt gets v0 and is interrupted
    srv.rules["c"] = {
        "drop_after": 2**20, "drop_times": 1, "version": 0,
        "rate": 16 * 2**20, "drop_when": appended(monkeypatch),
    }
    dl = Downloader(
        f"{srv.url}/c", tmp_path / "c", ifexists="overwrite", max_attempts=1
    )
    statuses = asyncio.run(collect(dl))
    assert statuses[-1]["status"] == "error"
    assert 0 < (tmp_path / "c.download").stat().st_size <= 2**20
    # remote file changes; the next run must not mix v0 and v1 bytes
    srv.rules["c"] = {"version": 1}
    statuses = asyncio.run(collect(
        Downloader(f"{srv.url}/c", tmp_path / "c", ifexists="overwrite")
    ))
    assert statuses[-1]["status"] == "done"
    assert (tmp_path / "c").read_bytes() == content("c", 1)


def test_retry_on_503_with_retry_after(srv, tmp_path, monkeypatch):
    import brainspresso.download.downloader as dl
    delays = []
    original = dl._retry_delay
    monkeypatch.setattr(
        dl, "_retry_delay",
        lambda exc, attempt: delays.append(original(exc, attempt)) or 0.01
    )
    srv.rules["d"] = {
        "fail_times": 2, "fail_status": 503, "retry_after": "7"
    }
    statuses = asyncio.run(collect(
        Downloader(f"{srv.url}/d", tmp_path / "d", ifexists="overwrite")
    ))
    assert statuses[-1]["status"] == "done"
    assert delays == [7.0, 7.0]
    assert (tmp_path / "d").read_bytes() == content("d")


def test_http_error_is_not_written(srv, tmp_path, monkeypatch):
    fast_retries(monkeypatch)
    srv.rules["e"] = {"missing": True}
    statuses = asyncio.run(collect(
        Downloader(f"{srv.url}/e", tmp_path / "e", ifexists="overwrite")
    ))
    assert statuses[-1]["status"] == "error"
    assert "404" in statuses[-1]["message"]
    assert srv.counts["GET"] == 1    # 404 is not retried
    assert not (tmp_path / "e").exists()


def test_error_after_max_attempts(srv, tmp_path, monkeypatch):
    fast_retries(monkeypatch)
    srv.rules["f"] = {"fail_times": 100, "fail_status": 502}
    statuses = asyncio.run(collect(Downloader(
        f"{srv.url}/f", tmp_path / "f", ifexists="overwrite", max_attempts=3
    )))
    assert statuses[-1]["status"] == "error"
    assert srv.gets["f"] == 3
    assert not (tmp_path / "f").exists()


def test_auth_on_401(srv, tmp_path):
    srv.rules["g"] = {"auth": True}
    calls = []

    async def auth(session):
        calls.append(1)
        async with session.post(f"{srv.url}/login") as r:
            await r.read()

    statuses = asyncio.run(collect(Downloader(
        f"{srv.url}/g", tmp_path / "g", ifexists="overwrite", auth=auth
    )))
    assert statuses[-1]["status"] == "done"
    assert calls == [1]
    assert (tmp_path / "g").read_bytes() == content("g")


def test_digest_ok_and_mismatch(srv, tmp_path):
    md5 = hashlib.md5(content("h")).hexdigest()
    statuses = asyncio.run(collect(Downloader(
        f"{srv.url}/h", tmp_path / "h", ifexists="overwrite",
        digests={"md5": md5},
    )))
    assert {"checksum": "ok"} in statuses
    assert statuses[-1]["status"] == "done"

    statuses = asyncio.run(collect(Downloader(
        f"{srv.url}/h", tmp_path / "h2", ifexists="overwrite",
        digests={"md5": "0" * 32},
    )))
    assert statuses[-1]["status"] == "error"
    assert statuses[-1]["checksum"] == "differs"
    assert not (tmp_path / "h2").exists()
    assert not (tmp_path / "h2.download").exists()


def test_skip_existing_does_not_touch_network(srv, tmp_path):
    (tmp_path / "i").write_bytes(b"local")
    DownloadManager(
        [Downloader(f"{srv.url}/i", tmp_path / "i")],
        ifexists="skip", on_error="raise",
    ).run()
    assert srv.counts["GET"] == srv.counts["HEAD"] == 0
    assert (tmp_path / "i").read_bytes() == b"local"


def test_known_size_short_read_is_resumed(srv, tmp_path, monkeypatch):
    fast_retries(monkeypatch)
    srv.rules["j"] = {"drop_after": 1000, "drop_times": 1}
    statuses = asyncio.run(collect(Downloader(
        f"{srv.url}/j", tmp_path / "j", ifexists="overwrite", size=SIZE,
    )))
    assert statuses[-1]["status"] == "done"
    assert (tmp_path / "j").read_bytes() == content("j")


# ----------------------------------------------------------------------
#   Actions (used by `roast`)
# ----------------------------------------------------------------------

def test_action_writes_new_files(tmp_path):
    # Regressions: writing a new file with ifexists="different" raised
    # IndexError, and file-object actions (CopyJSON...) failed with
    # "'OpenedFile' object has no attribute 'close'".
    import json as _json

    from brainspresso.actions import Action, CopyJSON
    (tmp_path / "in.json").write_text('{"x": 1}')

    statuses = list(CopyJSON(tmp_path / "in.json", tmp_path / "out.json"))
    assert statuses[-1]["status"] == "done"
    assert _json.loads((tmp_path / "out.json").read_text()) == {"x": 1}

    def write(f):
        f.write(b"abc")

    out = tmp_path / "sub" / "out.bin"
    statuses = list(Action(tmp_path / "in.json", out, write))
    assert statuses[-1]["status"] == "done"
    assert out.read_bytes() == b"abc"

    # Same file again: skipped
    statuses = list(Action(tmp_path / "in.json", out, write))
    assert statuses[-1]["status"] == "skipped"


def test_sessions_honour_proxy_settings():
    # aiohttp ignores HTTP(S)_PROXY unless trust_env=True
    from brainspresso.download import new_session

    async def check():
        async with new_session() as session:
            return session.trust_env, session.timeout.total

    assert asyncio.run(check()) == (True, None)


# ----------------------------------------------------------------------
#   The download lock never leaks
# ----------------------------------------------------------------------

def assert_download_lock_free(dst):
    assert not locks._entries()
    with FileLock(dst.with_name(dst.name + ".download"), exclusive=True,
                  timeout=0):
        pass


def test_download_lock_released_after_an_open_error(tmp_path, monkeypatch):
    dst = tmp_path / "x"
    real = incomplete.aiofiles.open

    def fail(path, *args, **kwargs):
        if str(path).endswith(".download"):
            raise OSError(errno.EIO, "boom")
        return real(path, *args, **kwargs)

    monkeypatch.setattr(incomplete.aiofiles, "open", fail)

    async def go():
        async with IncompleteFile(dst):
            raise AssertionError("not reached")

    with pytest.raises(OSError, match="boom"):
        asyncio.run(go())
    assert_download_lock_free(dst)


def test_download_lock_released_when_cancelled_while_acquiring(
    tmp_path, monkeypatch,
):
    dst = tmp_path / "x"
    acquire, started = FileLock.acquire, threading.Event()

    def slow(self):
        started.set()
        time.sleep(0.5)                 # (cancelled meanwhile)
        return acquire(self)

    monkeypatch.setattr(FileLock, "acquire", slow)

    async def enter():
        async with IncompleteFile(dst):
            raise AssertionError("not reached")

    async def go():
        task = asyncio.ensure_future(enter())
        while not started.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(go())               # (waits for the executor's acquire)
    monkeypatch.undo()
    assert_download_lock_free(dst)


def test_download_lock_released_when_cancelled_after_acquiring(
    tmp_path, monkeypatch,
):
    dst = tmp_path / "x"

    async def stuck(self):
        await asyncio.sleep(60)

    monkeypatch.setattr(IncompleteFile, "_enter", stuck)

    async def go():
        task = asyncio.ensure_future(IncompleteFile(dst).__aenter__())
        await asyncio.sleep(0.3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(go())
    assert_download_lock_free(dst)


def test_download_lock_released_when_closing_fails(tmp_path):
    dst = tmp_path / "x"

    async def failing_close():
        raise OSError(errno.EIO, "close failed")

    async def go():
        async with IncompleteFile(dst) as partial:
            await partial.append(b"data")
            await partial.file.close()
            partial.file.close = failing_close

    with pytest.raises(OSError, match="close failed"):
        asyncio.run(go())
    assert_download_lock_free(dst)
