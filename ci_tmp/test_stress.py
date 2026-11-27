"""TEMPORARY: the dropped-connection tests, 50 times each, in parallel."""
import pytest
import test_download_new as new
import test_download_old as old

NAMES = ["test_restart_when_remote_changed",
         "test_resume_after_dropped_connection"]


@pytest.fixture(scope="module")
def servers():
    gens = [old.server.__wrapped__(), new.server.__wrapped__()]
    srvs = [next(g) for g in gens]
    yield dict(zip(("old", "new"), srvs))
    for g in gens:
        next(g, None)


@pytest.mark.parametrize("i", range(50))
@pytest.mark.parametrize("name", NAMES)
@pytest.mark.parametrize("version", ["old", "new"])
def test_loop(servers, tmp_path, monkeypatch, version, name, i):
    srv = servers[version]
    srv.reset()
    module = old if version == "old" else new
    getattr(module, name)(srv, tmp_path, monkeypatch)
