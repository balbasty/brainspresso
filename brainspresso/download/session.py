"""Creation of aiohttp sessions with brainspresso defaults."""
__all__ = ["new_session"]
import aiohttp

from brainspresso.download.constants import TIMEOUT


def new_session(**kwargs) -> aiohttp.ClientSession:
    """
    Create an aiohttp session with brainspresso's defaults.

    * `timeout=TIMEOUT`: no limit on the total duration of a download,
      only on connection and stalls.
    * `trust_env=True`: honour the `HTTP(S)_PROXY` / `NO_PROXY`
      environment variables (and `~/.netrc`), like `requests` and `curl`
      do. aiohttp ignores them by default, which makes every download
      fail on networks that require a proxy.
    """
    kwargs.setdefault("timeout", TIMEOUT)
    kwargs.setdefault("trust_env", True)
    return aiohttp.ClientSession(**kwargs)
