
from brainspresso.sources.base import LocalSource, StaticURLs


def sources(urls=()) -> dict:
    """
    Sources of ADNI-1: the data are under a data use agreement. They are
    either downloaded from URLs that the user got from the LONI portal
    (`urls`), or put in `sourcedata/` by the user (`local`).
    """
    return {
        "urls": StaticURLs({"raw": list(urls)}, name="urls"),
        "local": LocalSource(provides={"raw", "meta"}),
    }


