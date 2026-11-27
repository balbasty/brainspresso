"""`bx adni1`: ADNI-1 (not in the catalog yet)."""
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

from brainspresso.datasets.adni.i.harvest import sources
from brainspresso.datasets.base import Dataset
from brainspresso.datasets.options import OptionGroup, option_group

HELP = """
ADNI-1 dataset (Alzheimer's Disease)

* **Project**       Alzheimer's Disease Neuroimaging Initiative (ADNI)
* **Subproject**    ADNI-1
* **Modalities**    T1w, DTI, PET
* **Populations**   Controls, Alzheimer
* **URL**           https://adni.loni.usc.edu
"""  # noqa: E501


@option_group("ADNI-1", 50)
class URLOptions(OptionGroup):
    """`--urls`."""

    urls: Annotated[tuple[str, ...], Parameter(
        help="URL(s) to download (from the LONI portal); a file lists one "
        "per line", consume_multiple=True,
    )] = ()

    def all_urls(self) -> list[str]:
        """URLs, with those of the files read."""
        out = []
        for url in self.urls:
            if Path(url).exists():
                # ("utf-8-sig": a BOM is not part of the first URL)
                text = Path(url).read_text(encoding="utf-8-sig")
                out += [x.strip() for x in text.splitlines()
                        if x.strip()]
            else:
                out.append(url)
        return out


class ADNI1(Dataset):
    """ADNI-1: URLs of the LONI portal, or files put in `sourcedata/`."""

    name = "adni1"
    folder = "ADNI-1"
    title = "ADNI-1"
    help = HELP
    harvest_subjects = False
    harvest_jobs = 1

    def options(self, command):
        """`--urls` (harvest)."""
        return {"urls": URLOptions} if command == "harvest" else {}

    def sources(self, args=None):
        """`urls` (from `--urls`), `local`."""
        urls = args.extra("urls").all_urls() if args is not None and \
            args.extra("urls") is not None else ()
        return sources(urls)
