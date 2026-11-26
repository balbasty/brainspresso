import json
from pathlib import Path
from humanize import naturalsize
from typing import Annotated, Literal, Iterable

import aiohttp
from cyclopts import Parameter

from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import LayoutChoice, resolve_layout
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.ui import human2bytes
from brainspresso.download import CHUNK_SIZE
from brainspresso.download import IfExists
from brainspresso.sources.base import (
    DownloadOptions, LabelMap, LocalSource, RemoteEntry, Source,
    SourceUnavailable, StaticURLs, Wants, configure_sources,
    harvest_sources, subject_selection,
)
from brainspresso.sources.credentials import XnatAuth
from brainspresso.sources.xnat_listing import list_project_files
from brainspresso.datasets.IXI.command import ixi

# ----------------------------------------------------------------------
#   NITRC-IR (XNAT project "ixi"), the current host
# ----------------------------------------------------------------------
NITRC_SERVER = 'https://www.nitrc.org/ir'
NITRC_PROJECT = 'ixi'

# XNAT scan ID -> brainspresso key
SCAN2KEY = {
    'T1': 'T1w', 'T2': 'T2w', 'PD': 'PDw', 'MRA': 'angio', 'DTI': 'dwi',
}

# ----------------------------------------------------------------------
#   Original host (Imperial College), now offline
# ----------------------------------------------------------------------
URLBASE = 'https://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI'
URLS = {
    'meta': [f'{URLBASE}/IXI.xls'],
    'T1w': [f'{URLBASE}/IXI-T1.tar'],
    'T2w': [f'{URLBASE}/IXI-T2.tar'],
    'PDw': [f'{URLBASE}/IXI-PD.tar'],
    'angio': [f'{URLBASE}/IXI-MRA.tar'],
    'dwi': [
        f'{URLBASE}/IXI-DTI.tar',
        f'{URLBASE}/bvecs.txt',
        f'{URLBASE}/bvals.txt',
    ],
}
IMAGES = ("T1w", "T2w", "PDw", "angio", "dwi")

KeyChoice = Literal["T1w", "T2w", "PDw", "angio", "dwi", "meta"]
GroupLogin = Parameter(group="nitrc login (optional)")


def experiment_subject(label: str) -> int | None:
    """'12_HH' -> 12"""
    head = label.split('_')[0]
    return int(head) if head.isdigit() else None


def _label(experiment: str) -> str | None:
    sub = experiment_subject(experiment)
    return None if sub is None else f"{sub:03d}"


def roast_ixi(ctx):
    """Roaster of the IXI sources (imported on use: needs xlrd)"""
    from brainspresso.datasets.IXI.roaster import roast_ixi
    return roast_ixi(ctx)


class NitrcIR(Source):
    """
    IXI images from NITRC-IR (XNAT project "ixi", no login needed), in
    `sourcedata/IXI-{T1,T2,PD,MRA,DTI}/`, with the file names of the
    original tarballs, and the b-values/vectors of each subject as
    `IXI-DTI/<IXI###-Site-####>-{bvals,bvecs}.txt`. The experiments
    (scan dates) are saved in `sourcedata/nitrc_experiments.json`.

    Per-run options: `user`, `password` (only if anonymous access is
    refused).
    """

    name = "nitrc"
    provides = frozenset(IMAGES)
    labels = LabelMap(_label)
    roaster = staticmethod(roast_ixi)
    server: str = NITRC_SERVER
    project: str = NITRC_PROJECT
    user: str | None = None
    password: str | None = None

    def __init__(self, server: str | None = None,
                 project: str | None = None, **kwargs):
        super().__init__(**kwargs)
        if server is not None:
            self.server = server
        if project is not None:
            self.project = project

    def auth(self):
        if not self.user:
            return None
        return XnatAuth(self.server, self.user, self.password or "",
                        password_required=False)

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
        wants = keys if callable(keys) else Wants(keys)
        basic = aiohttp.BasicAuth(self.user, self.password or "") \
            if self.user else None
        try:
            experiments, files = list_project_files(
                self.server, self.project,
                lambda exp: self.keeps_subject(selection,
                                               exp.get("label", "")),
                auth=basic,
            )
        except aiohttp.ClientResponseError as e:
            if e.status in (401, 403):
                raise PermissionError(
                    f"{self.server}: access denied ({e.status}); give "
                    f"credentials with `--user`/`--password`"
                ) from e
            raise SourceUnavailable(f"{self.server}: {e}") from e
        except (aiohttp.ClientConnectionError, OSError) as e:
            raise SourceUnavailable(f"{self.server}: {e}") from e
        # Session metadata (scan date) for reference
        out = [RemoteEntry("nitrc_experiments.json", meta={
            "generated": True,
            "content": json.dumps(
                {e["label"]: {"ID": e["ID"], "date": e.get("date")}
                 for e in experiments}, indent=2),
        })]

        # Subject prefix (IXI###-Site-####) of each experiment, from its
        # image file names, used to name its b-values/vectors.
        prefixes = {}
        for f in files:
            m = f.meta
            if m["collection"] == 'NIfTI' and f.name.startswith('IXI'):
                prefixes.setdefault(m["experiment"],
                                    '-'.join(f.name.split('-')[:3]))

        for f in files:
            m = f.meta
            key = SCAN2KEY.get(m["scan"])
            if key is None or not wants(key):
                continue
            folder = f"IXI-{m['scan']}"
            if m["collection"] == 'NIfTI':
                local = f"{folder}/{f.name}"
            elif m["collection"] in ('bval', 'bvec'):
                prefix = prefixes.get(m["experiment"], m["experiment"])
                local = f"{folder}/{prefix}-{m['collection']}s.txt"
            else:       # SNAPSHOTS (QC thumbnails)
                continue
            f.meta["local"] = local
            out.append(f)
        return out

    def local_path(self, entry: RemoteEntry) -> str:
        return entry.meta.get("local", entry.path)


class Imperial(StaticURLs):
    """Original IXI tarballs and `IXI.xls` (brain-development.org;
    offline)"""

    name = "imperial"
    urls = URLS
    roaster = staticmethod(roast_ixi)


def sources() -> dict:
    """Sources of IXI"""
    return {
        "nitrc": NitrcIR(),
        "imperial": Imperial(),
        # IXI.xls shipped with brainspresso (or put in sourcedata/)
        "bundled": LocalSource(provides={"meta"}, name="bundled"),
    }


DEFAULTS = {**{key: ["nitrc", "imperial"] for key in IMAGES},
            "meta": ["bundled"]}


@ixi.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = tuple(URLS.keys()),
    subs: Iterable[int] = (),
    exclude_subs: Iterable[int] = (),
    if_exists: IfExists.Choice = "skip",
    source: Literal["nitrc", "imperial"] | None = None,
    user: Annotated[str | None, GroupLogin] = None,
    password: Annotated[str | None, GroupLogin] = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = None,
    layout: LayoutChoice | None = None,
    log: str | None = None,
) -> None:
    """
    Download source data for the IXI dataset.

    With the default source (NITRC-IR), images are saved under
    `IXI/sourcedata/IXI-{T1,T2,PD,MRA,DTI}/`, with the same file names
    as in the original tarballs, and the b-values/vectors of each
    subject as `IXI-DTI/<IXI###-Site-####>-{bvals,bvecs}.txt`.
    NITRC-IR does not host `IXI.xls` (demographics): `roast` uses the
    copy shipped with brainspresso if there is none in `sourcedata/`.

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `IXI` folder will be created.
    keys : [list of] {"T1w", "T2w", "PDw", "angio", "dwi", "meta"}
        Modalities to download
    subs : [list of] int
        Only download these subjects (nitrc only; all if empty)
    exclude_subs : [list of] int
        Do not download these subjects (nitrc only)
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour if a file already exists
    source : {"nitrc", "imperial"}
        Default: nitrc for images (imperial if nitrc is unavailable),
        the bundled `IXI.xls` for metadata.

        * nitrc: NITRC-IR, project "ixi" (no login needed).
        * imperial: original tarballs (brain-development.org; offline).
    user : str
        NITRC username (only if anonymous access is refused)
    password : str
        NITRC password (only if anonymous access is refused)
    packet : int
        Packet size to download, in bytes
    jobs : int
        Number of parallel downloads
    layout : {"study", "flat"}
        Layout of the dataset (where source data does not move, but
        later commands reuse it): "study" (raw dataset in
        `rawbids/`) or "flat" (raw dataset at the root). Default:
        the layout of an existing dataset, else "study".
    log : str
        Path to log file
    """
    setup_filelog(log)
    if isinstance(keys, str):
        keys = [keys]
    keys = list({key: None for key in keys or URLS}.keys())
    root: Path = Path(get_tree_path(path)) / 'IXI'
    dataset = resolve_layout(root, layout)
    record_run(dataset, "harvest", {"dataset": dataset.root.name})
    Path(dataset.source).mkdir(parents=True, exist_ok=True)
    harvest_sources(
        configure_sources(sources(), user=user, password=password),
        DEFAULTS, dataset,
        selection=subject_selection(subs, exclude_subs, width=3),
        keys=Wants(keys),
        options=DownloadOptions(ifexists=if_exists, jobs=jobs,
                                chunk_size=human2bytes(packet)),
        override=source,
    )


def nitrc_downloaders(
    src: Path,
    *,
    keys: set[str],
    subs: set[int] = frozenset(),
    exclude_subs: set[int] = frozenset(),
    user: str | None = None,
    password: str | None = None,
    ifexists: IfExists.Choice = "skip",
    chunk_size: int = CHUNK_SIZE,
    server: str = NITRC_SERVER,
    project: str = NITRC_PROJECT,
):
    """Downloaders of the IXI files hosted on NITRC-IR, into the source
    folder `src`"""
    source = NitrcIR(server, project).configure(user=user,
                                                password=password)
    options = DownloadOptions(ifexists=ifexists, chunk_size=chunk_size)
    entries = source.list(subject_selection(subs, exclude_subs, width=3),
                          Wants(keys))
    return source.downloaders(entries, src, options)
