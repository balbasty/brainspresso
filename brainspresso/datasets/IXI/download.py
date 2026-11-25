from pathlib import Path
from urllib.parse import urlparse
from humanize import naturalsize
from typing import Annotated, Literal, Iterable

import aiohttp
from cyclopts import Parameter

from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.ui import human2bytes
from brainspresso.utils.io import write_json
from brainspresso.download import Downloader
from brainspresso.download import DownloadManager
from brainspresso.download import CHUNK_SIZE
from brainspresso.download import IfExists
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

KeyChoice = Literal["T1w", "T2w", "PDw", "angio", "dwi", "meta"]
GroupLogin = Parameter(group="nitrc login (optional)")


@ixi.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = tuple(URLS.keys()),
    subs: Iterable[int] = (),
    exclude_subs: Iterable[int] = (),
    if_exists: IfExists.Choice = "skip",
    source: Literal["nitrc", "imperial"] = "nitrc",
    user: Annotated[str | None, GroupLogin] = None,
    password: Annotated[str | None, GroupLogin] = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = None,
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
    log : str
        Path to log file
    """
    setup_filelog(log)
    if isinstance(keys, str):
        keys = [keys]
    keys = list({key: None for key in keys or URLS}.keys())
    root: Path = Path(get_tree_path(path)) / 'IXI'
    src = root / 'sourcedata'
    src.mkdir(parents=True, exist_ok=True)
    chunk_size = human2bytes(packet)

    if source == "imperial":
        downloaders = (
            Downloader(
                url, src / Path(urlparse(url).path).name,
                ifexists=if_exists, chunk_size=chunk_size,
            )
            for key in keys
            for url in URLS[key]
        )
    else:
        downloaders = nitrc_downloaders(
            src,
            keys=set(keys),
            subs={int(s) for s in ([subs] if isinstance(subs, int) else subs)},
            exclude_subs={
                int(s) for s in (
                    [exclude_subs] if isinstance(exclude_subs, int)
                    else exclude_subs
                )
            },
            auth=aiohttp.BasicAuth(user, password or "") if user else None,
            ifexists=if_exists,
            chunk_size=chunk_size,
        )

    DownloadManager(
        downloaders, ifexists=if_exists, jobs=jobs, path="short",
    ).run()


def experiment_subject(label: str) -> int | None:
    """'12_HH' -> 12"""
    head = label.split('_')[0]
    return int(head) if head.isdigit() else None


def nitrc_downloaders(
    src: Path,
    *,
    keys: set[str],
    subs: set[int] = frozenset(),
    exclude_subs: set[int] = frozenset(),
    auth: aiohttp.BasicAuth | None = None,
    ifexists: IfExists.Choice = "skip",
    chunk_size: int = CHUNK_SIZE,
    server: str = NITRC_SERVER,
    project: str = NITRC_PROJECT,
):
    """Yield downloaders for the IXI files hosted on NITRC-IR"""

    def keep_experiment(exp: dict) -> bool:
        sub = experiment_subject(exp.get("label", ""))
        if sub is None:
            return False
        if subs and sub not in subs:
            return False
        return sub not in exclude_subs

    experiments, files = list_project_files(
        server, project, keep_experiment, auth=auth,
    )
    # Session metadata (scan date) for reference
    write_json(
        {e["label"]: {"ID": e["ID"], "date": e.get("date")}
         for e in experiments},
        src / 'nitrc_experiments.json',
    )

    # Subject prefix (IXI###-Site-####) of each experiment, from its
    # image file names, used to name its b-values/vectors.
    prefixes = {}
    for f in files:
        if f.collection == 'NIfTI' and f.name.startswith('IXI'):
            prefixes.setdefault(f.experiment, '-'.join(f.name.split('-')[:3]))

    for f in files:
        key = SCAN2KEY.get(f.scan)
        if key not in keys:
            continue
        folder = src / f'IXI-{f.scan}'
        if f.collection == 'NIfTI':
            dst = folder / f.name
        elif f.collection in ('bval', 'bvec'):
            prefix = prefixes.get(f.experiment, f.experiment)
            dst = folder / f'{prefix}-{f.collection}s.txt'
        else:       # SNAPSHOTS (QC thumbnails)
            continue
        yield Downloader(
            f.url, dst, size=f.size, auth=_session_auth(auth),
            ifexists=ifexists, chunk_size=chunk_size,
        )


def _session_auth(auth: aiohttp.BasicAuth | None):
    """Downloader auth hook: send basic credentials on 401/403"""
    if auth is None:
        return None

    async def login(session: aiohttp.ClientSession) -> None:
        async with session.get(
            f'{NITRC_SERVER}/data/JSESSION', auth=auth
        ) as r:
            await r.read()

    return login
