from pathlib import Path
from typing import Iterable
from humanize import naturalsize
from logging import getLogger
from warnings import filterwarnings
from urllib3.exceptions import InsecureRequestWarning

from braindataprep.utils.ui import human2bytes
from braindataprep.utils.path import get_tree_path
from braindataprep.utils.log import setup_filelog
from braindataprep.utils.keys import compat_keys
from braindataprep.download import DownloadManager
from braindataprep.download import Downloader
from braindataprep.download import IfExists
from braindataprep.download import CHUNK_SIZE
from braindataprep.sources.xnat import XNAT
from braindataprep.sources.nitrc import nitrc_authentifier
from braindataprep.datasets.ABIDE.I.command import abide1
from braindataprep.datasets.ABIDE.I.keys import allkeys

lg = getLogger(__name__)

SITES = [
    'Caltech',
    'CMU',
    'KKI',
    'Leuven-1',
    'Leuven-2',
    'MaxMun',
    'NYU',
    'OHSU',
    'Olin',
    'Pitt',
    'SBL',
    'SDSU',
    'Stanford',
    'Trinity',
    'UCLA1',
    'UCLA2',
    'UM1',
    'UM2',
    'USM',
    'Yale',
]

URL_META = 'https://fcp_private.projects.nitrc.org/downloads'
URL_META += '/abide_mrdata_r01_release/PhenotypicData/Phenotypic_V1_0b.csv'
URL_PARAM = 'https://fcon_1000.projects.nitrc.org/indi/abide/scan_params'
URLS = [URL_META]
for site in SITES:
    URLS.extend([
        f'{URL_PARAM}/{site}/anat.pdf',
        f'{URL_PARAM}/{site}/rest.pdf',
    ])


@abide1.command
def download_xnat(
    path: str | None = None,
    *,
    keys: Iterable[str] = "all",
    subs: Iterable[int | str] | None = tuple(),
    exclude_subs: Iterable[int | str] | None = tuple(),
    if_exists: IfExists.Choice = "skip",
    user: str | None = None,
    password: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = 1,
    log: str | None = None,
):
    """
    Download source data for the ABIDE-I dataset.

    **Key hierarchy:**

    * all
      * meta                Metadata
      * raw                 All raw data
        * anat             Anatomical T1w scans
        * rest             Resting-state fMRI
      * derivatives         All derivatives
        * proc             All processed data
          * proc-min      Minimally processed data
          * ants          ANTs
          * ccs           Connectome Computation System
          * civet         Civet
          * cpac          Configurable Pipeline for the Analysis of Connectomes
          * dparsf        Data Processing Assistant for Resting-State fMRI
          * fs            FreeSurfer
          * niak          NeuroImaging Analysis Kit
        * qa               All quality assesment
          * qa-man        Manual QA
          * qa-pcp        Preprocessed Connectomes Project

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `ABIDE-1` folder will be created.
    keys : [list of] str
        Data categories to download
    subs : [list of] int
        Only bidsify these subjects (all if empty)
    exclude_subs : [list of] int
        Do not bidsify these subjects
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour if a file already exists
    user : str
        NITRC username
    password : str
        NITRC password
    packet : int
        Packet size to download, in bytes
    log : str
        Path to log file

    """  # noqa: E501
    setup_filelog(log)
    packet = human2bytes(packet)
    path = Path(get_tree_path(path))
    keys = set(keys)
    src = path / 'ABIDE-1' / 'sourcedata'

    xnat = XNAT(user, password, open=True)

    assessor_alias = {
        'man_qa': 'qa-man',
        'pcp_qa': 'qa-pcp',
        'min_proc': 'proc-min'
    }

    # Format subjects
    def expand_sub_range(subs):
        for i, sub in enumerate(subs):
            if isinstance(sub, str) and ':' in sub:
                sub = sub.split(':')
                start, stop = sub[0], sub[1]
                step = sub[2] if len(sub) > 2 else ''
                step = int(step) if step else 1
                if step < 1:
                    raise ValueError('Subject range: step must be positive')
                start = int(start) if start else 0
                stop = int(stop) if stop else None
                if stop is None:
                    raise ValueError('Subject range: Stop must be provided')
                subs = subs[:i] + list(range(start, stop, step)) + subs[i+1:]
        return subs

    if isinstance(subs, (int, str)):
        subs = [subs]
    subs = list(subs or [])
    subs = expand_sub_range(subs)

    if isinstance(exclude_subs, int):
        exclude_subs = [exclude_subs]
    exclude_subs = list(set(exclude_subs or []))
    exclude_subs = set(expand_sub_range(exclude_subs))

    # Get subject IDs
    submap = xnat.get_subjects('ABIDE')
    submap = {
        int(sub.split('/')[-1].split('_')[-1]): sub.split('/')[-1]
        for sub in submap
    }
    if not subs:
        subs = list(submap.keys())
    elif subs and isinstance(subs[0], str):
        # Might be a file that contains subject IDs
        tmp, subs = subs, []
        for sub in tmp:
            if Path(sub).exists():
                with open(sub, 'rt') as f:
                    for line in f:
                        subs.append(int(line))
            else:
                subs.append(int(sub))
    subs = set(subs) - exclude_subs

    # Accumulate downloaders
    def _all_downloaders():
        opt = dict(chunk_size=human2bytes(packet), ifexists=if_exists)

        # Get downloaders for metadata
        if (keys & compat_keys("meta", allkeys)):
            urls = iter(URLS)
            yield Downloader(
                next(urls), src / 'Phenotypic_V1_0b.csv',
                ifexists=if_exists,
                chunk_size=packet,
                auth=nitrc_authentifier(user, password),
                get_opt=dict(verify=False)
            )
            for url in urls:
                yield Downloader(
                    url, src / '/'.join(url.split('/')[-2:]),
                    ifexists=if_exists,
                    chunk_size=packet,
                    auth=nitrc_authentifier(user, password),
                    get_opt=dict(verify=False)
                )

        # Get downloaders for image data
        for sub in subs:
            scans = xnat.get_scans('ABIDE', submap[sub], submap[sub])
            for scan in scans:
                # filter on scan type (maybe not robust enough?)
                if not (keys & compat_keys(scan, allkeys)):
                    continue
                fname = src / submap[sub] / f'{scan}.tar.gz'
                yield xnat.get_downloader(
                    'ABIDE', submap[sub], submap[sub], scan, fname,
                    **opt)

            # derivatives
            assessors = xnat.get_all_assessors(
                'ABIDE', submap[sub], submap[sub],
            )
            assessors = {
                x.split('-')[-1]: x.split('/')[-1]
                for x in assessors
            }

            for assessor_key, assessor in assessors.items():
                key_alias = assessor_alias.get(assessor_key, assessor_key)
                if keys & compat_keys(key_alias, allkeys):
                    fname = src / submap[sub] / f'{assessor}.tar.gz'
                    yield xnat.get_downloader(
                        'ABIDE', submap[sub], submap[sub], assessor, fname,
                        type='assessor', **opt
                    )

    def all_downloaders():
        # Fix authentifier (use async)
        print("hello")
        for dl in _all_downloaders():
            print(dl)
            dl.auth = xnat.async_auth
            yield dl

    # Download all
    filterwarnings('ignore', category=InsecureRequestWarning)
    DownloadManager(
        all_downloaders(),
        ifexists=if_exists,
        path='full',
        jobs=jobs,
    ).run()
    xnat.close()
