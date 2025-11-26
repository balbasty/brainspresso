# stdlib
from pathlib import Path
from typing import Iterable
from logging import getLogger
from os.path import sep

# externals
import fsspec
from humanize import naturalsize

# internals
from brainspresso.utils.io import write_json
from brainspresso.utils.ui import human2bytes
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.keys import compat_keys
from brainspresso.download import DownloadManager
from brainspresso.download import Downloader
from brainspresso.download import IfExists
from brainspresso.download import CHUNK_SIZE
from brainspresso.datasets.ADHD200.command import adhd200
from brainspresso.datasets.ADHD200.keys import allkeys

lg = getLogger(__name__)

S3URL = "https://s3.amazonaws.com"
S3PATH = "fcp-indi/data/Projects/ADHD200"
DATA = f"{S3PATH}/RawData"
PROC = f"{S3PATH}/Outputs"
SURF = f"{S3PATH}/surfaces"


@adhd200.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Iterable[str] = ("all",),
    subs: Iterable[int | str] | None = tuple(),
    exclude_subs: Iterable[int | str] | None = tuple(),
    sites: Iterable[str] | None = tuple(),
    exclude_sites: Iterable[str] | None = tuple(),
    if_exists: IfExists.Choice = "skip",
    bidsify: bool = True,
    packet: int | str = naturalsize(CHUNK_SIZE),
    log: str | None = None,
    jobs: int | None = None
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
          * denoise       Denoised
          * fmriprep      fmriPrep workflow
          * mindboggle    Mindboggle workflow
          * cpa           Configurable Pipeline for the Analysis of Connectomes
          * fs            FreeSurfer
        * qa               All quality assesment
          * mriqc         Automated QC

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `ABIDE-1` folder will be created.
    keys : [list of] str
        Data categories to download
    subs : [list of] int
        Only download these subjects (all if empty)
    exclude_subs : [list of] int
        Do not download these subjects
    sites : [list of] {Brown, KKI, NYU, NeuroIMAGE, OHSU, Peking, Pittsburg, WashU}
        Only download these sites (all if empty)
    exclude_sites : [list of] str
        Do not download these sites
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour if a file already exists
    bidsify : bool
        If True, download and organize data directly in `rawdata`.
        Otherwise, download data into `sourcedata`.
    packet : int
        Packet size to download, in bytes
    log : str
        Path to log file
    jobs : int
        Number of parallel jobs

    """  # noqa: E501
    setup_filelog(log)
    packet = human2bytes(packet)
    path = Path(get_tree_path(path))
    keys = set(keys)
    src = path / 'ADHD-200' / 'sourcedata'
    raw = path / 'ADHD-200' / 'rawdata'
    out = raw if bidsify else src

    fs = fsspec.filesystem("s3", anon=True)

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

    # Accumulate downloaders
    def all_downloaders():
        opt = dict(
            chunk_size=human2bytes(packet),
            ifexists=if_exists,
        )

        # build sub2site map
        sub2site = {}
        for site in fs.ls(DATA, detail=True):
            if site["StorageClass"] != "DIRECTORY":
                continue
            site = site["Key"].split("/")[-1]
            for sub in fs.ls(f"{DATA}/{site}"):
                if sub.endswith(".csv"):
                    continue
                sub = int(sub.split("/")[-1])
                sub2site[f"{sub:07d}"] = site
        write_json(sub2site, src / "sub2site.json")

        # Get downloaders for metadata
        if (keys & compat_keys("meta", allkeys)):
            urls = fs.glob(f"{DATA}/*.csv")
            for url in urls:
                name = url.split('/')[-1]
                yield Downloader(f"{S3URL}/url", src / name, **opt)

        # Get downloaders for image data
        if (keys & compat_keys("raw", allkeys)):
            path_raw = DATA
            for site in fs.ls(path_raw, detail=True):
                if site["StorageClass"] != "DIRECTORY":
                    continue
                site = site["Key"].split("/")[-1]
                if site in exclude_sites or (sites and site not in sites):
                    continue
                path_site = f"{path_raw}/{site}"
                for sub in fs.ls(path_site):
                    if sub.endswith(".csv"):
                        continue
                    sub = int(sub.split("/")[-1])
                    if (
                        int(sub) in exclude_subs or
                        (subs and int(sub) not in subs)
                    ):
                        continue
                    sub = f"{sub:07d}"
                    path_sub = f"{path_site}/{sub}"
                    for ses in fs.ls(path_sub):
                        ses = ses.split("/")[-1]
                        ses_id = int(ses.split('_')[-1])
                        path_ses = f"{path_sub}/{ses}"
                        for mod_run in fs.ls(path_ses):
                            mod_run = mod_run.split("/")[-1]
                            mod, run = mod_run.split('_')
                            if not (keys & compat_keys(mod, allkeys)):
                                continue
                            run = int(run)
                            path = out / f"sub-{sub}" / f"ses-{ses_id}"
                            if mod == "anat":
                                base = f"sub-{sub}_ses-{ses_id}_T1w.nii.gz"
                                path = path / "anat" / base
                                mod = "mprage"
                            else:
                                assert mod == "rest"
                                base = (
                                    f"sub-{sub}_ses-{ses_id}_task-rest_"
                                    f"run-{run}_bold.nii.gz"
                                )
                                path = path / "func" / base
                            url = f"{S3URL}/{path_ses}/{mod_run}/{mod}.nii.gz"
                            yield Downloader(url, path, **opt)

        # Get downloaders for freesurfer data
        if (keys & compat_keys("fs", allkeys)):
            for dirpath, _, fnames in fs.walk(SURF):

                if not fnames:
                    continue

                # path: fcp-indi/data/Projects/ADHD200/surfaces/freesurfer/5.3/{sub}/...
                splitpath = dirpath.split("/")
                if len(splitpath) < 8:
                    continue

                sub = int(splitpath[7])
                if sub in exclude_subs or (subs and sub not in subs):
                    continue
                site = sub2site.get(f"{sub:07d}", None)
                if site in exclude_sites or (sites and site not in sites):
                    continue

                fsver = splitpath[6]
                stripdirpath = sep.join(splitpath[7:])
                for fname in fnames:
                    url = f"{S3URL}/{dirpath}/{fname}"
                    path = src / f"freesurfer-{fsver}" / stripdirpath / fname
                    yield Downloader(url, path, **opt)

        # Get downloaders for preprocessed data
        if (keys & compat_keys("preproc", allkeys)):
            for dirpath, _, fnames in fs.walk(PROC):

                if not fnames:
                    continue

                splitpath = dirpath.split("/")
                if len(splitpath) < 8:
                    continue

                proc_keys = (
                    "cpa", "denoise", "fmriprep", "mindboggle", "mriqc",
                    "mindboggle/ants", "mindboggle/freesurfer"
                )

                ok = True
                for key in proc_keys:
                    if key not in dirpath:
                        continue
                    ok = ok and (keys & compat_keys(key, allkeys))
                if not ok:
                    continue

                sub = None
                for part in splitpath:
                    if part.startswith("sub-"):
                        sub = int(part.split("-")[-1])
                        break

                if sub is not None:
                    if sub in exclude_subs or (subs and sub not in subs):
                        continue
                    site = sub2site.get(f"{sub:07d}", None)
                    if site in exclude_sites or (sites and site not in sites):
                        continue

                # path: fcp-indi/data/Projects/ADHD200/Outputs/{pipeline}/...
                stripdirpath = sep.join(splitpath[5:])
                for fname in fnames:
                    url = f"{S3URL}/{dirpath}/{fname}"
                    yield Downloader(url, src / stripdirpath / fname, **opt)

    # Download all
    DownloadManager(
        all_downloaders(),
        ifexists=if_exists,
        path='full',
        jobs=jobs,
        on_error="raise"
    ).run()
