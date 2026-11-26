# stdlib
from pathlib import Path
from typing import Iterable
from logging import getLogger

# externals
from humanize import naturalsize

# internals
from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import LayoutChoice, resolve_layout
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
from brainspresso.sources import fcp

lg = getLogger(__name__)

# Paths relative to s3://fcp-indi/data/Projects
DATA = "ADHD200/RawData"
PROC = "ADHD200/Outputs"
SURF = "ADHD200/surfaces"


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
    layout: LayoutChoice | None = None,
    log: str | None = None,
    jobs: int | None = None
):
    """
    Download source data for the ADHD-200 dataset.

    Data are listed and downloaded from the public INDI bucket
    (s3://fcp-indi/data/Projects/ADHD200); no login is needed.

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
        * qa               All quality assessment
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
        If True, download and organize data directly in the raw BIDS
        dataset (`rawbids/`, or the dataset folder with `--layout flat`).
        Otherwise, download data into `sourcedata`.
    packet : int
        Packet size to download, in bytes
    layout : {"study", "flat"}
        Layout of the dataset: "study" (raw dataset in `rawbids/`)
        or "flat" (raw dataset at the root). Default:
        the layout of an existing dataset, else "study".
    log : str
        Path to log file
    jobs : int
        Number of parallel jobs

    """  # noqa: E501
    setup_filelog(log)
    packet = human2bytes(packet)
    path = Path(get_tree_path(path))
    keys = set(keys)
    dataset = resolve_layout(path / 'ADHD-200', layout)
    src = dataset.source
    record_run(dataset, "harvest", {"dataset": "ADHD-200"})
    raw = dataset.raw
    out = raw if bidsify else src

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
            chunk_size=packet,
            ifexists=if_exists,
        )

        def downloader(obj, dst):
            return Downloader(
                fcp.get_bucket().url(obj.key), dst,
                size=obj.size, mtime=obj.mtime,
                digests={"md5": obj.md5} if obj.md5 else None,
                **opt,
            )

        # List raw data (also used to map subjects to sites)
        # keys: data/Projects/ADHD200/RawData/<site>/<sub>/<ses>/<mod>_<run>/*
        raw_objects = fcp.list_objects(DATA)
        prefix = f"{fcp.PROJECTS}/{DATA}/"
        raw_entries = []
        sub2site = {}
        for obj in raw_objects:
            parts = obj.key[len(prefix):].split("/")
            raw_entries.append((parts, obj))
            if len(parts) == 5 and parts[1].isdigit():
                sub2site[f"{int(parts[1]):07d}"] = parts[0]
        src.mkdir(parents=True, exist_ok=True)
        write_json(sub2site, src / "sub2site.json")

        def keep_site(site):
            return not (
                site in exclude_sites or (sites and site not in sites)
            )

        def keep_sub(sub):
            return not (sub in exclude_subs or (subs and sub not in subs))

        # Get downloaders for metadata (top-level csv files)
        if (keys & compat_keys("meta", allkeys)):
            for parts, obj in raw_entries:
                if len(parts) == 1 and parts[0].endswith(".csv"):
                    yield downloader(obj, src / parts[0])

        # Get downloaders for image data
        if (keys & compat_keys("raw", allkeys)):
            for parts, obj in raw_entries:
                if len(parts) != 5:
                    continue
                site, sub, ses, mod_run, fname = parts
                if not keep_site(site) or not sub.isdigit():
                    continue
                sub = int(sub)
                if not keep_sub(sub):
                    continue
                mod, run = mod_run.split('_')
                if not (keys & compat_keys(mod, allkeys)):
                    continue
                expected = {"anat": "mprage.nii.gz", "rest": "rest.nii.gz"}
                if fname != expected.get(mod):
                    continue
                run = int(run)
                ses_id = int(ses.split('_')[-1])
                sub = f"{sub:07d}"
                path = out / f"sub-{sub}" / f"ses-{ses_id}"
                if mod == "anat":
                    base = f"sub-{sub}_ses-{ses_id}_T1w.nii.gz"
                    path = path / "anat" / base
                else:
                    base = (
                        f"sub-{sub}_ses-{ses_id}_task-rest_"
                        f"run-{run}_bold.nii.gz"
                    )
                    path = path / "func" / base
                yield downloader(obj, path)

        # Get downloaders for freesurfer data
        # keys: data/Projects/ADHD200/surfaces/freesurfer/<ver>/<sub>/...
        if (keys & compat_keys("fs", allkeys)):
            prefix = f"{fcp.PROJECTS}/{SURF}/"
            for obj in fcp.list_objects(SURF):
                parts = obj.key[len(prefix):].split("/")
                if len(parts) < 4 or not parts[2].isdigit():
                    continue
                _, fsver, sub, *rest = parts
                sub = int(sub)
                if not keep_sub(sub):
                    continue
                if not keep_site(sub2site.get(f"{sub:07d}", None)):
                    continue
                dst = src / f"freesurfer-{fsver}" / parts[2]
                yield downloader(obj, dst.joinpath(*rest))

        # Get downloaders for preprocessed data
        # keys: data/Projects/ADHD200/Outputs/<pipeline>/...
        if (keys & compat_keys("proc", allkeys)):
            proc_keys = (
                "cpa", "denoise", "fmriprep", "mindboggle", "mriqc",
                "mindboggle/ants", "mindboggle/freesurfer"
            )
            prefix = f"{fcp.PROJECTS}/{PROC}/"
            for obj in fcp.list_objects(PROC):
                rel = obj.key[len(prefix):]
                parts = rel.split("/")
                if len(parts) < 4:
                    continue
                dirpath = "/".join(parts[:-1])
                ok = True
                for key in proc_keys:
                    if key not in dirpath:
                        continue
                    ok = ok and bool(keys & compat_keys(key, allkeys))
                if not ok:
                    continue
                sub = None
                for part in parts:
                    if part.startswith("sub-"):
                        label = part.split("-")[-1]
                        sub = int(label) if label.isdigit() else None
                        break
                if sub is not None:
                    if not keep_sub(sub):
                        continue
                    if not keep_site(sub2site.get(f"{sub:07d}", None)):
                        continue
                yield downloader(obj, src.joinpath(*parts))

    # Download all
    DownloadManager(
        all_downloaders(),
        ifexists=if_exists,
        path='full',
        jobs=jobs,
        on_error="raise"
    ).run()
