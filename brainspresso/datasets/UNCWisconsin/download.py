"""
UNC-Wisconsin Rhesus Macaque Neurodevelopment Database, from
data.kitware.com (Girder, public).

Source layout (folder `scan_data` of the collection):

    <sub>/<age>months/sMRI/ORIG/<sub>_<age>months_{T1,T2}_ORIG.nrrd
    <sub>/<age>months/sMRI/Reg2Atlas/...           (derivatives)
    <sub>/<age>months/DTI/ORIG/<...>.nrrd/<sub>_<age>months_dwi.nrrd
    <sub>/<age>months/DTI/{AUTOQC,PROCESSED,MASKMANUAL}/...  (derivatives)

Subject folders hold `dob`, `sex`; session folders hold `scan_date`,
`scan_age`, `scan_weight_kg` (Girder metadata).

Harvested into `UNC-Wisconsin/sourcedata/scan_data/` (same layout), and
the metadata into `UNC-Wisconsin/sourcedata/folders.json`.
"""
from pathlib import Path, PurePosixPath
from typing import Annotated, Iterable

from cyclopts import Parameter
from humanize import naturalsize

from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import LayoutChoice, resolve_layout
from brainspresso.download import CHUNK_SIZE, DownloadManager, IfExists
from brainspresso.sources.girder import Girder, KITWARE
from brainspresso.utils.io import write_json
from brainspresso.utils.keys import compat_keys
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.ui import human2bytes
from brainspresso.datasets.UNCWisconsin.command import uncwisc
from brainspresso.datasets.UNCWisconsin.keys import allkeys

DATASET = "UNC-Wisconsin"
COLLECTION = "54b582c38d777f4362aa9cb3"
SCAN_DATA = "553e6db18d777f082b5918eb"      # folder "scan_data"

# (modality folder, kind folder) -> key
FOLDER_KEYS = {
    ("sMRI", "ORIG"): "anat",
    ("DTI", "ORIG"): "dwi",
    ("sMRI", "Reg2Atlas"): "reg",
    ("DTI", "AUTOQC"): "dti-qc",
    ("DTI", "PROCESSED"): "dti-proc",
    ("DTI", "MASKMANUAL"): "dti-proc",
}
# file name suffix (in sMRI/ORIG) -> key
ANAT_KEYS = {"_T1_ORIG.nrrd": "T1w", "_T2_ORIG.nrrd": "T2w"}

Many = Parameter(consume_multiple=True)


def _labels(values, prefix) -> set[str]:
    if isinstance(values, (str, int)):
        values = [values]
    out = set()
    for v in values or ():
        v = str(getattr(v, "value", v))
        out.add(v[len(prefix):] if v.startswith(prefix) else v)
    return out


@uncwisc.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Annotated[Iterable[str], Many] = ("raw", "meta"),
    subs: Annotated[Iterable[str], Many] = (),
    exclude_subs: Annotated[Iterable[str], Many] = (),
    sessions: Annotated[Iterable[str], Many] = (),
    if_exists: IfExists.Choice = "skip",
    token: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = None,
    layout: LayoutChoice | None = None,
    log: str | None = None,
):
    """
    Download the UNC-Wisconsin rhesus macaque database (NRRD files).

    The full database is ~260 GB (raw data: ~1 GB per session, mostly
    DWI). Use `--subs`, `--sessions` and `--keys` to download a subset.

    **Keys:**
    * all
      * meta              Subject and session metadata
      * raw               Raw data
        * anat           T1w, T2w (sMRI/ORIG)
        * dwi            Diffusion-weighted images (DTI/ORIG)
      * derivatives       Processed data (not converted by `roast` yet)
        * reg            Atlas-registered T1w/T2w, brain masks
        * dti-qc         QCed DWI, tensors, FA, MD (DTI/AUTOQC)
        * dti-proc       Masked tensors, FA, MD (DTI/PROCESSED, MASKMANUAL)

    Parameters
    ----------
    path
        Path to root of all datasets. An `UNC-Wisconsin` folder will be
        created.
    keys
        Data categories to download (default: raw, meta)
    subs
        Only download these subjects (e.g. 001 002)
    exclude_subs
        Do not download these subjects
    sessions
        Only download these sessions (e.g. 12months 24months)
    if_exists
        Behaviour if a file already exists
    token
        Girder API token (not needed: the collection is public)
    packet
        Packet size to download, in bytes
    jobs
        Number of parallel downloads
    layout
        Layout of the dataset (where source data does not move, but
        later commands reuse it): "study" (raw dataset in
        `rawbids/`) or "flat" (raw dataset at the root). Default:
        the layout of an existing dataset, else "study".
    log
        Path to log file
    """
    setup_filelog(log)
    keys = _labels(keys, "") or {"raw", "meta"}
    subs = _labels(subs, "sub-")
    exclude_subs = _labels(exclude_subs, "sub-")
    sessions = _labels(sessions, "ses-")
    dataset = resolve_layout(Path(get_tree_path(path)) / DATASET, layout)
    src = dataset.source
    record_run(dataset, "harvest", {"dataset": dataset.root.name})

    def want(key: str) -> bool:
        return bool(keys & compat_keys(key, allkeys))

    def keep_folder(p: PurePosixPath) -> bool:
        parts = p.parts
        if len(parts) >= 1:
            if subs and parts[0] not in subs:
                return False
            if parts[0] in exclude_subs:
                return False
        if len(parts) >= 2 and sessions and parts[1] not in sessions:
            return False
        if len(parts) == 3:
            return any(
                mod == parts[2] and want(key)
                for (mod, _), key in FOLDER_KEYS.items()
            )
        if len(parts) >= 4:
            key = FOLDER_KEYS.get((parts[2], parts[3]))
            return key is not None and want(key)
        return True

    def keep(f) -> bool:
        parts = f.path.parts
        if len(parts) < 5:
            return False
        if (parts[2], parts[3]) == ("sMRI", "ORIG"):
            for suffix, key in ANAT_KEYS.items():
                if f.path.name.endswith(suffix):
                    return want(key)
            return False
        return True

    girder = Girder(KITWARE, token=token)
    folders = []

    def downloaders():
        yield from girder.mirror(
            "folder", SCAN_DATA, src / "scan_data", keep,
            keep_folder=keep_folder, folders=folders,
            ifexists=if_exists, chunk_size=human2bytes(packet),
        )
        if want("meta"):
            # Subject and session metadata (merged with previous runs)
            meta_path = src / "folders.json"
            meta = {}
            if meta_path.exists():
                import json
                meta = json.loads(meta_path.read_text())
            for f in folders:
                if f.meta.get("folder_type") in ("subject", "scan"):
                    meta[str(f.path)] = f.meta
            write_json(dict(sorted(meta.items())), meta_path)

    DownloadManager(
        downloaders(), ifexists=if_exists, jobs=jobs, path="short",
    ).run()
