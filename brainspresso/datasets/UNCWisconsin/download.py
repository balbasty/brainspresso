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
import json
from pathlib import Path, PurePosixPath
from typing import Annotated, Iterable

from cyclopts import Parameter
from humanize import naturalsize

from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import LayoutChoice, resolve_layout
from brainspresso.bids.select import Selection
from brainspresso.download import CHUNK_SIZE, IfExists
from brainspresso.sources.base import DownloadOptions, RemoteEntry, Wants
from brainspresso.sources.girder import GirderSource, KITWARE
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


def roast_uncwisc(ctx):
    """Roaster of what `Girder` writes (imported on use: needs nrrd)"""
    from brainspresso.datasets.UNCWisconsin.roaster import roast_uncwisc
    return roast_uncwisc(ctx)


class Girder(GirderSource):
    """
    UNC-Wisconsin from data.kitware.com: `scan_data/` mirrored in
    `sourcedata/scan_data/`, subject and session metadata (Girder folder
    metadata) in `sourcedata/folders.json` (merged with previous runs).
    """

    name = "girder"
    provides = frozenset({"raw", "meta", "derivatives"})
    roaster = staticmethod(roast_uncwisc)

    def __init__(self, **kwargs):
        # module globals, read when the source is made (tests patch them)
        super().__init__("folder", SCAN_DATA, api=KITWARE, **kwargs)

    def keep_folder(self, p: PurePosixPath, selection, wants) -> bool:
        parts = p.parts
        if len(parts) >= 1 and not self.keeps_subject(selection, parts[0]):
            return False
        sessions = getattr(selection, "sessions", None)
        if len(parts) >= 2 and sessions and parts[1] not in sessions:
            return False
        if len(parts) == 3:
            return any(
                mod == parts[2] and wants(key)
                for (mod, _), key in FOLDER_KEYS.items()
            )
        if len(parts) >= 4:
            key = FOLDER_KEYS.get((parts[2], parts[3]))
            return key is not None and wants(key)
        return True

    def keep(self, f, selection, wants) -> bool:
        parts = f.relpath.parts
        if len(parts) < 5:
            return False
        if (parts[2], parts[3]) == ("sMRI", "ORIG"):
            for suffix, key in ANAT_KEYS.items():
                if f.name.endswith(suffix):
                    return wants(key)
            return False
        return True

    def folder_entries(self, folders, selection, wants):
        if not wants("meta"):
            return []
        meta = {str(f.path): f.meta for f in folders
                if f.meta.get("folder_type") in ("subject", "scan")}
        return [RemoteEntry("folders.json", meta={"content": meta,
                                                  "generated": True})]

    def fetch(self, entries, layout, options=None):
        out = []
        for entry in entries:
            meta = entry.meta.get("content")
            if isinstance(meta, dict):
                # Subject and session metadata, merged with previous runs
                path = self.resolve(layout, entry)
                old = json.loads(path.read_text()) if path.exists() else {}
                old.update(meta)
                entry = RemoteEntry(entry.path, meta={
                    "generated": True,
                    "content": json.dumps(dict(sorted(old.items())),
                                          indent=2)})
            out.append(entry)
        return super().fetch(out, layout, options)

    def local_path(self, entry) -> str:
        if entry.meta.get("generated"):
            return entry.path
        return f"scan_data/{entry.path}"


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
    dataset = resolve_layout(Path(get_tree_path(path)) / DATASET, layout)
    record_run(dataset, "harvest", {"dataset": dataset.root.name})
    selection = Selection(subs=_labels(subs, "sub-"),
                          exclude_subs=_labels(exclude_subs, "sub-"),
                          sessions=_labels(sessions, "ses-"))
    Girder().configure(token=token).harvest(
        dataset, selection, Wants(keys, allkeys),
        DownloadOptions(ifexists=if_exists, jobs=jobs,
                        chunk_size=human2bytes(packet)),
    )
