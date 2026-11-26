"""
Designed Database of MR Brain Images of Healthy Volunteers (Bullitt,
CASILab, UNC), from data.kitware.com (Girder, public).

Source layout (folder "Designed Database of MR Brain Images of Healthy
Volunteers" of the collection):

    Normal-<NNN>/T1-Flash/Normal<NNN>-T1-Flash.mha    (also "T!-Flash")
    Normal-<NNN>/T1-MPRage/Normal<NNN>-T1-MPRage.mha  (also "MPRage")
    Normal-<NNN>/T2/Normal<NNN>-T2.mha
    Normal-<NNN>/MRA/Normal<NNN>-MRA.mha
    Normal-<NNN>/DTI/Normal<NNN>-DTI.mha
    Normal-<NNN>/AuxillaryData/SkullStripped-T1-Flash.mha
    Normal-<NNN>/AuxillaryData/VascularNetwork.tre
    Administration/...  (demographics PDF, sample DICOM headers)
    Phantoms/...

Harvested into `Bullitt/sourcedata/` with the same layout.
"""
from pathlib import Path, PurePosixPath
from typing import Annotated, Iterable

from cyclopts import Parameter
from humanize import naturalsize

from brainspresso.bids.harvest import record_run
from brainspresso.bids.layout import LayoutChoice, resolve_layout
from brainspresso.bids.select import Selection
from brainspresso.download import CHUNK_SIZE, IfExists
from brainspresso.sources.base import DownloadOptions, Wants
from brainspresso.sources.girder import GirderSource, KITWARE
from brainspresso.utils.log import setup_filelog
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.ui import human2bytes
from brainspresso.datasets.Bullitt.command import bullitt
from brainspresso.datasets.Bullitt.keys import allkeys

DATASET = "Bullitt"
COLLECTION = "591086ee8d777f16d01e0724"
ROOT = "58a372e38d777f0721a64dc6"   # "Designed Database of MR Brain ..."

# subject sub-folder -> key
FOLDER_KEYS = {
    "T1-Flash": "flash", "T!-Flash": "flash",
    "T1-MPRage": "mprage", "MPRage": "mprage",
    "T2": "T2w", "MRA": "angio", "DTI": "dwi",
}
# AuxillaryData file name prefix -> key
AUX_KEYS = {"SkullStripped": "skullstripped", "VascularNetwork": "vessels"}

Many = Parameter(consume_multiple=True)


def subject_label(folder: str) -> str | None:
    """'Normal-001' -> '001'"""
    if not folder.startswith("Normal-"):
        return None
    return folder[len("Normal-"):]


def _labels(values, prefix) -> set[str]:
    if isinstance(values, (str, int)):
        values = [values]
    out = set()
    for v in values or ():
        v = str(getattr(v, "value", v))
        v = v[len(prefix):] if v.startswith(prefix) else v
        out.add(f"{int(v):03d}" if v.isdigit() else v)
    return out


class Girder(GirderSource):
    """Bullitt database from data.kitware.com, mirrored in `sourcedata/`"""

    name = "girder"
    provides = frozenset({"raw", "meta", "derivatives", "phantoms"})

    def __init__(self, **kwargs):
        # module globals, read when the source is made (tests patch them)
        super().__init__("folder", ROOT, api=KITWARE, **kwargs)

    def keep_folder(self, p: PurePosixPath, selection, wants) -> bool:
        top = p.parts[0]
        if top == "Administration":
            return wants("meta")
        if top == "Phantoms":
            return wants("phantoms")
        sub = subject_label(top)
        if sub is None or not self.keeps_subject(selection, sub):
            return False
        if len(p.parts) >= 2:
            if p.parts[1] == "AuxillaryData":
                return wants("derivatives")
            key = FOLDER_KEYS.get(p.parts[1])
            return key is not None and wants(key)
        return True

    def keep(self, f, selection, wants) -> bool:
        parts = f.relpath.parts
        if len(parts) >= 3 and parts[1] == "AuxillaryData":
            prefix = f.name.split("-")[0].split(".")[0]
            key = AUX_KEYS.get(prefix)
            return key is not None and wants(key)
        return True


@bullitt.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Annotated[Iterable[str], Many] = ("raw", "meta", "derivatives"),
    subs: Annotated[Iterable[str], Many] = (),
    exclude_subs: Annotated[Iterable[str], Many] = (),
    if_exists: IfExists.Choice = "skip",
    token: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = None,
    layout: LayoutChoice | None = None,
    log: str | None = None,
):
    """
    Download the Bullitt healthy MR database (MetaImage files, ~6 GB).

    **Keys:**
    * all
      * meta              Administration folder (demographics PDF,
                          sample DICOM headers)
      * raw
        * anat
          * T1w          flash, mprage
          * T2w
          * angio        MRA
        * dwi            DTI (downloaded but not converted by roast)
      * derivatives
        * skullstripped  Skull-stripped T1 (FLASH)
        * vessels        Vascular networks (.tre)
      * phantoms          BrainWeb and SPL phantoms

    Parameters
    ----------
    path
        Path to root of all datasets. A `Bullitt` folder will be created.
    keys
        Data categories to download (default: raw, meta, derivatives)
    subs
        Only download these subjects (e.g. 1 2 or 001 002)
    exclude_subs
        Do not download these subjects
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
    keys = {str(getattr(k, "value", k)) for k in (
        [keys] if isinstance(keys, str) else keys
    )} or {"raw", "meta", "derivatives"}
    subs = _labels(subs, "sub-")
    exclude_subs = _labels(exclude_subs, "sub-")
    dataset = resolve_layout(Path(get_tree_path(path)) / DATASET, layout)
    record_run(dataset, "harvest", {"dataset": dataset.root.name})
    Girder().configure(token=token).harvest(
        dataset, Selection(subs=subs, exclude_subs=exclude_subs),
        Wants(keys, allkeys),
        DownloadOptions(ifexists=if_exists, jobs=jobs,
                        chunk_size=human2bytes(packet)),
    )
