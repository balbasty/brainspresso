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
from pathlib import PurePosixPath

from brainspresso.datasets.unc_wisconsin.roaster import roast_uncwisc
from brainspresso.sources.base import RemoteEntry
from brainspresso.sources.girder import KITWARE, GirderSource

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
        sessions = getattr(selection, "ses", None)
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
                old = (json.loads(path.read_text(encoding="utf-8"))
                       if path.exists() else {})
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


