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
from pathlib import PurePosixPath


from brainspresso.sources.girder import GirderSource, KITWARE

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


