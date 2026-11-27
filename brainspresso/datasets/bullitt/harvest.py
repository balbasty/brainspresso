"""
Harvest the Bullitt database from data.kitware.com (Girder, public).

Designed Database of MR Brain Images of Healthy Volunteers (Bullitt,
CASILab, UNC).

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

from brainspresso.datasets.bullitt.roaster import (
    AUX_KEYS,
    FOLDER_KEYS,
    roast_bullitt,
    subject_label,
)
from brainspresso.sources.girder import KITWARE, GirderSource

__all__ = ["AUX_KEYS", "FOLDER_KEYS", "Girder", "subject_label"]

DATASET = "Bullitt"
COLLECTION = "591086ee8d777f16d01e0724"
ROOT = "58a372e38d777f0721a64dc6"   # "Designed Database of MR Brain ..."


class Girder(GirderSource):
    """
    Bullitt database from data.kitware.com, mirrored in `sourcedata/`.

    Parameters
    ----------
    **kwargs
        Those of `GirderSource`
    """

    name = "girder"
    provides = frozenset({"raw", "meta", "derivatives", "phantoms"})
    roaster = staticmethod(roast_bullitt)

    def __init__(self, **kwargs):
        # module globals, read when the source is made (tests patch them)
        super().__init__("folder", ROOT, api=KITWARE, **kwargs)

    def keep_folder(self, p: PurePosixPath, selection, wants) -> bool:
        """
        Whether to walk a folder.

        Parameters
        ----------
        p : PurePosixPath
            Folder, relative to the root folder
        selection : Selection
            Subject selection
        wants : callable(str) -> bool
            Whether a key is wanted

        Returns
        -------
        bool
            Administration: `meta`; Phantoms: `phantoms`; subject
            folders: selected subjects, the folders of wanted keys
        """
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
        """
        Whether to download a file.

        Parameters
        ----------
        f : RemoteEntry
            File (`relpath`: relative to the root folder)
        selection : Selection
            Subject selection
        wants : callable(str) -> bool
            Whether a key is wanted

        Returns
        -------
        bool
            AuxillaryData: the files of wanted keys; others: True (their
            folders are filtered by `keep_folder`)
        """
        parts = f.relpath.parts
        if len(parts) >= 3 and parts[1] == "AuxillaryData":
            prefix = f.name.split("-")[0].split(".")[0]
            key = AUX_KEYS.get(prefix)
            return key is not None and wants(key)
        return True


