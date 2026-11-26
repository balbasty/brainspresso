"""
Legacy roaster (`Bidsifier` class, rewritten in step 5) of ABIDE-I
(NITRC tarballs and s3://fcp-indi).

Expected input
--------------
ABIDE-1/
    sourcedata/
        Phenotypic_V1_0b.csv
        {site}_{participant_id}/anat.tar.gz
        {site}_{participant_id}/rest.tar.gz

Expected output
---------------
ABIDE-1/
    dataset_description.json      (DatasetType "study")
    sourcedata/
    rawbids/
        dataset_description.json
        participants.{tsv|json}
        phenotype/
            adir.{tsv|json}
            ados.{tsv|json}
            ados_gotham.{tsv|json}
            srs.{tsv|json}
            vineland.{tsv|json}
            wisc_iv.{tsv|json}
        sub-{:05d}/
            anat/
                sub-{:05d}_T1w.{nii.gz|json}
            func/
                sub-{:05d}_task-rest_run-{:02d}_bold.nii.gz

With `--layout flat`, the content of `rawbids/` is in `ABIDE-1/` itself
(and `sourcedata/`, `derivatives/` stay where they are).
"""
import shutil
import tarfile
from logging import getLogger
from pathlib import Path, PosixPath
from typing import IO, Any, Callable, Iterator

from brainspresso.actions import Action, Fail
from brainspresso.datasets.ABIDE.I.bidsifier import (
    BidsifierBase,
    TemplateError,
)
from brainspresso.datasets.ABIDE.I.keys import allkeys
from brainspresso.utils.io import write_from_buffer
from brainspresso.utils.keys import compat_keys

lg = getLogger(__name__)


Status = dict[str, Any]


# (path inside the FCP layout, source file, function that writes the data,
#  function that opens the data)
Member = tuple[PosixPath, Path, Callable[[IO[bytes]], None],
               Callable[[], IO[bytes]]]


class Bidsifier(BidsifierBase):
    """
    ABIDE-I - bidsifying logic for the FCP layout.

    The raw data can be either
    * NITRC tarballs: `sourcedata/<Site>.tgz`, or
    * a folder downloaded from s3://fcp-indi: `sourcedata/RawData/`.
    Both contain `<Site>/<id>/session_<n>/<mod>_<n>/<file>.nii.gz`.
    """

    @property
    def _phenotype_paths(self) -> Iterator[Path]:
        for site in self.SITES:
            site = site.upper()
            yield from self.src.glob(f"phenotypic_{site}*.csv")

    # ------------------------------------------------------------------
    #   Write raw data
    # ------------------------------------------------------------------
    def make_raw(self) -> Iterator[Status]:

        if not compat_keys('raw', allkeys) & self.keys:
            lg.debug("no raw")
            return

        def run(members: Iterator[Member]) -> Iterator[Status]:
            for action, mod in self._iter_raw_actions(members):
                fname = (action.name if isinstance(action, Fail)
                         else action.dst.name)
                for status in action:
                    yield from self.fixstatus(status, fname, mod)

        # NITRC tarballs
        for path in sorted(self.src.glob('*.tgz')):
            try:
                with tarfile.open(path, 'r:gz') as tar:
                    yield from run(self._tar_members(tar))
            except Exception as e:
                lg.error(f"{path}: {e}")

        # Folder downloaded from S3
        rawdir = self.src / 'RawData'
        if rawdir.is_dir():
            yield from run(self._folder_members(rawdir))

        yield {'status': 'done', 'message': ''}

    @staticmethod
    def _tar_members(tar: tarfile.TarFile) -> Iterator[Member]:
        for member in tar:
            if not member.name.endswith('.nii.gz'):
                continue

            def write(f, member=member):
                write_from_buffer(tar.extractfile(member), f)

            def open_(member=member):
                return tar.extractfile(member)

            yield PosixPath(member.name), Path(tar.name), write, open_

    @staticmethod
    def _folder_members(rawdir: Path) -> Iterator[Member]:
        for path in sorted(rawdir.rglob('*.nii.gz')):

            def write(f, path=path):
                with open(path, 'rb') as inp:
                    shutil.copyfileobj(inp, f)

            def open_(path=path):
                return open(path, 'rb')

            yield (PosixPath(path.relative_to(rawdir).as_posix()), path,
                   write, open_)

    def _iter_raw_actions(
        self, members: Iterator[Member]
    ) -> Iterator[tuple[Action, str]]:
        for memberpath, srcpath, write, open_ in members:
            if len(memberpath.parts) != 5:
                continue
            site, id, ses, mod, fname = memberpath.parts
            id = int(id)
            ses = int(ses.split('_')[-1])
            base, *ext = fname.split(".")
            ext = ".".join(["", *ext])
            if self.subs and id not in self.subs:
                continue
            if self.exclude_subs and id in self.exclude_subs:
                continue
            if base == 'mprage':
                if not (compat_keys('T1w', allkeys) & self.keys):
                    continue
                cat = 'anat'
                mod = 'T1w'
                tpl = 'T1w.json'
            elif base == 'rest':
                if not (compat_keys('bold', allkeys) & self.keys):
                    continue
                cat = 'func'
                mod = 'task-rest_bold'
                tpl = 'bold.json'
            else:
                continue
            dst = self.raw / f'sub-{id:05d}' / cat
            if self.json != 'only':
                yield Action(
                    srcpath, dst / f'sub-{id:05d}_{mod}{ext}', write
                ), mod
            if self.json != 'no' and ext == ".nii.gz":
                try:
                    json = self.template(site, tpl)
                except TemplateError as e:
                    # counted as an error: the run ends partial
                    yield Fail(f"{srcpath}: {memberpath}: {e}",
                               name=f'sub-{id:05d}_{mod}.json'), mod
                    continue
                yield self.sidecar(
                    json, dst / f'sub-{id:05d}_{mod}.json', mod,
                    open_, memberpath.name
                ), mod
