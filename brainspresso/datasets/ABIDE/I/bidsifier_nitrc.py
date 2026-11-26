import shutil
import tarfile
from logging import getLogger
from pathlib import Path, PosixPath
from typing import IO, Callable, Iterator, Any

from brainspresso.utils.io import write_from_buffer
from brainspresso.utils.keys import compat_keys
from brainspresso.actions import Action
from brainspresso.actions import CopyJSON
from brainspresso.datasets.ABIDE.I.keys import allkeys
from brainspresso.datasets.ABIDE.I.bidsifier import BidsifierBase
from brainspresso.datasets.ABIDE.I.bidsifier import TemplateError

lg = getLogger(__name__)


Status = dict[str, Any]


# (path inside the FCP layout, source file, function that writes the data)
Member = tuple[PosixPath, Path, Callable[[IO[bytes]], None]]


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
                fname = action.dst.name
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

            yield PosixPath(member.name), Path(tar.name), write

    @staticmethod
    def _folder_members(rawdir: Path) -> Iterator[Member]:
        for path in sorted(rawdir.rglob('*.nii.gz')):

            def write(f, path=path):
                with open(path, 'rb') as inp:
                    shutil.copyfileobj(inp, f)

            yield PosixPath(path.relative_to(rawdir).as_posix()), path, write

    def _iter_raw_actions(
        self, members: Iterator[Member]
    ) -> Iterator[tuple[Action, str]]:
        for memberpath, srcpath, write in members:
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
                    lg.error(f"{srcpath}: {memberpath}: {e}")
                    continue
                yield CopyJSON(
                    json, dst / f'sub-{id:05d}_{mod}.json'
                ), mod
