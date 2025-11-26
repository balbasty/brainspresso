import tarfile
from logging import getLogger
from pathlib import Path, PosixPath
from typing import Iterator, Any

from brainspresso.utils.io import write_from_buffer
from brainspresso.utils.keys import compat_keys
from brainspresso.actions import Action
from brainspresso.actions import CopyJSON
from brainspresso.datasets.ABIDE.I.keys import allkeys
from brainspresso.datasets.ABIDE.I.bidsifier import BidsifierBase

lg = getLogger(__name__)


Status = dict[str, Any]


class Bidsifier(BidsifierBase):
    """ABIDE-I - bidsifying logic (NITRC source)"""

    @property
    def _phenotype_paths(self) -> Iterator[Path]:
        for site in self.SITES:
            site = site.upper()
            yield from self.src.glob(f"phenotypic_{site}*.csv")

    # ------------------------------------------------------------------
    #   Write rawdata
    # ------------------------------------------------------------------
    def make_raw(self) -> Iterator[Status]:

        if not compat_keys('raw', allkeys) & self.keys:
            lg.debug("no raw")
            return

        progress = 0
        # yield {'progress': progress}
        paths = self.src.glob('*.tgz')
        for path in paths:
            try:
                with tarfile.open(path, 'r:gz') as tar:
                    for action, mod in self._iter_raw_actions(tar):
                        fname = action.dst.name
                        for status in action:
                            yield from self.fixstatus(status, fname, mod)
                        progress += 1
                        # yield {'progress': progress}
            except Exception as e:
                lg.error(f"{path}: {e}")
        yield {'status': 'done', 'message': ''}

    def _iter_raw_actions(self, tar) -> Iterator[tuple[Action, str]]:
        for member in tar:
            memberpath = PosixPath(member.name)
            if not memberpath.name.endswith('.nii.gz'):
                continue
            site, id, ses, mod, fname = memberpath.parts
            site, *sample = site.split('_')
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
                json = self.TPLDIR / site / 'T1w.json'
            elif base == 'rest':
                if not (compat_keys('bold', allkeys) & self.keys):
                    continue
                cat = 'func'
                mod = 'task-rest_bold'
                json = self.TPLDIR / site / 'bold.json'
            else:
                continue
            dst = self.raw / f'sub-{id:05d}' / cat
            if self.json != 'only':
                yield Action(
                    tar.name, dst / f'sub-{id:05d}_{mod}{ext}',
                    lambda f: write_from_buffer(tar.extractfile(member), f)
                ), mod
            if self.json != 'no' and ext == ".nii.gz":
                yield CopyJSON(
                    json, dst / f'sub-{id:05d}_{mod}.json'
                ), mod
