import tarfile
from logging import getLogger
from pathlib import PosixPath

from braindataprep.utils.io import write_from_buffer
from braindataprep.utils.keys import compat_keys
from braindataprep.utils.keys import lower_keys
from braindataprep.freesurfer import bidsify as fs
from braindataprep.actions import Action
from braindataprep.actions import WriteBytes
from braindataprep.actions import CopyJSON
from braindataprep.datasets.ABIDE.I.keys import allleaves, allkeys
from braindataprep.datasets.ABIDE.I.bidsifier import BidsifierBase

lg = getLogger(__name__)


class Bidsifier(BidsifierBase):
    """ABIDE-I - bidsifying logic (XNAT source)"""

    @property
    def _phenotype_paths(self):
        yield self.src / "Phenotypic_V1_0b.csv"

    # ------------------------------------------------------------------
    #   Run all actions
    # ------------------------------------------------------------------

    def _run(self):
        """Must be run from inside the `out` context."""
        if not self.subs:
            self.subs = set()
            for site in self.SITES:
                for fname in self.src.glob(site + '_*'):
                    id = int(fname.name.split('_')[1])
                    self.subs.add(id)
        self.subs -= self.exclude_subs

        # Metadata
        self.nb_errors = self.nb_skipped = 0
        for status in self.make_meta():
            status.setdefault('modality', 'meta')
            self.out(status)

        # Raw and lightly processed data are stored in the same archive
        rawkeys = (allleaves - lower_keys('derivatives')) - lower_keys('meta')
        for key in rawkeys:
            if not (compat_keys(key, allkeys) & self.keys):
                continue
            if ({key} & self.exclude_keys):
                continue
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_raw(key):
                self.out(status)

        # Freesurfer outputs are stored in their own archive
        do_fs = bool(compat_keys('fs', allkeys) & self.keys)
        do_fs |= bool(compat_keys('fs-all', allkeys) & self.keys)
        do_fs &= not bool({'fs', 'fs-all'} & self.exclude_keys)
        if do_fs:
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_freesurfer():
                status.setdefault('modality', 'fs')
                self.out(status)

        # TODO: other derivatives

    # ------------------------------------------------------------------
    #   Write rawdata
    # ------------------------------------------------------------------
    def make_raw(self, key):
        # Run actions
        yield {'progress': 0}
        for i, id in enumerate(self.subs):
            for action in self._make_raw(key, id):
                for status in action:
                    yield from self.fixstatus(status, action.dst.name)
            yield {'progress': 100*(i+1)/len(self.subs)}
        yield {'status': 'done', 'message': ''}

    def _make_raw(self, key, id):
        """Process one subject"""
        fname = "anat" if key == "T1w" else "rest"
        paths = list(self.src.glob(f'*_{id:05d}/{fname}.tar.gz'))
        if not paths:
            raise ValueError(key, id)
        for path in paths:
            try:
                with tarfile.open(path, 'r:gz') as tar:
                    yield from self._make_raw_scan(tar)
            except Exception as e:
                lg.error(f"{path}: {e}")

    def _make_raw_scan(self, tar):
        member = tar.getmembers()[0]
        memberpath = PosixPath(member.name)
        site, id = memberpath.parts[0].split('_')
        id = int(id)
        if memberpath.name.split('.')[0] in ('anat', 'mprage'):
            cat = 'anat'
            mod = 'T1w'
            json = self.TPLDIR / site / 'T1w.json'
        elif memberpath.name.split('.')[0] == 'rest':
            cat = 'func'
            mod = 'task-rest_bold'
            json = self.TPLDIR / site / 'bold.json'
        else:
            return
        dst = self.raw / f'sub-{id:05d}' / cat
        if self.json != 'only':
            yield Action(
                tar.name, dst / f'sub-{id:05d}_{mod}.nii.gz',
                lambda f: write_from_buffer(tar.extractfile(member), f)
            )
        if self.json != 'no':
            yield CopyJSON(json, dst / f'sub-{id:05d}_{mod}.json')

    # ------------------------------------------------------------------
    #   Write freesurfer
    # ------------------------------------------------------------------
    def make_freesurfer(self):
        # Run actions
        yield {'progress': 0}
        for i, id in enumerate(self.subs):
            for action in self._make_freesurfer(id):
                for status in action:
                    yield from self.fixstatus(status, action.dst.name)
                yield {'progress': 100*(i+1)/len(self.subs)}
        yield {'progress': 100}
        yield {'status': 'done', 'message': ''}

    def _make_freesurfer(self, id):
        """Process one subject"""
        paths = self.src.glob(f'OAS3{id:05d}_MR_*/*Freesurfer*.tar.gz')
        dfs = self.drvmap['fs']
        for path in paths:
            ses = path.name.split('.')[0].split('_')[-1]

            # Unpack raw freesurfer outputs
            # under "derivatives/oasis-freesurfer/sourcedata/sub-{04d}/ses-{}"
            with tarfile.open(str(path), 'r:gz') as tar:
                for member in tar.getmembers():
                    tarpath = PosixPath(member.name)
                    if 'fs-all' not in self.keys:
                        if not str(tarpath).endswith(fs.bidsifiable_outputs):
                            continue
                    dst = dfs / 'sourcedata' / f'sub-{id:05d}' / f'ses-{ses}'
                    dst = dst.joinpath(*tarpath.parts[6:])
                    yield WriteBytes(
                        tar.extractfile(member),
                        dst,
                        src=tar.name,
                    )

            # Bidsify under "derivatives/oasis-freesurfer/sub-{04d}/ses-{}"
            src = dfs / 'sourcedata' / f'sub-{id:05d}' / f'ses-{ses}'
            dst = dfs / f'sub-{id:05d}' / f'ses-{ses}'
            srcbase = f'bids:raw:sub-{id:05d}/anat/sub-{id:05d}/ses-{ses}/'
            sourcefiles = [srcbase + 'sub-{id:05d}_ses-{ses}_T1w.nii.gz']
            yield from fs.bidsify(src, dst, sourcefiles, json=self.json)
