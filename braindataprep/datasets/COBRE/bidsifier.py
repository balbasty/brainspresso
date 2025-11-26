import tarfile
import csv
from logging import getLogger
from pathlib import Path, PosixPath
from typing import Literal, Iterable, Set

from braindataprep.utils.io import copy_from_buffer
from braindataprep.utils.io import write_tsv
from braindataprep.utils.tabular import bidsify_tab
from braindataprep.actions import Action
from braindataprep.actions import IfExists
from braindataprep.actions import CopyBytes
from braindataprep.actions import CopyJSON
from braindataprep.actions import WrapAction

lg = getLogger(__name__)

KeyChoice = Literal["meta", "T1w", "func"]


class Bidsifier:

    # Folder containing template README/JSON/...
    TPLDIR = Path(__file__).parent / 'templates'

    def __init__(
        self,
        root: Path,
        *,
        keys: Set[KeyChoice] = set(KeyChoice.__args__),
        subs: Iterable[int] = tuple(),
        exclude_subs: Iterable[int] = tuple(),
        json: Literal["yes", "no", "only"] | bool = True,
        ifexists: IfExists.Choice = "skip"
    ):
        self.root = root
        self.keys = keys
        self.subs = subs
        self.exclude_subs = exclude_subs
        self.json = json
        self.ifexists = ifexists

    def init(self):
        """Prepare common stuff"""
        # Printer
        self.out = bidsify_tab()
        # Folder
        self.src = self.root / 'sourcedata'
        self.raw = self.root / 'rawdata'
        # Track errors
        self.nb_errors = 0
        self.nb_skipped = 0

    # ------------------------------------------------------------------
    #   Run all
    # ------------------------------------------------------------------
    def run(self):
        """Run all actions"""
        self.init()
        with self.out as self.out:
            self._run()

    def _run(self):
        """Must be run from inside the `out` context."""
        # Metadata
        if 'meta' in self.keys:
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_meta():
                status.setdefault('modality', 'meta')
                self.out(status)

        # T1/T2/PD/MRA are simple "anat" scans that can be processed
        # identically.
        if self.keys.intersection({"T1w", "func"}):
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_raw():
                status.setdefault('modality', 'raw')
                self.out(status)

    # ------------------------------------------------------------------
    #   Helpers
    # ------------------------------------------------------------------
    def fixstatus(self, status: dict, fname: str):
        status.setdefault('path', fname)
        yield status
        if status.get('status', '') == 'error':
            self.nb_errors += 1
            yield {'errors': self.nb_errors}
        elif status.get('status', '') == 'skipped':
            self.nb_skipped += 1
            yield {'skipped': self.nb_skipped}

    # ------------------------------------------------------------------
    #   Generate root metadata (dataset, participants, etc)
    # ------------------------------------------------------------------
    def make_meta(self):

        # Define future actions
        opt = dict(ifexists=self.ifexists)
        actions = {
            'README':
                CopyBytes(
                    self.TPLDIR / 'README',
                    self.root / 'README',
                    **opt
                ),
            'dataset_description.json':
                CopyJSON(
                    self.TPLDIR / 'dataset_description.json',
                    self.root / 'dataset_description.json',
                    **opt
                ),
            'participants.json':
                CopyJSON(
                    self.TPLDIR / 'participants.json',
                    self.root / 'participants.json',
                    **opt
                ),
            'participants.tsv':
                WrapAction(
                    self.src / 'COBRE_phenotypic_data.csv',
                    self.root / 'participants.tsv',
                    self.make_participants,
                    mode="t", input="path",
                    **opt
                ),
        }

        # Perform actions
        yield {'progress': 0}
        for i, (fname, action) in enumerate(actions.items()):
            for status in action:
                yield from self.fixstatus(status, fname)
            yield {'progress': 100*(i+1)/len(actions)}
        yield {'status': 'done', 'message': ''}

    # ------------------------------------------------------------------
    #   Generate structural modalities
    # ------------------------------------------------------------------
    def make_raw(self):
        # Check that archive is available, and open it
        tarpath = self.src / 'COBRE_scan_data.tgz'
        # if tar is None:
        if not tarpath.exists():
            lg.warning('COBRE_scan_data.tar not found')
            return
        with tarfile.open(tarpath) as tar:
            yield from self._make_raw(tar)

    def _make_raw(self, tar):
        opt = dict(ifexists=self.ifexists)

        def skip_subject(id):
            id = int(id)
            return ((self.subs and id not in self.subs)
                    or id in self.exclude_subs)

        def parse_member(member):

            # Get name
            path = PosixPath(member.name)
            id = path.parts[1]
            if skip_subject(id):
                return
            if path.name == 'mprage.nii.gz':
                cat = 'anat'
                key = 'T1w'
                flag = ''
            else:
                assert path.name == 'rest.nii.gz'
                cat = 'func'
                key = 'bold'
                flag = 'task-rest_'
            dst = self.raw / f'sub-{id}' / cat
            name = f'sub-{id}_{flag}{key}'

            # JSON file
            if self.json != 'no':
                fname = name + '.json'
                for status in CopyJSON(
                    self.TPLDIR / f'{key}.json', dst / fname, **opt
                ):
                    yield from self.fixstatus(status, fname)

            # NIFTI file
            if self.json != 'only':
                fname = name + '.nii.gz'
                for status in Action(
                    tar.name, dst / fname,
                    lambda fp: copy_from_buffer(tar.extractfile(member), fp),
                    **opt
                ):
                    yield from self.fixstatus(status, fname)

        # Count number of subjects
        nsub = 0
        ids = set()
        for path in tar.getnames():
            if not path.endswith('.nii.gz'):
                continue
            path = PosixPath(path)
            id = path.parts[1]
            if id in ids:
                continue
            ids.add(id)
            nsub += not skip_subject(id)

        # Process each subject
        nscan = nsub * (bool('T1w' in self.keys) + bool('func' in self.keys))
        iscan = 0
        for member in tar.getmembers():
            if not member.name.endswith('.nii.gz'):
                continue
            path = PosixPath(member.name)
            id = path.parts[1]
            if skip_subject(id):
                continue
            if path.name == 'mprage.nii.gz' and 'T1w' not in self.keys:
                continue
            elif path.name == 'rest.nii.gz' and 'func' not in self.keys:
                continue
            iscan += 1
            yield from parse_member(member)
            yield {'progress': 100*iscan/nscan}

        yield {'status': 'done', 'message': ''}

    # ------------------------------------------------------------------
    #   Generate participant file
    # ------------------------------------------------------------------
    @staticmethod
    def make_participants(path_csv, path_tsv):

        HEADER = [
            'participant_id',
            'age',
            'sex',
            'handedness',
            'patient',
            'icd9',
            'icd9_subtype',
        ]

        MAPROW = [
            (lambda x: x[0]),
            (lambda x: x[1]),
            (lambda x: x[2][0]),
            (lambda x: x[3][0]),
            (lambda x: 'Y' if x[4][0] == 'P' else 'N'),
            (lambda x: {'None': ''}.get(x[5], x[5].split()[0])),
            (lambda x: x[5].split()[1] if len(x[5].split()) > 1 else ''),
        ]

        def iter_rows():
            with open(path_csv, 'rt', newline='') as textio:
                csvio = csv.reader(textio)
                next(csvio)
                yield HEADER
                for row in csvio:
                    row = list(row)
                    row = [elem(row) for elem in MAPROW]
                    if 'Disenrolled' in row:
                        row = row[:1] + ['n/a'] * 6
                    yield row

        write_tsv(iter_rows(), path_tsv)
