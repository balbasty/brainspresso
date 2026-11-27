"""
Legacy roaster (`Bidsifier` class, rewritten in step 5) of COBRE.

Expected input
--------------
COBRE/
  sourcedata/
    COBRE_parameters_mprage.csv
    COBRE_parameters_rest.csv
    COBRE_phenotypic_data.csv
    COBRE_scan_data.tgz

Expected output
---------------
COBRE/
  dataset_description.json          (DatasetType "study")
  sourcedata/
  rawbids/
      README
      dataset_description.json      (DatasetType "raw")
      participants.tsv
      participants.json
      sub-{07d}/
          anat/
              sub-{07d}_T1w.nii.gz
              sub-{07d}_T1w.json
          func/
              sub-{07d}_bold.nii.gz
              sub-{07d}_bold.json

With `--layout flat`, the content of `rawbids/` is in `COBRE/` itself
(and `sourcedata/`, `derivatives/` stay where they are).
"""
import csv
import tarfile
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterable, Literal, Set

from brainspresso.actions import (
    Action,
    CopyBytes,
    CopyJSON,
    IfExists,
    WrapAction,
)
from brainspresso.bids.harvest import finish_roast
from brainspresso.bids.layout import Layout, resolve_layout
from brainspresso.utils.io import copy_from_buffer, write_tsv
from brainspresso.utils.tabular import bidsify_tab

lg = getLogger(__name__)

KeyChoice = Literal["meta", "T1w", "func"]


class Bidsifier:

    # Folder containing template README/JSON/...
    TPLDIR = Path(__file__).parent / 'templates'

    def __init__(
        self,
        root: Path,
        *,
        layout: str | Layout | None = None,
        keys: Set[KeyChoice] = set(KeyChoice.__args__),
        subs: Iterable[int] = tuple(),
        exclude_subs: Iterable[int] = tuple(),
        json: Literal["yes", "no", "only"] | bool = True,
        ifexists: IfExists.Choice = "skip"
    ):
        self.layout: Layout = resolve_layout(root, layout)
        self.root: Path = self.layout.root
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
        self.src = self.layout.source
        self.raw = self.layout.raw
        # Track errors
        self.nb_errors = 0
        self.nb_skipped = 0

    # ------------------------------------------------------------------
    #   Run all
    # ------------------------------------------------------------------
    def run(self):
        """Run all actions"""
        # the state folder holds the lock files (`actions.locks`)
        self.layout.ensure_state()
        self.init()
        with self.out as self.out:
            self._run()
        finish_roast(self.layout, keys=self.keys)

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
                    self.raw / 'README',
                    **opt
                ),
            'dataset_description.json':
                CopyJSON(
                    self.TPLDIR / 'dataset_description.json',
                    self.raw / 'dataset_description.json',
                    **opt,
                    compare=True,
                ),
            'participants.json':
                CopyJSON(
                    self.TPLDIR / 'participants.json',
                    self.raw / 'participants.json',
                    **opt,
                    compare=True,
                ),
            'participants.tsv':
                WrapAction(
                    self.src / 'COBRE_phenotypic_data.csv',
                    self.raw / 'participants.tsv',
                    self.make_participants,
                    mode="t", input="path",
                    compare=True,
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
            path = PurePosixPath(member.name)
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
            path = PurePosixPath(path)
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
            path = PurePosixPath(member.name)
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
            with open(path_csv, 'rt', newline='', encoding='utf-8') as textio:
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
