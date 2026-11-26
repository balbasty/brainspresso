import nibabel as nib
import numpy as np
from logging import getLogger
from functools import partial
from gzip import GzipFile
from pathlib import Path, PosixPath
from typing import Literal, Iterable, Set

from brainspresso.bids.harvest import finish_roast
from brainspresso.bids.layout import Layout, resolve_layout
from brainspresso.utils.path import fileparts
from brainspresso.utils.archive import open_archive
from brainspresso.utils.io import copy_from_buffer
from brainspresso.utils.io import write_tsv
from brainspresso.utils.tabular import bidsify_tab
from brainspresso.actions import Action
from brainspresso.actions import CopyBytes
from brainspresso.actions import CopyJSON
from brainspresso.actions import WrapAction

lg = getLogger(__name__)
import xlrd  # noqa: E402  (extra: brainspresso[ixi])

KeyChoice = Literal["meta", "T1w", "T2w", "PDw", "angio", "dwi"]
IfExists = Literal["error", "skip", "overwrite", "different", "refresh"]


class Bidsifier:

    # Folder containing template README/JSON/...
    TPLDIR = Path(__file__).parent / 'templates'

    # Conversion between IXI and BIDS modality names
    IXI2BIDS = {
        'T1': 'T1w',
        'T2': 'T2w',
        'PD': 'PDw',
        'MRA': 'angio',
        'DTI': 'dwi',
    }
    BIDS2IXI = {
        'T1w': 'T1',
        'T2w': 'T2',
        'PDw': 'PD',
        'angio': 'MRA',
        'dwi': 'DTI',
    }

    def __init__(
        self,
        root: Path,
        *,
        layout: str | Layout | None = None,
        keys: Set[KeyChoice] = set(KeyChoice.__args__),
        subs: Iterable[int] = tuple(),
        exclude_subs: Iterable[int] = tuple(),
        json: Literal["yes", "no", "only"] | bool = True,
        ifexists: IfExists = "skip"
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
        for key in self.keys.intersection(set(['T1w', 'T2w', 'PDw', 'angio'])):
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_modality(key):
                status.setdefault('modality', key)
                self.out(status)

        # DWI scans are stored as individual 3D niftis (one per bval/bvec)
        # whereas BIDS prefers 4D niftis
        # We also need to deal with the bvals/bvecs files.
        if 'dwi' in self.keys:
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_dwi():
                status.setdefault('modality', 'dwi')
                self.out(status)

    # ------------------------------------------------------------------
    #   Helpers
    # ------------------------------------------------------------------
    def open(self, ixi_key: str):
        """
        Open the images of a modality (e.g. "T1"): either the original
        tarball `sourcedata/IXI-T1.tar` or a folder `sourcedata/IXI-T1/`
        (harvested from NITRC-IR). Returns None if neither exists.
        """
        return open_archive(
            self.src / f'IXI-{ixi_key}.tar', self.src / f'IXI-{ixi_key}'
        )

    def find(self, name: str) -> Path | None:
        """Find a metadata file in `sourcedata/`, or shipped templates"""
        for folder in (self.src, self.TPLDIR):
            if (folder / name).exists():
                return folder / name
        return None

    @staticmethod
    def get_sites(archive) -> dict:
        """Get all available sites"""
        sitemap = {}
        for member in archive.getmembers():
            name = PosixPath(member.name).name
            if not name.startswith('IXI'):
                continue
            ixi_id, site, *_ = name.split('-')
            ixi_id = int(ixi_id[3:])
            sitemap[ixi_id] = site
        return sitemap

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

        # The mapping from subject to site is only available through
        # individual filenames. We therefore need to first parse one of
        # the tar to build this mapping.
        sites = None
        for key in ['T1', 'T2', 'PD', 'MRA', 'DTI']:
            archive = self.open(key)
            if archive is not None:
                with archive:
                    sites = self.get_sites(archive)
                break
        if sites is None:
            lg.error("No IXI images available. Cannot compute sites.")

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
                    **opt
                ),
            'participants.json':
                CopyJSON(
                    self.TPLDIR / 'participants.json',
                    self.raw / 'participants.json',
                    **opt
                ),
            'participants.tsv':
                WrapAction(
                    self.find('IXI.xls') or self.src / 'IXI.xls',
                    self.raw / 'participants.tsv',
                    partial(self.make_participants, sites=sites),
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
    def make_modality(self, key: str):
        # Check that archive is available, and open it
        archive = self.open(self.BIDS2IXI[key])
        if archive is None:
            lg.warning(f'IXI-{self.BIDS2IXI[key]}(.tar) not found')
            return
        with archive:
            yield from self._make_modality(key, archive)

    def _make_modality(self, key: str, tar):
        opt = dict(ifexists=self.ifexists)

        def skip_subject(id):
            return ((self.subs and id not in self.subs)
                    or id in self.exclude_subs)

        def parse_member(member):

            # Get name
            path = PosixPath(member.name)
            id, site, *_ = path.name.split('-')
            id = int(id[3:])
            if skip_subject(id):
                return
            dst = self.raw / f'sub-{id:03d}' / 'anat'
            name = f'sub-{id:03d}_{key}'

            # JSON file
            if self.json != 'no':
                fname = name + '.json'
                for status in CopyJSON(
                    self.TPLDIR / site / f'{key}.json', dst / fname, **opt
                ):
                    yield from self.fixstatus(status, fname)

            # NIFTI file
            if self.json != 'only':
                fname = name + '.nii.gz'
                for status in Action(
                    tar.sources(member.name), dst / fname,
                    lambda fp: copy_from_buffer(tar.extractfile(member), fp),
                    **opt
                ):
                    yield from self.fixstatus(status, fname)

        # Count number of subjects
        nsub = 0
        for path in tar.getnames():
            path = PosixPath(path)
            id = int(path.name.split('-')[0][3:])
            nsub += not skip_subject(id)

        # Process each subject
        isub = 0
        for member in tar.getmembers():
            path = PosixPath(member.name)
            id = int(path.name.split('-')[0][3:])
            if skip_subject(id):
                continue
            isub += 1
            yield from parse_member(member)
            yield {'progress': 100*isub/nsub}

        yield {'status': 'done', 'message': ''}

    # ------------------------------------------------------------------
    #   Generate DWI modality
    # ------------------------------------------------------------------
    def make_dwi(self):
        # Check that archive is available, and open it
        archive = self.open('DTI')
        if archive is None:
            lg.warning('IXI-DTI(.tar) not found')
            return
        with archive:
            yield from self._make_dwi(archive)

    def _make_dwi(self, tar):
        opt = dict(ifexists=self.ifexists)

        # First, copy bvals/bvecs.
        # The original release has a single bvals.txt/bvecs.txt, common
        # to all subjects, that we place at the top of the raw dataset
        # (`layout.raw`). NITRC-IR has one pair per subject (harvested as
        # `IXI-DTI/<IXI###-Site-####>-{bvals,bvecs}.txt`), written next
        # to each subject's dwi file below.
        common = {}
        for fname, source in (('dwi.bval', 'bvals.txt'),
                              ('dwi.bvec', 'bvecs.txt')):
            path = self.find(source)
            if path is None:
                continue
            common[source] = path
            for status in CopyBytes(path, self.raw / fname, **opt):
                yield from self.fixstatus(status, fname)

        # Process individual subjects
        # DWI scans are stored as individual 3D volumes in the archive,
        # but BIDS prefers 4D files. For each subject, we unpack each
        # 3D volume in memory, concatenate them and store them as a 4D
        # nifti.
        #
        # NOTE
        #   we concatenate along the 4-th dimension (indexed 3)
        #   nifti states that the 4-th dimension is reserved for time,
        #   but in terms of acquisition, a dwi series is really like an
        #   fmri time series, where gradients change at each volume.
        #   AY says directions are always along the 4-th dimension, not
        #   the 5-th, in the diffusion world).

        # Get list of all subjects and map to their channels and site
        ids = {}
        chs = {}
        sts = {}
        for path in tar.getnames():
            # Get ID
            _, basename, _ = fileparts(path)
            id, site, *_, dti_id = basename.split('-')
            id = int(id[3:])
            if (
                (self.subs and id not in self.subs) or
                id in self.exclude_subs
            ):
                continue
            sts[id] = site
            ids.setdefault(id, [])
            chs.setdefault(id, [])
            ids[id].append(path)
            chs[id].append(int(dti_id))

        # reorder channels
        for id in ids:
            tmp = list(zip(ids[id], chs[id]))
            tmp.sort(key=lambda x: x[1])
            ids[id] = [x[0] for x in tmp]
            chs[id] = [x[1] for x in tmp]
        nsub = len(ids)

        # Loop through each subject
        isub = 0
        for id, site in sts.items():
            isub += 1

            dst = self.raw / f'sub-{id:03d}' / 'dwi'
            basename = f'sub-{id:03d}_dwi'

            # Write JSON
            if self.json != 'no':
                name = basename + '.json'
                lg.info(f'write {name}')
                for status in CopyJSON(
                    self.TPLDIR / site / 'dwi.json', dst / name, **opt
                ):
                    yield from self.fixstatus(status, name)

            if self.json == 'only':
                yield {'progress': 100*isub/nsub}
                continue

            # Now, concatenate volumes

            # ----------------------------------------------------------
            # This is our (future) concatenatino action for delayed
            def cat_action(path):
                # Load all channels
                membernames = ids[id]
                dat = []
                for i, membername in enumerate(membernames):
                    yield {'status': f'load ch-{i:02d}'}
                    member = tar.getmember(membername)
                    nii = nib.Nifti1Image.from_stream(
                        GzipFile(fileobj=tar.extractfile(member))
                    )
                    dat.append(np.asarray(nii.dataobj).squeeze())

                # Fallback (this happened in one of the subjects...)
                if len(set([tuple(dat1.shape) for dat1 in dat])) > 1:
                    lg.error(
                        f'sub-{id:03d}_dwi | incompatible shapes'
                    )
                    raise RuntimeError('incompatible shapes')

                # Ensure 4D
                yield {'status': 'stacking channels'}
                dat = list(map(lambda x: x[..., None], dat))
                dat = np.concatenate(dat, axis=3)
                yield {'status': 'writing stack'}
                affine, header = nii.affine, nii.header
                nib.save(nib.Nifti1Image(dat, affine, header), path)
            # ----------------------------------------------------------

            name = basename + '.nii.gz'
            for status in Action(
                tar.sources(*ids[id]), dst / name, cat_action,
                ifexists=self.ifexists, input='path',
            ):
                yield from self.fixstatus(status, name)

            # Per-subject b-values/vectors (NITRC-IR)
            prefix = '-'.join(PosixPath(ids[id][0]).name.split('-')[:3])
            for ext, source in (('bval', 'bvals.txt'), ('bvec', 'bvecs.txt')):
                if source in common:
                    continue
                path = self.src / 'IXI-DTI' / f'{prefix}-{source}'
                fname = f'{basename}.{ext}'
                if not path.exists():
                    lg.error(f'{source} not found for sub-{id:03d}')
                    yield from self.fixstatus(
                        {'status': 'error', 'message': f'no {source}'},
                        fname,
                    )
                    continue
                for status in CopyBytes(path, dst / fname, **opt):
                    yield from self.fixstatus(status, fname)

            yield {'progress': 100*isub/nsub}

        yield {'status': 'done', 'message': ''}

    # ------------------------------------------------------------------
    #   Generate participant file
    # ------------------------------------------------------------------
    @staticmethod
    def make_participants(path_xls, path_tsv, sites):
        book = xlrd.open_workbook(path_xls)
        sheet = book.sheet_by_index(0)

        ixi_header = [
            'IXI_ID',
            'SEX_ID',
            'HEIGHT',
            'WEIGHT',
            'ETHNIC_ID',
            'MARITAL_ID',
            'OCCUPATION_ID',
            'QUALIFICATION_ID',
            'DOB',
            'DATE_AVAILABLE',
            'STUDY_DATE',
            'AGE',
        ]
        ixi_age = {
            1: 'M',     # Male
            2: 'F',     # Female
        }
        ixi_ethnicity = {
            1: 'W',     # White
            2: 'B',     # Black or black british
            3: 'A',     # Asian or asian british
            5: 'C',     # Chinese
            6: 'O',     # Other
        }
        ixi_marital_status = {
            1: 'S',     # Single
            2: 'M',     # Married
            3: 'C',     # Cohabiting
            4: 'D',     # Divorced/separated
            5: 'W',     # Widowed
        }
        ixi_occupation = {
            1: 'FT',    # Go out to full time employment
            2: 'PT',    # Go out to part time employment (<25hrs)
            3: 'S',     # Study at college or university
            4: 'H',     # Full-time housework
            5: 'R',     # Retired
            6: 'U',     # Unemployed
            7: 'WFH',   # Work for pay at home
            8: 'O',     # Other
        }
        ixi_qualification = {
            1: 'N',     # No qualifications
            2: 'O',     # O-levels, GCSEs, or CSEs
            3: 'A',     # A-levels
            4: 'F',     # Further education e.g. City & Guilds / NVQs
            5: 'U',     # University or Polytechnic degree
        }
        participants_header = [
            'participant_id',
            'site',
            'age',
            'sex',
            'height',
            'weight',
            'dob',
            'ethnicity',
            'marital_status',
            'occupation',
            'qualification',
            'study_date',
        ]

        def parse_date(date):
            if not isinstance(date, str):
                date = xlrd.xldate.xldate_as_tuple(date, book.datemode)
                date = f'{date[0]:04d}-{date[1]:02d}-{date[2]:02d}'
            return date

        def iter_rows():
            yield participants_header
            for n in range(1, sheet.nrows):
                ixi_row = sheet.row(n)
                if ixi_row[ixi_header.index("DATE_AVAILABLE")].value == 0:
                    continue
                ixi_id = int(ixi_row[ixi_header.index("IXI_ID")].value)
                if ixi_id not in sites:
                    continue
                participant = [
                    f'sub-{ixi_id:03d}',
                    sites[ixi_id],
                    ixi_row[ixi_header.index('AGE')].value or 'n/a',
                    ixi_age.get(
                        ixi_row[ixi_header.index('SEX_ID')].value,
                        'n/a'),
                    ixi_row[ixi_header.index('HEIGHT')].value or 'n/a',
                    ixi_row[ixi_header.index('WEIGHT')].value or 'n/a',
                    parse_date(
                        ixi_row[ixi_header.index('DOB')].value
                    ) or 'n/a',
                    ixi_ethnicity.get(
                        ixi_row[ixi_header.index('ETHNIC_ID')].value,
                        'n/a'),
                    ixi_marital_status.get(
                        ixi_row[ixi_header.index('MARITAL_ID')].value,
                        'n/a'),
                    ixi_occupation.get(
                        ixi_row[ixi_header.index('OCCUPATION_ID')].value,
                        'n/a'),
                    ixi_qualification.get(
                        ixi_row[ixi_header.index('QUALIFICATION_ID')].value,
                        'n/a'),
                    parse_date(
                        ixi_row[ixi_header.index('STUDY_DATE')].value
                    ) or 'n/a',
                ]
                yield participant

        write_tsv(iter_rows(), path_tsv)
