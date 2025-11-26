import tarfile
import csv
from io import TextIOWrapper
from logging import getLogger
from pathlib import Path, PosixPath
from typing import Iterable, Iterator, Literal

from braindataprep.utils.tabular import bidsify_tab
from braindataprep.utils.tabular import Status
from braindataprep.utils.io import read_json
from braindataprep.utils.io import write_json
from braindataprep.utils.io import write_tsv
from braindataprep.utils.io import write_from_buffer
from braindataprep.utils.keys import compat_keys
from braindataprep.utils.keys import lower_keys
from braindataprep.freesurfer import bidsify as fs
from braindataprep.actions import IfExists
from braindataprep.actions import Action
from braindataprep.actions import WriteBytes
from braindataprep.actions import CopyJSON
from braindataprep.actions import CopyBytes
from braindataprep.actions import WriteTSV
from braindataprep.datasets.OASIS.III.keys import allleaves

lg = getLogger(__name__)


class Bidsifier:
    """OASIS-III - bidsifying logic"""

    # ------------------------------------------------------------------
    #   Constants
    # ------------------------------------------------------------------

    # Folder containing template README/JSON/...
    TPLDIR = Path(__file__).parent / 'templates'

    # ------------------------------------------------------------------
    #   Initialise
    # ------------------------------------------------------------------
    def __init__(
        self,
        root: Path,
        *,
        keys: Iterable[str] = allleaves,
        exclude_keys: Iterable[str] = set(),
        subs: Iterable[int] = tuple(),
        exclude_subs: Iterable[int] = tuple(),
        json: Literal["yes", "no", "only"] | bool = True,
        ifexists: IfExists.Choice = "skip",
    ):
        self.root: Path = Path(root)
        self.keys: set[str] = set(keys)
        self.exclude_keys: set[str] = set(*map(lower_keys, exclude_keys))
        self.subs: set[int] = set(subs)
        self.exclude_subs: set[int] = set(exclude_subs)
        self.json: Literal["yes", "no", "only"] = (
            "yes" if json is True else
            "no" if json is False else json
        )
        self.ifexists: IfExists.Choice = ifexists

    def init(self):
        """Prepare common stuff"""
        # Printer
        self.out = bidsify_tab()
        # Folder
        self.src = self.root / 'sourcedata'
        self.raw = self.root / 'rawdata'
        self.pheno = self.raw / 'phenotype'
        self.drv = self.root / 'derivatives'
        self.dfs = self.drv / 'oasis-freesurfer'
        # Track errors
        self.nb_errors = 0
        self.nb_skipped = 0

    # ------------------------------------------------------------------
    #   Run all actions
    # ------------------------------------------------------------------
    def run(self):
        """Run all actions"""
        self.init()
        with self.out as self.out:
            self._run()

    def _run(self):
        """Must be run from inside the `out` context."""
        if not self.subs:
            self.subs = set()
            for fname in self.src.glob('OAS3*'):
                id = int(fname.name.split('_')[0][4:])
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
            if not (compat_keys(key) & self.keys):
                continue
            if ({key} & self.exclude_keys):
                continue
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_raw(key):
                status.setdefault('modality', key)
                self.out(status)

        # Freesurfer outputs are stored in their own archive
        do_fs = bool(compat_keys('fs') & self.keys)
        do_fs |= bool(compat_keys('fs-all') & self.keys)
        do_fs &= not bool({'fs', 'fs-all'} & self.exclude_keys)
        if do_fs:
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_freesurfer():
                status.setdefault('modality', 'fs')
                self.out(status)

        # TODO: PUP

    # ------------------------------------------------------------------
    #   Helpers
    # ------------------------------------------------------------------
    def fixstatus(self, status: Status, fname: str | Path) -> Iterator[Status]:
        status.setdefault('path', fname)
        yield status
        if status.get('status', '') == 'error':
            self.nb_errors += 1
            yield {'errors': self.nb_errors}
        elif status.get('status', '') == 'skipped':
            self.nb_skipped += 1
            yield {'skipped': self.nb_skipped}

    # ------------------------------------------------------------------
    #   Write metadata files
    # ------------------------------------------------------------------
    def make_meta(self) -> Iterator[Status]:
        # Register future actions
        actions = {}

        if compat_keys('meta') & self.keys:
            actions = {
                **actions,
                'README':
                    CopyBytes(
                        self.TPLDIR / 'README',
                        self.root / 'README',
                    ),
                'dataset_description.json':
                    CopyJSON(
                        self.TPLDIR / 'dataset_description.json',
                        self.root / 'dataset_description.json',
                    ),
                'rawdata/dataset_description.json':
                    CopyJSON(
                        self.TPLDIR / 'dataset_description.json',
                        self.raw / 'dataset_description.json',
                    ),
                'participants.json':
                    CopyJSON(
                        self.TPLDIR / 'participants.json',
                        self.root / 'participants.json',
                    ),
                'sessions.json':
                    CopyJSON(
                        self.TPLDIR / 'sessions.json',
                        self.root / 'sessions.json',
                    ),
                'participants.tsv':
                    self.make_participants(),
            }

        # Register Phenotypes actions
        if compat_keys('pheno') & self.keys:
            for action in self.make_phenotypes():
                actions[action.dst.name] = action

        # Register Freesurfer actions
        fskeys = compat_keys('fs') | compat_keys('fs-all')
        if fskeys & self.keys:
            for action in fs.bidsify_toplevel(self.dfs, (5, 3)):
                actions[action.dst.name] = action

        # Perform actions
        yield {'progress': 0}
        with IfExists(self.ifexists):
            for i, (fname, action) in enumerate(actions.items()):
                for status in action:
                    yield from self.fixstatus(status, fname)
                yield {'progress': 100*(i+1)/len(actions)}
        yield {'progress': 100}

        yield {'status': 'done', 'message': ''}

    # ------------------------------------------------------------------
    #   Write rawdata
    # ------------------------------------------------------------------
    def make_raw(self, key):

        # cat:      OASIS category    -- in folder: OAS3{id}_{cat}_{ses}/
        # subcat:   OASIS subcategory -- in filename: {subcat}{n}.nii.gz
        # bidscat:  BIDS category     -- in folder: {bidscat}/
        # bidsmod:  BIDS modality     -- in filename: sub-{id}_{bidsmod}.nii.gz
        # bidsacq:  BIDS acquisition  -- in filename: acq-{bidsacq}_pet.nii.gz
        cat = subcat = bidscat = bidsmod = bidsacq = None
        if key in lower_keys('mri'):
            cat = 'MR'
            bidsmod = key
            if key in lower_keys('anat'):
                bidscat = 'anat'
                if key in lower_keys('swi'):
                    subcat = 'swi'
                else:
                    subcat = 'anat'
            elif key in lower_keys('func'):
                bidscat = 'func'
                subcat = 'func'
            elif key in lower_keys('perf'):
                bidscat = 'perf'
                subcat = 'func'
            else:
                subcat = key
                if key in ('fmap', 'fieldmap'):
                    bidscat = 'fmap'
                elif key == 'dwi':
                    bidscat = 'dwi'
                else:
                    assert False
        elif key in lower_keys('pet'):
            subcat = bidscat = bidsmod = 'pet'
            if key in lower_keys('fdg'):
                cat = bidsacq = 'FDG'
            elif key in lower_keys('pib'):
                cat = bidsacq = 'PIB'
            elif key in lower_keys('av45'):
                cat = bidsacq = 'AV45'
            elif key in lower_keys('av1451'):
                cat = bidsacq = 'AV1451'
            else:
                assert False
        elif key in lower_keys('ct'):
            cat = subcat = bidsmod = 'CT'
            bidscat = 'ct'
        else:
            assert False, f"{key} not an MR/PET/CT"

        # Run actions
        yield {'progress': 0}
        for i, id in enumerate(self.subs):
            for action in self._make_raw(
                cat, subcat, bidscat, bidsmod, bidsacq, id
            ):
                for status in action:
                    yield from self.fixstatus(status, action.dst.name)
            yield {'progress': 100*(i+1)/len(self.subs)}
        yield {'status': 'done', 'message': ''}

    def _make_raw(self, cat, subcat, bidscat, bidsmod, bidsacq, id):
        """Process one subject"""
        paths = self.src.glob(f'OAS3{id:04d}_{cat}_*/{subcat}*.tar.gz')
        for path in paths:
            try:
                with tarfile.open(path, 'r:gz') as tar:
                    yield from self._make_raw_scan(
                        tar, bidscat, bidsmod, bidsacq, id
                    )
            except Exception as e:
                lg.error(f"{path}: {e}")

    def _make_raw_scan(self, tar, bidscat, bidsmod, bidsacq, id):
        members = tar.getnames()
        if not any(x.endswith(f'_{bidsmod}.nii.gz') for x in members):
            return
        if bidsacq and not any(f'_acq-{bidsacq}_' in x for x in members):
            return
        for member in tar.getmembers():
            membername = PosixPath(member.name)
            flags = membername.name.split('_')
            for flag in flags:
                flag = flag.split('-')
                if flag[0] in ('ses', 'sess'):
                    ses = flag[1]
                    break
            dst = self.raw / f'sub-{id:04d}' / f'ses-{ses}' / bidscat
            mname = self.fix_name(membername.name, id)
            if (
                (mname.endswith('.json') and self.json != 'no')
                or
                (mname.endswith('.nii.gz') and self.json != 'only')
            ):
                yield Action(
                    tar.name, dst / mname,
                    lambda f:
                        write_from_buffer(tar.extractfile(member), f)
                )
            if self.json != 'no':
                # Update session file
                yield self._action_update_session(id, ses)

    def _action_update_session(self, sub: int, ses: str) -> Action:
        delay = int(ses[1:])  # time after entry, in days
        session_file = self.raw / f'sub-{sub:04d}' / 'sessions.tsv'
        participants_file = self.root / 'participants.tsv'
        cdr_file = self.pheno / 'UDS_b4_cdr.tsv'

        def action(dst: Path):
            # Read existing rows
            if dst.exists():
                with dst.open('rt') as f:
                    rows = list(csv.reader(f, delimiter='\t'))
            else:
                rows = []
            if not rows:
                header = ['session_id', 'pathology', 'age']
            else:
                header = rows[0]
                rows = rows[1:]

            if f'ses-{ses}' in (row[0] for row in rows):
                return

            # Read participant's age at entry (ses-d000)
            with open(participants_file, 'rt') as pf:
                preader = csv.reader(pf, delimiter='\t')
                pheader = next(preader)
                participants = [
                    row for row in preader
                    if row[0] == f'sub-{sub:04d}'
                ][0]
            age_at_entry = participants[pheader.index('age')]

            # Read diagnosis at phenotypes session closest to scan session
            with open(cdr_file, 'rt') as cf:
                creader = csv.reader(cf, delimiter='\t')
                cheader = next(creader)
                crows = [
                    row for row in creader
                    if row[0] == f'sub-{sub:04d}'
                ]
            prev_delay = cur_delay = None
            prev_diag = cur_diag = None
            for row in crows:
                cur_delay = int(row[cheader.index('delay')])
                cur_diag = row[cheader.index('DX1_CODE')]
                if cur_delay >= delay:
                    break
                prev_delay = cur_delay
                prev_diag = cur_diag

            if cur_delay is None:
                diag = prev_diag
            elif prev_delay is None:
                diag = cur_diag
            else:
                if abs(cur_delay - delay) < abs(prev_delay - delay):
                    diag = cur_diag
                else:
                    diag = prev_diag

            # Append new row and sort
            age = float(age_at_entry) + delay / 365.25
            age = round(age*1E4) / 1E4  # 4 decimals
            rows.append([f'ses-{ses}', diag, age])
            rows.sort(key=lambda r: int(r[0][5:]))

            # Write back
            with dst.open('wt', newline='') as f:
                writer = csv.writer(f, delimiter='\t')
                writer.writerow(header)
                writer.writerows(rows)

        return Action(
            [participants_file, cdr_file, session_file],
            session_file, action, input="path", mode="a+",
            ifexists='overwrite',
        )

    def fix_name(self, name, id):
        substitutions = {
            'sess-': 'ses-',
            f'sub-OAS3{id:04d}': f'sub-{id:04d}',
            'task-restingstateMB4': 'task-rest_acq-MB4',
            'task-restingstate_': 'task-rest_',
            't2star': 't2starw',
        }
        for old, new in substitutions.items():
            name = name.replace(old, new)
        return name

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
        paths = self.src.glob(f'OAS3{id:04d}_MR_*/*Freesurfer*.tar.gz')
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
                    dst = self.dfs/'sourcedata'/f'sub-{id:04d}'/f'ses-{ses}'
                    dst = dst.joinpath(*tarpath.parts[6:])
                    yield WriteBytes(
                        tar.extractfile(member),
                        dst,
                        src=tar.name,
                    )

            # Bidsify under "derivatives/oasis-freesurfer/sub-{04d}/ses-{}"
            src = self.dfs / 'sourcedata' / f'sub-{id:04d}' / f'ses-{ses}'
            dst = self.dfs / f'sub-{id:04d}' / f'ses-{ses}'
            srcbase = f'bids:raw:sub-{id:04d}/anat/sub-{id:04d}/ses-{ses}/'
            sourcefiles = [srcbase + 'sub-{id:04d}_ses-{ses}_T1w.nii.gz']
            yield from fs.bidsify(src, dst, sourcefiles, json=self.json)

    # ------------------------------------------------------------------
    #   Write phenotypes
    # ------------------------------------------------------------------

    PHENOCOLMAP = {
        'days_to_visit': 'delay',
        'age at visit': 'age',
    }

    PHENOFILES = {
        'a1': 'a1_demographics',
        'a2': 'a2_informant',
        'a3': 'a3_family_history',
        'a4': 'a4_medications',
        'a5': 'a5_health_history',
        'b1': 'b1_physical',
        'b2': 'b2_hiscvd',
        'b3': 'b3_updrs',
        'b4': 'b4_cdr',
        'b5': 'b5_npiq',
        'b6': 'b6_gds',
        'b7': 'b7_fas',
        'b8': 'b8_neurofind',
        'b9': 'b9_symptoms',
        'c1': 'c1_neuropsy',
        'd1': 'd1_diagnosis',
        'd2': 'd2_medical_conditions',
    }

    def make_phenotypes(self):
        for form in self.PHENOFILES:
            yield from self._make_phenotype(form)

    def _make_phenotype(self, form):
        if form == 'a4':
            yield from self._make_phenotype_a4()
            return

        base = self.PHENOFILES[form]
        input_path = self.src / 'OASIS3_data_files' / f'UDS{form}.tar.gz'
        jsonO_path = self.TPLDIR/'phenotype'/'UDSvOAS3'/f'UDS_{base}.json'
        json2_path = self.TPLDIR/'phenotype'/'UDSv2'/f'UDSv2_{base}.json'
        json3_path = self.TPLDIR/'phenotype'/'UDSv3'/f'UDSv3_{base}.json'

        if form == 'c1':
            input_path = self.src / 'OASIS3_data_files' / 'pychometrics.tar.gz'
            jsonO_path = jsonO_path.with_name('UDS_c2_neuropsy.json')
            json3_path = json3_path.with_name('UDSv3_c2_neuropsy.json')
            if not input_path.exists():
                return

        output_path = self.pheno / f'UDS_{base}'

        def action_tsv():

            def fixheader(header):
                for elem in header:
                    if elem == 'OASISID':
                        yield 'participant_id'
                    elif elem == 'OASIS_session_label':
                        yield 'session_id'
                    elif elem == 'days_to_visit':
                        yield 'delay'
                    elif elem == 'age at visit':
                        yield 'age'
                    else:
                        yield elem.upper()

            def fixrow(row, header):
                for elem, title in zip(row, header):
                    if title == 'participant_id':
                        yield f"sub-{int(elem[4:]):04d}"
                    elif title == 'session_id':
                        yield "ses-" + elem.split('_')[-1]
                    elif not elem:
                        yield 'n/a'
                    elif title == 'delay':
                        yield int(elem)
                    elif title == 'age':
                        yield float(elem)
                    elif title in ('DX1', 'DX2', 'DX3', 'DX4', 'DX5'):
                        if elem in ('A', '.'):
                            elem = 'n/a'
                        yield elem
                    else:
                        yield elem

            def yield_rows():
                with tarfile.open(input_path, 'r:gz') as tar:
                    member = list(tar.getmembers())[0]
                    with tar.extractfile(member) as binio:
                        with TextIOWrapper(binio, newline='') as textio:
                            csvio = csv.reader(textio, delimiter=',')
                            header = next(csvio)
                            header = list(fixheader(header))
                            yield header
                            for row in csvio:
                                yield fixrow(row, header)

            return WriteTSV(yield_rows(), output_path.with_suffix('.tsv'),
                            src=input_path)

        def action_json():

            def action(out):

                opath = output_path.with_suffix('.tsv')
                with open(opath, 'rt', newline='') as textio:
                    tsvio = csv.reader(textio, delimiter='\t')
                    header = next(tsvio)
                # Load JSON templates
                jsonvO = jsonv2 = jsonv3 = None
                if jsonO_path.exists():
                    jsonvO = self.fix_phoenotype(form, read_json(jsonO_path))
                if json2_path.exists():
                    jsonv2 = self.fix_phoenotype(form, read_json(json2_path))
                if json3_path.exists():
                    jsonv3 = self.fix_phoenotype(form, read_json(json3_path))
                # Build JSON with appropriate keys taken from template
                json_base = jsonvO or jsonv3 or jsonv2
                if not json_base:
                    raise RuntimeError(f'Non template json {form}')
                json = {
                    key: (json_base[key] if key in json_base else
                          jsonv3[key] if jsonv3 else jsonv2[key])
                    for key in ["MeasurementToolMetadata", "delay", "age"]
                }
                has_v2 = has_v3 = False
                for key in header:
                    if key in ("participant_id", "session_id", "delay", "age"):
                        continue
                    if jsonvO and key in jsonvO:
                        json[key] = jsonvO[key]
                    elif jsonv3 and key in jsonv3:
                        json[key] = jsonv3[key]
                        has_v3 = True
                    elif jsonv2 and key in jsonv2:
                        json[key] = jsonv2[key]
                        has_v2 = True
                    else:
                        json[key] = {
                            "Description": "[WARNING] Unknown phenotype"
                        }
                desc = json["MeasurementToolMetadata"]["Description"]
                if has_v2 and json_base is jsonv3:
                    desc = desc.replace('v3', 'v2+v3')
                elif has_v3 and json_base is jsonv2:
                    desc = desc.replace('v2', 'v2+v3')
                json["MeasurementToolMetadata"]["Description"] = desc
                # Write json
                write_json(json, out)

            return Action(
                output_path.with_suffix('.tsv'),
                output_path.with_suffix('.json'),
                action,
                mode="t"
            )

        if self.json != 'only':
            yield action_tsv()
        if self.json != 'no':
            yield action_json()

    def fix_phoenotype(self, form, obj):
        # Duplicate parents/siblings
        if form not in ('a3', 'b4'):
            return obj
        for key, val in dict(obj).items():
            if "{d}" not in key:
                continue
            if form == 'a3':
                assert key.startswith(("SIB", "KID", "REL"))
                nb = 20 if key.startswith("SIB") else 15
            elif form == 'b4':
                assert key.startswith("DX")
                nb = 5
            else:
                assert False
            del obj[key]
            desc = val.pop("Description")
            for d in range(nb):
                obj[key.format(d=d)] = {
                    'Description': desc.format(d=d),
                    **val
                }
        return obj

    def _make_phenotype_a4(self):
        base = self.PHENOFILES['a4']
        json2_path = self.TPLDIR/'phenotype'/'UDSv2'/f'UDSv2_{base}.json'
        json3_path = self.TPLDIR/'phenotype'/'UDSv3'/f'UDSv3_{base}.json'
        output_paths = {
            'a4d': self.pheno / 'UDS_a4d_medications',
            'a4g': self.pheno / 'UDS_a4g_medications'
        }

        def action_tsv(variant):
            input_path = self.src/'OASIS3_data_files'/f'UDS{variant}.tar.gz'
            output_path = output_paths[variant]

            def fixheader(header):
                for elem in header:
                    if elem == 'OASISID':
                        yield 'participant_id'
                    elif elem == 'OASIS_session_label':
                        yield 'session_id'
                    elif elem == 'days_to_visit':
                        yield 'delay'
                    elif elem == 'age at visit':
                        yield 'age'
                    elif elem == 'INRASEC':
                        elem = 'INRACESC'
                    elif elem.startswith('meds_'):
                        yield f'MEDNAME{int(elem.split("_")[-1]):02d}'
                    elif elem.startswith('drug'):
                        yield f'DRUGID{int(elem.split("drug")[-1]):02d}'
                    else:
                        yield elem.upper()

            def fixrow(row, header):
                for elem, title in zip(row, header):
                    if title == 'participant_id':
                        yield f"sub-{int(elem[4:]):04d}"
                    elif title == 'session_id':
                        yield "ses-" + elem.split('_')[-1]
                    elif not elem:
                        yield 'n/a'
                    elif title == 'delay':
                        yield int(elem)
                    elif title == 'age':
                        yield float(elem)
                    else:
                        yield elem

            def yield_rows():
                with tarfile.open(input_path, 'r:gz') as tar:
                    member = list(tar.getmembers())[0]
                    with tar.extractfile(member) as binio:
                        with TextIOWrapper(binio, newline='') as textio:
                            csvio = csv.reader(
                                textio, delimiter=',', quotechar='"'
                            )
                            header = next(csvio)
                            header = list(fixheader(header))
                            yield header
                            for row in csvio:
                                yield fixrow(row, header)

            return WriteTSV(yield_rows(), output_path.with_suffix('.tsv'),
                            src=input_path, escapechar='\\')

        def action_json(variant):
            output_path = output_paths[variant]

            def action(out):

                opath = output_path.with_suffix('.tsv')
                with open(opath, 'rt', newline='') as textio:
                    tsvio = csv.reader(textio, delimiter='\t')
                    header = next(tsvio)
                # Load JSON templates
                jsonv2 = jsonv3 = None
                if json2_path.exists():
                    jsonv2 = read_json(json2_path)
                if json3_path.exists():
                    jsonv3 = read_json(json3_path)
                # Build JSON with appropriate keys taken from template
                json_base = jsonv2 or jsonv3
                json = {
                    key: json_base[key]
                    for key in ["MeasurementToolMetadata", "delay", "age"]
                }
                has_v2 = has_v3 = False
                for key in header:
                    if key in ("participant_id", "session_id", "delay", "age"):
                        continue
                    elif jsonv2 and key in jsonv2:
                        json[key] = jsonv2[key]
                        has_v2 = True
                    elif jsonv3 and key in jsonv3:
                        json[key] = jsonv3[key]
                        has_v3 = True
                    elif key.startswith('DRUGID'):
                        json[key] = jsonv2['DRUGID']
                    elif key.startswith('MEDNAME'):
                        json[key] = {
                            "Description":
                                "What is the name of the medication?"
                        }
                    else:
                        json[key] = {
                            "Description": "[WARNING] Unknown phenotype"
                        }
                desc = json["MeasurementToolMetadata"]["Description"]
                if has_v2 and json_base is jsonv3:
                    desc = desc.replace('v3', 'v2+v3')
                elif has_v3 and json_base is jsonv2:
                    desc = json["MeasurementToolMetadata"]["Description"]
                json["MeasurementToolMetadata"]["Description"] = desc
                # Write json
                write_json(json, out)

            return Action(
                output_path.with_suffix('.tsv'),
                output_path.with_suffix('.json'),
                action,
                mode="t"
            )

        if self.json != 'only':
            yield action_tsv('a4g')
            yield action_tsv('a4d')
        if self.json != 'no':
            yield action_json('a4g')
            yield action_json('a4d')

    # ------------------------------------------------------------------
    #   Write participants
    # ------------------------------------------------------------------

    PARTICIPANTS_HEADER_MAP = {
        'participant_id': 'OASISID',
        'sex': 'GENDER',
        'handedness': 'HAND',
        'age': 'AgeatEntry',
        'age_at_death': 'AgeatDeath',
        'educ': 'EDUC',
        'ses': 'SES',
        'race': 'racecode',
        'race_aian': 'AIAN',
        'race_nhpi': 'NHPI',
        'race_asian': 'ASIAN',
        'race_black': 'AA',
        'race_white': 'WHITE',
        'daddem': 'daddem',
        'momdem': 'momdem',
        'apoe': 'APOE',
    }

    RACE_MAP = {
        '0': 'U',   # Unknown
        '1': 'N',   # Native American (or Alaska)
        '2': 'A',   # Asian
        '3': 'H',   # Hawaiian or Pacific islanders
        '4': 'B',   # Black
        '5': 'W',   # White
        '6': 'M'    # More than one
    }

    PARTICIPANTS_ROW_MAP = {
        'participant_id': lambda x: f"sub-{int(x[4:]):04d}",
        'sex': lambda x: {'1': 'M', '2': 'F'}.get(x, 'n/a'),
        'handedness': lambda x: x or 'n/a',
        'age': lambda x: x or 'n/a',
        'age_at_death': lambda x: x or 'n/a',
        'educ': lambda x: x or 'n/a',
        'ses': lambda x: x or 'n/a',
        'race': lambda x: Bidsifier.RACE_MAP.get(x, 'n/a'),
        'race_aian': lambda x: {'0': 'N', '1': 'Y'}.get(x, 'n/a'),
        'race_nhpi': lambda x: {'0': 'N', '1': 'Y'}.get(x, 'n/a'),
        'race_asian': lambda x: {'0': 'N', '1': 'Y'}.get(x, 'n/a'),
        'race_black': lambda x: {'0': 'N', '1': 'Y'}.get(x, 'n/a'),
        'race_white': lambda x: {'0': 'N', '1': 'Y'}.get(x, 'n/a'),
        'daddem': lambda x: {'0': 'N', '1': 'Y', '5': 'U'}.get(x, 'n/a'),
        'momdem': lambda x: {'0': 'N', '1': 'Y', '5': 'U'}.get(x, 'n/a'),
        'apoe': lambda x: x or 'n/a'
    }

    def make_participants(self, tgt=None):
        tgt = tgt or self.root
        input_path = self.src / 'OASIS3_data_files' / 'demo.tar.gz'
        output_path = tgt / 'participants.tsv'

        def action_tsv(opath):
            headmap = self.PARTICIPANTS_HEADER_MAP
            rowmap = self.PARTICIPANTS_ROW_MAP

            def yield_rows():
                with tarfile.open(input_path, 'r:gz') as tar:
                    member = tar.getmembers()[0]
                    with tar.extractfile(member) as binio:
                        with TextIOWrapper(binio, newline='') as textio:
                            csvio = csv.reader(textio)
                            inp_header = next(csvio)
                            out_header = list(headmap.keys())
                            yield out_header
                            for row in csvio:
                                yield [
                                    rowmap[hout](
                                        row[inp_header.index(headmap[hout])]
                                    )
                                    for hout in out_header
                                ]

            write_tsv(yield_rows(), opath)

        return Action(input_path, output_path, action_tsv, input="path")
