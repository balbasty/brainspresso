import csv
import gzip
import json as jsonlib
from logging import getLogger
from pathlib import Path
from typing import IO, Callable, Iterable, Iterator, Literal

import nibabel as nib

from brainspresso.bids.harvest import finish_roast
from brainspresso.bids.layout import Layout, resolve_layout
from brainspresso.utils.tabular import bidsify_tab
from brainspresso.utils.tabular import Status
from brainspresso.utils.io import write_tsv
from brainspresso.utils.tsv import TableMapper
from brainspresso.utils.keys import compat_keys
from brainspresso.utils.keys import lower_keys
from brainspresso.actions import IfExists
from brainspresso.actions import Action
from brainspresso.actions import CopyJSON
from brainspresso.actions import CopyBytes
from brainspresso.actions import Fail
from brainspresso.actions import WriteJSON
from brainspresso.datasets.ABIDE.I.keys import allkeys

lg = getLogger(__name__)


class TemplateError(LookupError):
    """No sidecar template for a (site, template) pair."""


class BidsifierBase:
    """ABIDE-I - common bidsifying logic"""

    # ------------------------------------------------------------------
    #   Constants
    # ------------------------------------------------------------------

    # Folder containing template README/JSON/...
    TPLDIR: Path = Path(__file__).parent / 'templates'

    SITES: tuple[str, ...] = (
        'Caltech', 'CMU', 'KKI', 'Leuven', 'MaxMun', 'NYU', 'OHSU',
        'Olin', 'Pitt', 'SBL', 'SDSU', 'Stanford', 'Trinity', 'UCLA',
        'UM', 'USM', 'Yale',
    )

    # Sites whose sidecars are per sample (`templates/<Site>_<sample>/`).
    SAMPLES: dict[str, tuple[str, ...]] = {
        'CMU': ('a', 'b'),
        'Leuven': ('1', '2'),
        'MaxMun': ('a', 'b', 'c', 'd'),
        'UCLA': ('1', '2'),
        'UM': ('1', '2'),
    }
    # Parts of the NITRC tarballs of sites without samples (`NYU_a.tgz`)
    # (`sources.PARTS`, whose CMU and MaxMun parts are samples here)
    PARTS: dict[str, tuple[str, ...]] = {
        'NYU': ('a', 'b', 'c', 'd', 'e'),
    }

    @classmethod
    def parse_site(cls, site: str) -> tuple[str, str | None]:
        """Split a site name found in the data into (Site, sample).

        Accepts the spellings of all sources: S3/NITRC folders and
        tarballs (`MaxMun_a`, `UCLA_1`, `NYU_a`), XNAT subject labels and
        scan-parameter folders (`CMU`, `Leuven-1`, `UCLA1`) and phenotypic
        site IDs (`MAX_MUN`, `LEUVEN_1`). The sample is None when the data
        does not give it, or for a site without per-sample sidecars (whose
        only accepted suffix is a tarball part, e.g. `NYU_a`). Raises
        `TemplateError` for an unknown site, sample or suffix.
        """
        flat = ''.join(c for c in site if c.isalnum()).lower()
        for name in sorted(cls.SITES, key=len, reverse=True):
            if flat.startswith(name.lower()):
                break
        else:
            raise TemplateError(f'ABIDE-I: unknown site {site!r}')
        suffix = flat[len(name):] or None
        if name not in cls.SAMPLES:
            if suffix is not None and suffix not in cls.PARTS.get(name, ()):
                raise TemplateError(
                    f'ABIDE-I: unknown suffix {suffix!r} of site {name} '
                    f'(from {site!r}); known parts: '
                    f'{cls.PARTS.get(name, ())}')
            return name, None
        if suffix is not None and suffix not in cls.SAMPLES[name]:
            raise TemplateError(
                f'ABIDE-I: unknown sample {suffix!r} of site {name} '
                f'(from {site!r}); known samples: {cls.SAMPLES[name]}')
        return name, suffix

    _warned: set = set()

    @classmethod
    def template(cls, site: str, name: str) -> Path:
        """Sidecar template for a scan.

        `site` is the site name found in the data (see `parse_site`).

        * Site without samples: `templates/<Site>/<name>`.
        * Known sample: `templates/<Site>_<sample>/<name>`, else
          `templates/<Site>/<name>` (e.g. `CMU/T1w.json`, used by CMU_a,
          which has no anatomical printout).
        * Unknown sample (e.g. XNAT `CMU_50642`, `MaxMun`):
          `templates/<Site>/<name>` if it exists, a template holding only
          what the samples share (e.g. `CMU/bold.json`; a warning is
          logged); else the first sample's, which is only used where all
          samples share one protocol (see tests/test_abide1_templates.py).

        Raises `TemplateError`, naming the site and template, if none
        exists.
        """
        site_name, sample = cls.parse_site(site)
        candidates = []
        common = f'{site_name}/{name}'
        if site_name in cls.SAMPLES and sample is None:
            candidates = [common, f'{site_name}_{cls.SAMPLES[site_name][0]}'
                          f'/{name}']
        elif site_name in cls.SAMPLES:
            candidates = [f'{site_name}_{sample}/{name}', common]
        else:
            candidates = [common]
        for candidate in candidates:
            path = cls.TPLDIR / candidate
            if path.is_file():
                if (sample is None and site_name in cls.SAMPLES
                        and candidate == common
                        and (site, name) not in cls._warned):
                    cls._warned.add((site, name))
                    lg.warning(
                        f'ABIDE-I: site {site!r} names no sample of '
                        f'{site_name}: using the site template '
                        f'{candidate}')
                return path
        raise TemplateError(
            f'ABIDE-I: no template {name!r} for site {site!r} '
            f'(tried {", ".join(candidates)})')

    # plausible RepetitionTime (s) read from a NIfTI header
    TR_RANGE: tuple[float, float] = (0.1, 30.)

    @classmethod
    def header_tr(cls, fileobj: IO[bytes], name: str = '') -> float | None:
        """RepetitionTime (s) from a NIfTI-1 header (`pixdim[4]`, time
        unit of `xyzt_units`), or None if unset, of unknown unit or not
        plausible (`TR_RANGE`)."""
        if name.endswith('.gz'):
            fileobj = gzip.GzipFile(fileobj=fileobj)
        hdr = nib.Nifti1Header.from_fileobj(fileobj)
        if hdr['dim'][0] < 4:
            return None
        scale = {'sec': 1., 'msec': 1e-3, 'usec': 1e-6}.get(
            hdr.get_xyzt_units()[1])
        if scale is None:
            return None
        tr = float(hdr['pixdim'][4]) * scale
        lo, hi = cls.TR_RANGE
        return round(tr, 6) if lo <= tr <= hi else None

    @classmethod
    def sidecar(
        cls, tpl: Path, dst: Path, mod: str,
        open_image: Callable[[], IO[bytes]], image_name: str,
    ) -> Action:
        """Action writing the sidecar `dst` of a scan from template `tpl`.

        A bold template without `RepetitionTime` (REQUIRED by BIDS; e.g.
        the common `CMU/bold.json`, whose samples differ in TR) gets it
        from the NIfTI header (`open_image()`); if the header gives none,
        a `Fail` naming the missing key. (`SliceTiming`, RECOMMENDED,
        may be absent.)
        """
        if not mod.endswith('bold'):
            return CopyJSON(tpl, dst)
        meta = jsonlib.loads(tpl.read_text())
        if 'RepetitionTime' in meta:
            return CopyJSON(tpl, dst)
        try:
            with open_image() as f:
                tr = cls.header_tr(f, image_name)
        except Exception as e:
            lg.debug(f'{image_name}: {e}')
            tr = None
        if tr is None:
            return Fail(
                f'{dst.name}: template {tpl.relative_to(cls.TPLDIR)} has no '
                f'RepetitionTime (REQUIRED) and the NIfTI header of '
                f'{image_name} gives none', name=dst.name)
        return WriteJSON({**meta, 'RepetitionTime': tr}, dst, src=tpl)

    # ------------------------------------------------------------------
    #   Initialise
    # ------------------------------------------------------------------
    def __init__(
        self,
        root: Path,
        *,
        layout: str | Layout | None = None,
        keys: Iterable[str] = {"all"},
        exclude_keys: Iterable[str] = set(),
        subs: Iterable[int] = tuple(),
        exclude_subs: Iterable[int] = tuple(),
        json: Literal["yes", "no", "only"] | bool = True,
        ifexists: IfExists.Choice = "skip",
    ):
        self.layout: Layout = resolve_layout(root, layout)
        self.root: Path = self.layout.root
        self.keys: set[str] = set(keys)
        self.exclude_keys: set[str] = set(
            *map(lambda x: lower_keys(x, allkeys), exclude_keys)
        )
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
        self.src = self.layout.source
        self.raw = self.layout.raw
        self.pheno = self.layout.phenotype
        drv = self.layout.derivative
        self.drvmap = {
            'fs': drv('abide-freesurfer').root,
            'ants': drv('abide-ants').root,
            'civet': drv('abide-civet').root,
            'ccs': drv('abide-ccs').root,
            'cpac': drv('abide-cpac').root,
            'dparsf': drv('abide-dparsf').root,
            'niak': drv('abide-niak').root,
            'proc-min': drv('abide-processed').root,
            'qa-man': drv('abide-qa-man').root,
            'qa-pcp': drv('abide-qa-pcp').root,
        }
        # Track errors
        self.nb_errors = 0
        self.nb_skipped = 0

    # ------------------------------------------------------------------
    #   Run all actions
    # ------------------------------------------------------------------
    def run(self) -> None:
        """Run all actions"""
        # the state folder holds the lock files (`actions.locks`)
        self.layout.ensure_state()
        self.init()
        with self.out as self.out:
            self._run()
        finish_roast(self.layout, keys=self.keys)

    def _run(self):
        """Must be run from inside the `out` context."""

        self.nb_errors = {}
        self.nb_skipped = {}

        # Metadata
        for status in self.make_meta():
            self.out(status)

        # Raw and lightly processed data are stored in the same archive
        for status in self.make_raw():
            self.out(status)

        # Freesurfer outputs are stored in their own archive
        for status in self.make_freesurfer():
            self.out(status)

        # TODO: other derivatives

    # ------------------------------------------------------------------
    #   To implement in children
    # ------------------------------------------------------------------
    def make_raw(self) -> Iterator[Status]:
        if False:
            yield
        return

    def make_freesurfer(self) -> Iterator[Status]:
        if False:
            yield
        return

    # ------------------------------------------------------------------
    #   Helpers
    # ------------------------------------------------------------------
    def fixstatus(
        self, status: Status, fname: str | Path, mod: str
    ) -> Iterator[Status]:
        status.setdefault('modality', mod)
        status.setdefault('path', fname)
        yield status
        if status.get('status', '') == 'error':
            self.nb_errors.setdefault(mod, 0)
            self.nb_errors[mod] += 1
            yield {'errors': self.nb_errors[mod]}
        elif status.get('status', '') == 'skipped':
            self.nb_skipped.setdefault(mod, 0)
            self.nb_skipped[mod] += 1
            yield {'skipped': self.nb_skipped[mod]}

    # ------------------------------------------------------------------
    #   Write metadata files
    # ------------------------------------------------------------------
    def make_meta(self) -> Iterator[Status]:
        # Register future actions
        actions = {}

        if compat_keys('meta', allkeys) & self.keys:
            actions = {
                **actions,
                'README':
                    CopyBytes(
                        self.TPLDIR / 'README',
                        self.raw / 'README',
                    ),
                'dataset_description.json':
                    CopyJSON(
                        self.TPLDIR / 'dataset_description.json',
                        self.raw / 'dataset_description.json',
                        compare=True,
                    ),
                'participants.json':
                    CopyJSON(
                        self.TPLDIR / 'participants.json',
                        self.raw / 'participants.json',
                        compare=True,
                    ),
                'participants.tsv':
                    self.make_participants(),
            }

        # Register Phenotypes actions
        if compat_keys('pheno', allkeys) & self.keys:
            for action in self.make_phenotypes():
                actions[action.dst.name] = action

        # Perform actions
        if actions:
            yield {'progress': 0}
            with IfExists(self.ifexists):
                for i, (fname, action) in enumerate(actions.items()):
                    for status in action:
                        yield from self.fixstatus(status, fname, "meta")
                    yield {'progress': 100*(i+1)/len(actions)}
            yield {'progress': 100}
            yield {'status': 'done', 'message': ''}

    # ------------------------------------------------------------------
    #   Write participants
    # ------------------------------------------------------------------

    @property
    def _phenotype_paths(self) -> Iterator[Path]:
        raise NotImplementedError

    # NOTE
    #   There are two different ages in the csv: AGE_AT_SCAN and
    #   AGE_AT_MPRAGE. I am only keeping one of both for now (hopefully
    #   they are not too far apart)

    class ParticipantsMapper(TableMapper):

        class Converter(TableMapper.Converter):
            NANs = TableMapper.Converter.NANs.union({'-9999'})

        iq_types = {
            'DAS_II_SA': 'DAS-II-SA',
            'WISC': 'WISC',
            'WISC_III': 'WISC-III',
            'WISC_III_DUTCH': 'WISC-III-DUTCH',
            'WISC_IV_FULL': 'WISC-IV_FULL',
            'WISC_IV_4_SUBTESTS': 'WISC-IV_SUB',
            'WAIS_III': 'WAIS-III',
            'HAWIK_IV': 'HAWIK-IV',
        }

        header = {
            'participant_id': 'SUB_ID',
            'site': 'SITE_ID',
            'sample': 'SITE_ID',
            'sex': 'SEX',
            'age': 'AGE_AT_SCAN',
            'handedness': 'HANDEDNESS_CATEGORY',
            'handedness_score': 'HANDEDNESS_SCORES',
            'group': 'DX_GROUP',
            'dsm4': 'DSM_IV_TR',
            'fiq': 'FIQ',
            'viq': 'VIQ',
            'piq': 'PIQ',
            'fiq_type': 'FIQ_TEST_TYPE',
            'viq_type': 'VIQ_TEST_TYPE',
            'piq_type': 'PIQ_TEST_TYPE',
            'scq': 'SCQ_TOTAL',
            'aq': 'AQ_TOTAL',
            'comorbidity': 'COMORBIDITY',
            'med_status': 'CURRENT_MED_STATUS',
            'med_name': 'MEDICATION_NAME',
            'off_stimulant': 'OFF_STIMULANTS_AT_SCAN',
            'eye_status': 'EYE_STATUS_AT_SCAN',
            'bmi': 'BMI',
        }

        row = {
            'site': {
                'CALTECH': 'Caltech',
                'LEUVEN_1': 'Leuven',
                'LEUVEN_2': 'Leuven',
                'MAX_MUN': 'MaxMun',
                'PITT': 'Pitt',
                'STANFORD': 'Stanford',
                'TRINITY': 'Trinity',
                'UCLA_1': 'UCLA',
                'UCLA_2': 'UCLA',
                'UM_1': 'UM',
                'UM_2': 'UM',
                'YALE': 'Yale',
            },
            'sample': {
                'LEUVEN_1': 'Leuven1',
                'LEUVEN_2': 'Leuven2',
                'UCLA_1': 'UCLA1',
                'UCLA_2': 'UCLA2',
                'UM_1': 'UM1',
                'UM_2': 'UM2',
                'CALTECH': 'Caltech',
                'MAX_MUN': 'MaxMun',
                'PITT': 'Pitt',
                'STANFORD': 'Stanford',
                'TRINITY': 'Trinity',
                'YALE': 'Yale',
            },
            'sex': {'1': 'M', '2': 'F'},
            'handedness': {'Ambi': 'B'},
            'group': {'1': 'A', '2': 'C'},
            'fiq_type': iq_types,
            'viq_type': iq_types,
            'piq_type': iq_types,
            'med_status': {'0': 'N', '1': 'Y'},
            'off_stimulant': {'0': 'N', '1': 'Y'},
            'eye_status': {'1': 'O', '2': 'C'},
        }

    def make_participants(self) -> Action:
        input_paths = list(self._phenotype_paths)
        output_path = self.raw / 'participants.tsv'
        mapper = self.ParticipantsMapper

        def action_tsv(opath):

            def yield_rows():
                for i, input_path in enumerate(input_paths):
                    with open(input_path, newline='') as textio:
                        csvio = csv.reader(textio)
                        yield from mapper.remap(csvio, header=(i == 0))

            write_tsv(yield_rows(), opath)

        return Action(input_paths, output_path, action_tsv, input="path",
                      compare=True)

    # ------------------------------------------------------------------
    #   Write phenotypes
    # ------------------------------------------------------------------

    class ADIRMapper(TableMapper):

        class Converter(TableMapper.Converter):
            NANs = TableMapper.Converter.NANs.union({'-9999'})

        header = {
            'participant_id': 'SUB_ID',
            'social': 'ADI_R_SOCIAL_TOTAL_A',
            'verbal': 'ADI_R_VERBAL_TOTAL_BV',
            'rrb': 'ADI_RRB_TOTAL_C',
            'onset': 'ADI_R_ONSET_TOTAL_D',
            'reliable': 'ADI_R_RSRCH_RELIABLE',
        }
        row = {'reliable': {'0': 'N', '1': 'Y'}}

    class ADOSMapper(TableMapper):

        class Converter(TableMapper.Converter):
            NANs = TableMapper.Converter.NANs.union({'-9999'})

        header = {
            'participant_id': 'SUB_ID',
            'module': 'ADOS_MODULE',
            'total': 'ADOS_TOTAL',
            'comm': 'ADOS_COMM',
            'social': 'ADOS_SOCIAL',
            'stereo_behav': 'ADOS_STEREO_BEHAV',
            'reliable': 'ADOS_RSRCH_RELIABLE',
        }
        row = {'reliable': {'0': 'N', '1': 'Y'}}

    class ADOSGothamMapper(TableMapper):

        class Converter(TableMapper.Converter):
            NANs = TableMapper.Converter.NANs.union({'-9999'})

        header = {
            'participant_id': 'SUB_ID',
            'soc_affect': 'ADOS_GOTHAM_SOCAFFECT',
            'rrb': 'ADOS_GOTHAM_RRB',
            'total': 'ADOS_GOTHAM_TOTAL',
            'severity': 'ADOS_GOTHAM_SEVERITY',
        }

    class SRSMapper(TableMapper):

        class Converter(TableMapper.Converter):
            NANs = TableMapper.Converter.NANs.union({'-9999'})

        header = {
            'participant_id': 'SUB_ID',
            'version': 'SRS_VERSION',
            'raw_total': 'SRS_RAW_TOTAL',
            'awareness': 'SRS_AWARENESS',
            'cognition': 'SRS_COGNITION',
            'communication': 'SRS_COMMUNICATION',
            'motivation': 'SRS_MOTIVATION',
            'mannerisms': 'SRS_MANNERISMS',
        }
        row = {'version': {'1': 'C', '2': 'A'}}

    class VinelandMapper(TableMapper):

        class Converter(TableMapper.Converter):
            NANs = TableMapper.Converter.NANs.union({'-9999'})

        header = {
            'participant_id': 'SUB_ID',
            'receptive': 'VINELAND_RECEPTIVE_V_SCALED',
            'expressive': 'VINELAND_EXPRESSIVE_V_SCALED',
            'written': 'VINELAND_WRITTEN_V_SCALED',
            'communication': 'VINELAND_COMMUNICATION_STANDARD',
            'personal': 'VINELAND_PERSONAL_V_SCALED',
            'domestic': 'VINELAND_DOMESTIC_V_SCALED',
            'community': 'VINELAND_COMMUNITY_V_SCALED',
            'dailylvng': 'VINELAND_DAILYLVNG_STANDARD',
            'interpersonal': 'VINELAND_INTERPERSONAL_V_SCALED',
            'play': 'VINELAND_PLAY_V_SCALED',
            'coping': 'VINELAND_COPING_V_SCALED',
            'social': 'VINELAND_SOCIAL_STANDARD',
            'sum': 'VINELAND_SUM_SCORES',
            'abc': 'VINELAND_ABC_STANDARD',
            'informant': 'VINELAND_INFORMANT',
        }
        row = {'informant': {'1': 'P', '2': 'S'}}

    class WISC4Mapper(TableMapper):

        class Converter(TableMapper.Converter):
            NANs = TableMapper.Converter.NANs.union({'-9999'})

        header = {
            'participant_id': 'SUB_ID',
            'vci': 'WISC_IV_VCI',
            'pri': 'WISC_IV_PRI',
            'wmi': 'WISC_IV_WMI',
            'psi': 'WISC_IV_PSI',
            'sim': 'WISC_IV_SIM_SCALED',
            'vocab': 'WISC_IV_VOCAB_SCALED',
            'info': 'WISC_IV_INFO_SCALED',
            'blk_dsn': 'WISC_IV_BLK_DSN_SCALED',
            'pic_con': 'WISC_IV_PIC_CON_SCALED',
            'matrix': 'WISC_IV_MATRIX_SCALED',
            'digit_span': 'WISC_IV_DIGIT_SPAN_SCALED',
            'let_num': 'WISC_IV_LET_NUM_SCALED',
            'coding': 'WISC_IV_CODING_SCALED',
            'sym': 'WISC_IV_SYM_SCALED',
        }

    PHENOTYPES: dict[str, TableMapper] = {
        'adir': ADIRMapper,
        'ados': ADOSMapper,
        'ados_gotham': ADOSGothamMapper,
        'srs': SRSMapper,
        'vineland': VinelandMapper,
        'wisc_iv': WISC4Mapper,
    }

    def make_phenotypes(self) -> Iterator[Action]:
        for pheno in self.PHENOTYPES:
            yield from self._make_phenotype(pheno)

    def _make_phenotype(self, pheno) -> Iterator[Action]:
        mapper: TableMapper = self.PHENOTYPES[pheno]
        input_paths = list(self._phenotype_paths)
        output_path = self.layout.phenotype / f'{pheno}.tsv'
        input_json_path = self.TPLDIR / 'phenotype' / f'{pheno}.json'
        output_json_path = self.layout.phenotype / f'{pheno}.json'

        def action_tsv(opath: Path) -> None:

            def yield_rows() -> Iterator[list[str]]:
                for i, input_path in enumerate(input_paths):
                    with open(input_path, newline='') as textio:
                        csvio = csv.reader(textio)
                        yield from mapper.remap(csvio, header=(i == 0))

            write_tsv(yield_rows(), opath)

        if self.json != 'only':
            yield Action(input_paths, output_path, action_tsv,
                         input="path", compare=True)
        if self.json != 'no':
            yield CopyJSON(input_json_path, output_json_path, compare=True)
