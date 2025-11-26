import csv
from logging import getLogger
from pathlib import Path
from typing import Iterable, Iterator, Literal

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
from brainspresso.datasets.ABIDE.I.keys import allkeys

lg = getLogger(__name__)


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

    # ------------------------------------------------------------------
    #   Initialise
    # ------------------------------------------------------------------
    def __init__(
        self,
        root: Path,
        *,
        keys: Iterable[str] = {"all"},
        exclude_keys: Iterable[str] = set(),
        subs: Iterable[int] = tuple(),
        exclude_subs: Iterable[int] = tuple(),
        json: Literal["yes", "no", "only"] | bool = True,
        ifexists: IfExists.Choice = "skip",
    ):
        self.root: Path = Path(root)
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
        self.src = self.root / 'sourcedata'
        self.raw = self.root / 'rawdata'
        self.pheno = self.root / 'phenotype'
        self.drv = self.root / 'derivatives'
        self.drvmap = {
            'fs': self.drv / 'abide-freesurfer',
            'ants': self.drv / 'abide-ants',
            'civet': self.drv / 'abide-civet',
            'ccs': self.drv / 'abide-ccs',
            'cpac': self.drv / 'abide-cpac',
            'dparsf': self.drv / 'abide-dparsf',
            'niak': self.drv / 'abide-niak',
            'proc-min': self.drv / 'abide-processed',
            'qa-man': self.drv / 'abide-qa-man',
            'qa-pcp': self.drv / 'abide-qa-pcp',
        }
        # Track errors
        self.nb_errors = 0
        self.nb_skipped = 0

    # ------------------------------------------------------------------
    #   Run all actions
    # ------------------------------------------------------------------
    def run(self) -> None:
        """Run all actions"""
        self.init()
        with self.out as self.out:
            self._run()

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
                        self.root / 'README',
                    ),
                'dataset_description.json':
                    CopyJSON(
                        self.TPLDIR / 'dataset_description.json',
                        self.root / 'dataset_description.json',
                    ),
                'participants.json':
                    CopyJSON(
                        self.TPLDIR / 'participants.json',
                        self.root / 'participants.json',
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
    #   they are not too far appart)

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
        output_path = self.root / 'participants.tsv'
        mapper = self.ParticipantsMapper

        def action_tsv(opath):

            def yield_rows():
                for i, input_path in enumerate(input_paths):
                    with open(input_path, newline='') as textio:
                        csvio = csv.reader(textio)
                        yield from mapper.remap(csvio, header=(i == 0))

            write_tsv(yield_rows(), opath)

        return Action(input_paths, output_path, action_tsv, input="path")

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
        output_path = self.root / 'phenotype' / f'{pheno}.tsv'
        input_json_path = self.TPLDIR / 'phenotype' / f'{pheno}.json'
        output_json_path = self.root / 'phenotype' / f'{pheno}.json'

        def action_tsv(opath: Path) -> None:

            def yield_rows() -> Iterator[list[str]]:
                for i, input_path in enumerate(input_paths):
                    with open(input_path, newline='') as textio:
                        csvio = csv.reader(textio)
                        yield from mapper.remap(csvio, header=(i == 0))

            write_tsv(yield_rows(), opath)

        if self.json != 'only':
            yield Action(input_paths, output_path, action_tsv, input="path")
        if self.json != 'no':
            yield CopyJSON(input_json_path, output_json_path)
