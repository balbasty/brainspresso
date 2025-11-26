import tarfile
import csv
from logging import getLogger
from pathlib import Path, PosixPath
from typing import Iterable, Iterator, Literal

from brainspresso.utils.tabular import bidsify_tab
from brainspresso.utils.tabular import Status
from brainspresso.utils.io import write_tsv
from brainspresso.utils.io import write_from_buffer
from brainspresso.utils.tsv import TableMapper
from brainspresso.utils.keys import compat_keys
from brainspresso.utils.keys import lower_keys
from brainspresso.actions import IfExists
from brainspresso.actions import Action
from brainspresso.actions import CopyJSON
from brainspresso.actions import CopyBytes
from brainspresso.datasets.ABIDE.II.keys import allleaves, allkeys

lg = getLogger(__name__)


class Bidsifier:
    """ABIDE-II - bidsifying logic"""

    # ------------------------------------------------------------------
    #   Constants
    # ------------------------------------------------------------------

    # Folder containing template README/JSON/...
    TPLDIR = Path(__file__).parent / 'templates'

    SITES = (
        'BNI', 'EMC', 'ETH', 'GU', 'IU', 'IP', 'KUL', 'KKI', 'NYU', 'ONRC',
        'OHSU', 'TCD', 'SDSU', 'SU', 'UCD', 'UCLA', 'U_MIA', 'USM', 'UPSM'
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
        # Metadata
        self.nb_errors = self.nb_skipped = 0
        for status in self.make_meta():
            status.setdefault('modality', 'meta')
            self.out(status)

        # Raw and lightly processed data are stored in the same archive
        rawkeys = (allleaves - lower_keys('derivatives')) - lower_keys('meta')
        if rawkeys:
            for status in self.make_raw(rawkeys):
                self.out(status)

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
    def make_raw(self, keys):
        # Run actions
        progress = 0
        yield {'progress': progress}
        paths = self.src.glob('*.tar.gz')
        for path in paths:
            try:
                with tarfile.open(path, 'r:gz') as tar:
                    for action in self._make_raw(tar, keys):
                        for status in action:
                            yield from self.fixstatus(status, action.dst.name)
                        progress += 1
                        yield {'progress': progress}
            except Exception as e:
                lg.error(f"{path}: {e}")
        yield {'status': 'done', 'message': ''}

    def _make_raw(self, tar, keys):
        for member in tar.getmembers():
            memberpath = PosixPath(member.name)
            if not memberpath.name.endswith(('.nii.gz', '.bval', '.bvec')):
                return
            site, id, ses, mod, fname = memberpath.parts
            site = site[8:]
            site, sample = site.split('_')
            if site == "PITT":
                site = "UPSM"
            id = int(id)
            ses = int(ses.split('_')[-1])
            base, *ext = fname.split(".")
            ext = ".".join(["", *ext])
            if self.subs and id not in self.subs:
                return
            if self.exclude_subs and id in self.exclude_subs:
                return
            if base == 'anat':
                if "T1w" not in keys:
                    return
                cat = 'anat'
                mod = 'T1w'
                json = self.TPLDIR / site / sample / 'T1w.json'
            elif base == 'rest':
                if "rest" not in keys:
                    return
                cat = 'func'
                mod = 'task-rest_bold'
                json = self.TPLDIR / site / sample / 'bold.json'
            elif base == 'dti':
                if "dti" not in keys:
                    return
                cat = 'dwi'
                mod = 'dwi'
                json = self.TPLDIR / site / sample / 'dwi.json'
            else:
                return
            dst = self.raw / f'sub-{id:05d}' / f"ses-{ses:02d}" / cat
            if self.json != 'only':
                yield Action(
                    tar.name, dst / f'sub-{id:05d}_ses-{ses:02d}_{mod}{ext}',
                    lambda f: write_from_buffer(tar.extractfile(member), f)
                )
            if self.json != 'no' and ext == ".nii.gz":
                yield CopyJSON(
                    json, dst / f'sub-{id:05d}_ses-{ses:02d}_{mod}.json'
                )

    # ------------------------------------------------------------------
    #   Tabular helper
    # ------------------------------------------------------------------

    class ABIDE2Mapper(TableMapper):

        class Converter(TableMapper.Converter):
            NANs = TableMapper.Converter.NANs.union({'-9999'})

    # ------------------------------------------------------------------
    #   Write participants
    # ------------------------------------------------------------------

    # NOTE
    #   There are two different ages in the csv: AGE_AT_SCAN and
    #   AGE_AT_MPRAGE. I am only keeping one of both for now (hopefully
    #   they are not too far appart)

    class ParticipantsMapper(ABIDE2Mapper):

        iq_types = {
            'DAS-School': 'DAS-S',
            'DAS-PreSchool': 'DAS-PS',
            'DAS-II school age': 'DAS-2-SA',
            'DAS-II early year': 'DAS-2-EY',
            'KBIT-2': 'KBIT-2',
            'Raven': 'Raven',
            'SON-R': 'SON-R',
            'WAIS-III': 'WAIS-3',
            'WAIS-IV': 'WAIS-4',
            'WAIS-IV-NL': 'WAIS-4-NL',
            'WAIS-IV-NL (10 subtests)': 'WAIS-4-NL_SUB10',
            'WAIS-IV-NL (3 subtests)': 'WAIS-4-NL_SUB3',
            'WASI': 'WASI',
            'WASI-II': 'WASI-2',
            'WISC-III': 'WISC-3',
            'WISC-IV': 'WISC-4',
            'WISC-V': 'WISC-5',
            'WPPSI-III': 'WPPSI-3',
        }

        header = {
            'participant_id': 'SUB_ID',
            'site': 'SITE_ID',
            'sample': 'SITE_ID',
            'ndar_guid': 'NDAR_GUID',
            'sex': 'SEX',
            'age': 'AGE_AT_SCAN',
            'handedness': 'HANDEDNESS_CATEGORY',
            'handedness_score': 'HANDEDNESS_SCORES',
            'group': 'DX_GROUP',
            'dsm4': 'PDD_DSM_IV_TR',
            'dsm5': 'ASD_DSM_5',
            'fiq': 'FIQ',
            'viq': 'VIQ',
            'piq': 'PIQ',
            'fiq_type': 'FIQ_TEST_TYPE',
            'viq_type': 'VIQ_TEST_TYPE',
            'piq_type': 'PIQ_TEST_TYPE',
            'scq_version': 'SCQ_VERSION',
            'scq': 'SCQ_TOTAL',
            'aq': 'AQ_TOTAL',
            'nonasd_psydx_icd9code': 'NONASD_PSYDX_ICD9CODE',
            'nonasd_psydx_label': 'NONASD_PSYDX_LABEL',
            'med_status': 'CURRENT_MED_`STATUS',
            'med_name': 'CURRENT_MEDICATION_NAME',
            'off_stimulant': 'OFF_STIMULANTS_AT_SCAN',
            'eye_status': 'EYE_STATUS_AT_SCAN',
            'bmi': 'BMI',
        }

        row = {
            'site': {
                'ABIDE-I_UCLA_1': 'UCLA',
                'ABIDE-I_UCLA_2': 'UCLA',
                'ABIDE-I_PITT': 'UPSM',
                'ABIDE-II_BNI_1': 'BNI',
                'ABIDE-II_EMC_1': 'EMC',
                'ABIDE-II_ETH_1': 'ETH',
                'ABIDE-II_GU_1': 'GU',
                'ABIDE-II_IP_1': 'IP',
                'ABIDE-II_IU_1': 'IU',
                'ABIDE-II_KKI_1': 'KKI',
                'ABIDE-II_KUL_3': 'KUL',
                'ABIDE-II_NYU_2': 'NYU',
                'ABIDE-II_OHSU_1': 'OHSU',
                'ABIDE-II_ONRC_1': 'ONRC',
                'ABIDE-II_ONRC_2': 'ONRC',
                'ABIDE-II_SDSU_1': 'SDSU',
                'ABIDE-II_SU_1': 'SU',
                'ABIDE-II_SU_2': 'SU',
                'ABIDE-II_TCD_1': 'TCD',
                'ABIDE-II_UCD_1': 'UCD',
                'ABIDE-II_UCLA_Long': 'UCLA',
                'ABIDE-II_UPSM_Long': 'UPSM',
                'ABIDE-II_USM_1': 'USM',
                'ABIDE-II_U_MIA_1': 'UMia',
            },
            'sample': {
                'ABIDE-I_UCLA_1': 'UCLA1',
                'ABIDE-I_UCLA_2': 'UCLA2',
                'ABIDE-I_PITT': 'UPSM1',
                'ABIDE-II_BNI_1': 'BNI',
                'ABIDE-II_EMC_1': 'EMC',
                'ABIDE-II_ETH_1': 'ETH',
                'ABIDE-II_GU_1': 'GU',
                'ABIDE-II_IP_1': 'IP',
                'ABIDE-II_IU_1': 'IU',
                'ABIDE-II_KKI_1': 'KKI',
                'ABIDE-II_KUL_3': 'KUL',
                'ABIDE-II_NYU_2': 'NYU',
                'ABIDE-II_OHSU_1': 'OHSU',
                'ABIDE-II_ONRC_1': 'ONRC1',
                'ABIDE-II_ONRC_2': 'ONRC2',
                'ABIDE-II_SDSU_1': 'SDSU',
                'ABIDE-II_SU_1': 'SU1',
                'ABIDE-II_SU_2': 'SU2',
                'ABIDE-II_TCD_1': 'TCD',
                'ABIDE-II_UCD_1': 'UCD',
                'ABIDE-II_UCLA_Long': 'UCLALong',
                'ABIDE-II_UPSM_Long': 'UPSM',
                'ABIDE-II_USM_1': 'USM',
                'ABIDE-II_U_MIA_1': 'UMia',
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

    def make_participants(self):
        input_path = self.src / 'Phenotypic_V1_0b.csv'
        output_path = self.root / 'participants.tsv'

        def action_tsv(opath):

            def yield_rows():
                with open(input_path, newline='') as textio:
                    csvio = csv.reader(textio)
                    yield from self.ParticipantsMapper.remap(csvio)

            write_tsv(yield_rows(), opath)

        return Action(input_path, output_path, action_tsv, input="path")

    # ------------------------------------------------------------------
    #   Write phenotypes
    # ------------------------------------------------------------------

    class ADIRMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'social': 'ADI_R_SOCIAL_TOTAL_A',
            'verbal': 'ADI_R_VERBAL_TOTAL_BV',
            'nonverbal': 'ADI_R_NONVERBAL_TOTAL_BV',
            'rrb': 'ADI_R_RRB_TOTAL_C',
            'onset': 'ADI_R_ONSET_TOTAL_D',
            'reliable': 'ADI_R_RSRCH_RELIABLE',
        }
        row = {'reliable': {'0': 'N', '1': 'Y'}}

    class ADIRa1Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'direct_gaze': 'ADI_R_A1_DIRECT_GAZE',
            'social_smile': 'ADI_R_A1_SOCIAL_SMILE',
            'facial_expressions': 'ADI_R_A1_FACIAL_EXPRESSIONS',
            'total': 'ADI_R_A1_TOTAL',
        }

    class ADIRa2Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'imaginative_play': 'ADI_R_A2_IMAGINATIVE_PLAY',
            'interest_in_children': 'ADI_R_A2_INTEREST_IN_CHILDREN',
            'response_to_approaches': 'ADI_R_A2_RESPONSE_TO_APPROACHES',
            'group_play': 'ADI_R_A2_GROUP_PLAY',
            'higher': 'ADI_R_A2_HIGHER',
            'friendships': 'ADI_R_A2_FRIENDSHIPS',
            'total': 'ADI_R_A2_TOTAL',
        }

    class ADIRa3Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'showing_directing_attention':
                'ADI_R_A3_SHOWING_DIRECTING_ATTENTION',
            'offering_to_share': 'ADI_R_A3_OFFERING_TO_SHARE',
            'seeking_share_enjoyment': 'ADI_R_A3_SEEKING_SHARE_ENJOYMENT',
            'total': 'ADI_R_A3_Total',
        }

    class ADIRa4Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'use_others_body': 'ADI_R_A4_USE_OTHERS_BODY',
            'offering_comfort': 'ADI_R_A4_OFFERING_COMFORT',
            'quality_social_overtures': 'ADI_R_A4_QUALITY_SOCIAL_OVERTURES',
            'inappropriate_facial_expressions':
                'ADI_R_A4_INAPPROPRIATE_FACIAL_EXPRESSIONS',
            'appropriate_social_responses':
                'ADI_R_A4_APPROPRIATE_SOCIAL_RESPONSES',
            'total': 'ADI_R_A4_Total',
        }

    class ADIRb1Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'pointing_express_interest': 'ADI_R_B1_POINTING_EXPRESS_INTEREST',
            'nodding': 'ADI_R_B1_NODDING',
            'head_shaking': 'ADI_R_B1_HEAD_SHAKING',
            'conventional_gestures': 'ADI_R_B1_CONVENTIONAL_GESTURES',
            'total': 'ADI_R_B1_TOTAL',
        }

    class ADIRb2Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'social_verbalization': 'ADI_R_B2_SOCIAL_VERBALIZATION',
            'reciprocal_conversation': 'ADI_R_B2_RECIPROCAL_CONVERSATION',
            'total': 'ADI_R_B2_TOTAL',
        }

    class ADIRb3Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'stereotyped_utterances': 'ADI_R_B3_STEREOTYPED_UTTERANCES',
            'inappropriate_questions': 'ADI_R_B3_INAPPROPRIATE_QUESTIONS',
            'pronominal_reversal': 'ADI_R_B3_PRONOMINAL_REVERSAL',
            'neologisms': 'ADI_R_B3_NEOLOGISMS',
            'total': 'ADI_R_B3_Total',
        }

    class ADIRc1Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'unusual_preoccupations': 'ADI_R_C1_UNUSUAL_PREOCCUPATIONS',
            'cicumscribed_interests': 'ADI_R_C1_CIRCUMSCRIBED_INTERESTS',
            'total': 'ADI_R_C1_TOTAL',
        }

    class ADIRc2Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'verbal_rituals': 'ADI_R_C2_VERBAL_RITUALS',
            'compulsions': 'ADI_R_C2_COMPULSIONS',
            'total': 'ADI_R_C2_TOTAL',
        }

    class ADIRc3Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'hand_finger_mannerisms': 'ADI_R_C3_HAND_FINGER_MANNERISMS',
            'higher': 'ADI_R_C3_HIGHER',
            'other_complex_mannerisms': 'ADI_R_C3_OTHER_COMPLEX_MANNERISMS',
            'total': 'ADI_R_C3_TOTAL',
        }

    class ADIRc4Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'repetitive_use_objects': 'ADI_R_C4_REPETITIVE_USE_OBJECTS',
            'higher': 'ADI_R_C4_HIGHER',
            'unusual_sensory_interests': 'ADI_R_C4_UNUSUAL_SENSORY_INTERESTS',
            'total': 'ADI_R_C4_TOTAL',
        }

    class ADIRdMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'age_parent_noticed': 'ADI_R_D_AGE_PARENT_NOTICED',
            'age_first_single_words': 'ADI_R_D_AGE_FIRST_SINGLE_WORDS',
            'age_first_phrases': 'ADI_R_D_AGE_FIRST_PHRASES',
            'age_when_abnormality': 'ADI_R_D_AGE_WHEN_ABNORMALITY',
            'interviewer_judgement': 'ADI_R_D_INTERVIEWER_JUDGMENT',
        }

    class ADOS2Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'module': 'ADOS_MODULE',
            'social_affect': 'ADOS_2_SOCAFFECT',
            'rrb': 'ADOS_2_RRB',
            'total': 'ADOS_2_TOTAL',
            'severity': 'ADOS_2_SEVERITY_TOTAL',
            'reliable': 'ADOS_RSRCH_RELIABLE',
        }
        row = {'reliable': {'0': 'N', '1': 'Y'}}

    class ADOSGenericMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'module': 'ADOS_MODULE',
            'total': 'ADOS_G_TOTAL',
            'comm': 'ADOS_G_COMM',
            'social': 'ADOS_G_SOCIAL',
            'stereo_behav': 'ADOS_G_STEREO_BEHAV',
            'creativity': 'ADOS_G_CREATIVITY',
            'reliable': 'ADOS_RSRCH_RELIABLE',
        }
        row = {'reliable': {'0': 'N', '1': 'Y'}}

    class SRSMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'version': 'SRS_VERSION',
            'informant': 'SRS_INFORMANT',
            'total_raw': 'SRS_TOTAL_RAW',
            'awareness_raw': 'SRS_AWARENESS_RAW',
            'cognition_raw': 'SRS_COGNITION_RAW',
            'communication_raw': 'SRS_COMMUNICATION_RAW',
            'motivation_raw': 'SRS_MOTIVATION_RAW',
            'mannerisms_raw': 'SRS_MANNERISMS_RAW',
            'total_t': 'SRS_TOTAL_T',
            'awareness_t': 'SRS_AWARENESS_T',
            'cognition_t': 'SRS_COGNITION_T',
            'communication_t': 'SRS_COMMUNICATION_T',
            'motivation_t': 'SRS_MOTIVATION_T',
            'mannerisms_t': 'SRS_MANNERISMS_T',
        }
        row = {'version': {'1': 'C', '2': 'A'}}

    class VinelandMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'receptive': 'VINELAND_RECEPTIVE_V_SCALED',
            'expressive': 'VINELAND_EXPRESSIVE_V_SCALED',
            'written': 'VINELAND_WRITTEN_V_SCALED',
            'communication': 'VINELAND_COMMUNICATION_STANDARD',
            'personal': 'VINELAND_PERSONAL_V_SCALED',
            'domestic': 'VINELAND_DOMESTIC_V_SCALED',
            'community': 'VINELAND_COMMUNITY_V_SCALED',
            'dailylvng': 'VINELAND_DAILYLIVING_STANDARD',
            'interpersonal': 'VINELAND_INTERPERSONAL_V_SCALED',
            'play': 'VINELAND_PLAY_V_SCALED',
            'coping': 'VINELAND_COPING_V_SCALED',
            'social': 'VINELAND_SOCIAL_STANDARD',
            'gross': 'VINELAND_GROSS_V_SCALED',
            'fine': 'VINELAND_FINE_V_SCALED',
            'motor': 'VINELAND_MOTOR_STANDARD',
            'sum': 'VINELAND_SUM_SCORES',
            'abc': 'VINELAND_ABC_Standard',
            'informant': 'VINELAND_INFORMANT',
        }
        row = {'informant': {'1': 'P', '2': 'S', '3': 'O'}}

    class RBSR6Mapper(ABIDE2Mapper):

        headers = {
            'participant_id': 'SUB_ID',
            'stereotyped': 'RBSR_6SUBSCALE_STEREOTYPED',
            'self_injurious': 'RBSR_6SUBSCALE_SELF-INJURIOUS',
            'compulsive': 'RBSR_6SUBSCALE_COMPULSIVE',
            'ritualistic': 'RBSR_6SUBSCALE_RITUALISTIC',
            'sameness': 'RBSR_6SUBSCALE_SAMENESS',
            'restricted': 'RBSR_6SUBSCALE_RESTRICTED',
            'total': 'RBSR_6SUBSCALE_TOTAL',
        }

    class RBSR5Mapper(ABIDE2Mapper):

        headers = {
            'participant_id': 'SUB_ID',
            'stereotypic': 'RBSR_5SUBSCALE_STEREOTYPIC',
            'self_injurious': 'RBSR_5SUBSCALE_SELF-INJURIOUS',
            'compulsive': 'RBSR_5SUBSCALE_COMPULSIVE',
            'ritualistic': 'RBSR_5SUBSCALE_RITUAL   ISTIC',
            'restricted': 'RBSR_5SUBSCALE_RESTRICTED',
            'total': 'RBSR_5SUBSCALE_TOTAL',
        }

    class MASCMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'edition': 'MASC_EDITION',
            'total': 'MASC_TOTAL_T',
            'physical_tr': 'MASC_T/R_T',
            'physical_sa': 'MASC_S/A_T',
            'physical': 'MASC_PHYSICAL_TOTAL_T',
            'harm_perf': 'MASC_PER_T',
            'harm_ac': 'MASC_AC_T',
            'harm': 'MASC_HARM_TOTAL_T',
            'social_hr': 'MASC_H/R_T',
            'social_perf': 'MASC_PP_T',
            'social': 'MASC_SOCIAL_TOTAL_T',
            'separation_panic': 'MASC_SEP_T',
            'adi': 'MASC_ADI_T',
            'inconsistency_score': 'MASC_INCONSISTENCY_SCORE',
        }

    class BRIEFMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'version': 'BRIEF_VERSION',
            'informant': 'BRIEF_INFORMANT',
            'inhibit': 'BRIEF_INHIBIT_T',
            'shift': 'BRIEF_SHIFT_T',
            'emotional': 'BRIEF_EMOTIONAL_T',
            'bri': 'BRIEF_BRI_T',
            'initiate': 'BRIEF_INITIATE_T',
            'working': 'BRIEF_WORKING_T',
            'plan': 'BRIEF_PLAN_T',
            'organization': 'BRIEF_ORGANIZATION_T',
            'monitor': 'BRIEF_MONITOR_T',
            'mi': 'BRIEF_MI_T',
            'gec': 'BRIEF_GEC_T',
            'inconsistency_score': 'BRIEF_INCONSISTENCY_SCORE',
            'negativity_score': 'BRIEF_NEGATIVITY_SCORE',
        }
        row = {'version': {'1': 'C', '2': 'A'}}

    class CBCLOver6Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'activities': 'CBCL_6-18_ACTIVITIES_T',
            'social': 'CBCL_6-18_SOCIAL_T',
            'school': 'CBCL_6-18_SCHOOL_T',
            'total_competence': 'CBCL_6-18_TOTAL_COMPETENCE_T',
            'anxious': 'CBCL_6-18_ANXIOUS_T',
            'withdrawn': 'CBCL_6-18_WITHDRAWN_T',
            'somatic_complaint': 'CBCL_6-18_SOMATIC_COMPAINT_T',
            'social_problem': 'CBCL_6-18_SOCIAL_PROBLEM_T',
            'thought': 'CBCL_6-18_THOUGHT_T',
            'attention': 'CBCL_6-18_ATTENTION_T',
            'rule': 'CBCL_6-18_RULE_T',
            'aggressive': 'CBCL_6-18_AGGRESSIVE_T',
            'internal': 'CBCL_6-18_INTERNAL_T',
            'external': 'CBCL_6-18_EXTERNAL_T',
            'total_problem': 'CBCL_6-18_TOTAL_PROBLEM_T',
            'affective': 'CBCL_6-18_AFFECTIVE_T',
            'anxiety': 'CBCL_6-18_ANXIETY_T',
            'somatic_problem': 'CBCL_6-18_SOMATIC_PROBLEM_T',
            'attention_deficit': 'CBCL_6-18_ATTENTION_DEFICIT_T',
            'oppositional': 'CBCL_6-18_OPPOSITIONAL_T',
            'conduct': 'CBCL_6-18_CONDUCT_T',
            'sluggish': 'CBCL_6-18_SLUGGISH_T',
            'obsessive': 'CBCL_6-18_OBSESSIVE_T',
            'post_traumatic': 'CBCL_6-18_POST_TRAUMATIC_T',
        }

    class CBCLUnder5Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'emotion': 'CBCL_1.5-5_EMOTION_T',
            'anxious': 'CBCL_1.5-5_ANXIOUS_T',
            'somatic': 'CBCL_1.5-5_SOMANTIC_T',
            'withdrawn': 'CBCL_1.5-5_WITHDRAWN_T',
            'sleep': 'CBCL_1.5-5_SLEEP_T',
            'attention_problem': 'CBCL_1.5-5_ATTENTION_PROBLEM_T',
            'aggressive': 'CBCL_1.5-5_AGGRESSIVE_T',
            'internal': 'CBCL_1.5-5_INTERNAL_T',
            'external': 'CBCL_1.5-5_EXTERNAL_T',
            'total': 'CBCL_1.5-5_TOTAL_T',
            'stress': 'CBCL_1.5-5_STRESS_T',
            'affective': 'CBCL_1.5-5_AFFECTIVE_T',
            'anxiety': 'CBCL_1.5-5_ANXIETY_T',
            'pervasive': 'CBCL_1.5-5_PERVASIVE_T',
            'attention_deficit': 'CBCL_1.5-5_ATTENTION_DEFICIT_T',
            'oppositional': 'CBCL_1.5-5_OPPOSITIONAL_T',
        }

    class BDIMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'edition': 'BDI_EDITION',
            'total': 'BDI_TOTAL',
        }

    class VMIMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'edition': 'VMI_EDITION',
            'total': 'VMI_VMI_S',
        }

    class CELFOver9Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'edition': 'CELF_EDITION',
            'core': 'CELF_9-21_CORE_S',
            'receptive': 'CELF_9-21_RECEPTIVE_S',
            'expressive': 'CELF_9-21_EXPRESSIVE_S',
        }

    class CELFUnder8Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'edition': 'CELF_EDITION',
            'core': 'CELF_5-8_CORE_S',
            'receptive': 'CELF_5-8_RECEPTIVE_S',
            'expressive': 'CELF_5-8_EXPRESSIVE_S',
        }

    class BASCMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'version': 'BASC2_PRS_VERSION',
            'anger': 'BASC2_PRS_ANGER_T',
            'hyperactivity': 'BASC2_PRS_HYPERACTIVITY_T',
            'aggression': 'BASC2_PRS_AGGRESSION_T',
            'conduct': 'BASC2_PRS_CONDUCT_T',
            'external': 'BASC2_PRS_EXTERNAL_T',
            'external_mean': 'BASC2_PRS_EXTERNAL_MEAN_T',
            'anxiety': 'BASC2_PRS_ANXIETY_T',
            'depression': 'BASC2_PRS_DEPRESSION_T',
            'somatization': 'BASC2_PRS_SOMATIZATION_T',
            'internal': 'BASC2_PRS_INTERNAL_T',
            'internal_mean': 'BASC2_PRS_INTERNAL_MEAN_T',
            'atypicality': 'BASC2_PRS_ATYPICALITY_T',
            'withdrawal': 'BASC2_PRS_WITHDRAWAL_T',
            'attention': 'BASC2_PRS_ATTENTION_T',
            'bsi': 'BASC2_PRS_BSI_T',
            'bsi_mean': 'BASC2_PRS_BSI_MEAN_T',
            'adaptability': 'BASC2_PRS_ADAPTABILITY_T',
            'social': 'BASC2_PRS_SOCIAL_T',
            'leadership': 'BASC2_PRS_LEADERSHIP_T',
            'activities': 'BASC2_PRS_ACTIVITIES_T',
            'functional': 'BASC2_PRS_FUNCTIONAL_T',
            'adaptive': 'BASC2_PRS_ADAPTIVE_T',
            'adaptive_mean': 'BASC2_PRS_ADAPTIVE_MEAN_T',
            'bully': 'BASC2_PRS_BULLY_T',
            'dsd': 'BASC2_PRS_DSD_T',
            'emotional': 'BASC2_PRS_EMOTIONAL_T',
            'executive': 'BASC2_PRS_EXECUTIVE_T',
            'negative': 'BASC2_PRS_NEGATIVE_T',
            'resiliency': 'BASC2_PRS_RESILIENCY_T',
        }

    class CPRSMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'opp': 'CPRS_OPP',
            'cog_inatt': 'CPRS_COG-INATT',
            'hyperact': 'CPRS_HYPERACT',
            'anx_shy': 'CPRS_ANX_SHY',
            'perfect': 'CPRS_PERFECT',
            'social_prob': 'CPRS_SOCIAL_PROB',
            'psycho_somatic': 'CPRS_PSYCHO_SOMATIC',
            'conn_adhd': 'CPRS_CONN_ADHD',
            'rest_impul': 'CPRS_REST_IMPULS',
            'emot_lability': 'CPRS_EMOT_LABILITY',
            'conn_gi_total': 'CPRS_CONN_GI_TOTAL',
            'dsm_iv_inatt': 'CPRS_DSM_IV_INATT',
            'dsm_iv_hyper_impul': 'CPRS_DSM_IV_HYPER_IMPUL',
            'dsm_iv_total': 'CPRS_DSM_IV_TOTAL',
            'inatt_symptoms': 'CPRS_INATT_SYMPTOMS',
            'hyper_impul_symptoms': 'CPRS_HYPER_IMPUL_SYMPTOMS',
        }

    class CASIMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'adhd_i_cutoff': 'CASI_ADHD-I_CUTOFF',
            'adhd_h_cutoff': 'CASI_ADHD-H_CUTOFF',
            'adhd_c_cutoff': 'CASI_ADHD-C_CUTOFF',
            'odd_cutoff': 'CASI_ODD_CUTOFF',
            'cd_cutoff': 'CASI_CD_CUTOFF',
            'gad_cutoff': 'CASI_GAD_CUTOFF',
            'specific_phobia_cutoff': 'CASI_SPECIFIC_PHOBIA_CUTOFF',
            'obsessions_cutoff': 'CASI_OBSESSIONS_CUTOFF',
            'compulsions_cutoff': 'CASI_COMPULSIONS_CUTOFF',
            'ptsd_cutoff': 'CASI_PTSD_CUTOFF',
            'motor_tics_cutoff': 'CASI_MOTOR_TICS_CUTOFF',
            'vocal_tics_cutoff': 'CASI_VOCAL_TICS_CUTOFF',
            'social_phobia_cutoff': 'CASI_SOCIAL_PHOBIA_CUTOFF',
            'separation_cutoff': 'CASI_SEPARATION_CUTOFF',
            'schizophrenia_cutoff': 'CASI_SCHIZOPHRENIA_CUTOFF',
            'nocturnal_enuresis_cutoff': 'CASI_NOCTURNAL_ENURESIS_CUTOFF',
            'enuresis_encopresis_cutoff': 'CASI_ENURESIS_ENCOPRESIS_CUTOFF',
            'mde_cutoff': 'CASI_MDE_CUTOFF',
            'dysthymic_cutoff': 'CASI_DYSTHYMIC_CUTOFF',
            'autistic_cutoff': 'CASI_AUTISTIC_CUTOFF',
            'asperger_cutoff': 'CASI_ASPERGER_CUTOFF',
            'aspd_cutoff': 'CASI_ASPD_CUTOFF',
            'panic_attacks_cutoff': 'CASI_PANIC_ATTACKS_CUTOFF',
            'somatization_cutoff': 'CASI_SOMATIZATION_CUTOFF',
            'schizoid_personality_cutoff':
                'CASI_SCHIZOID_PERSONALITY_CUTOFF',
            'manic_episode_cutoff': 'CASI_MANIC_EPISODE_CUTOFF',
            'anorexia_nervosa_cutoff': 'CASI_ANOREXIA_NERVOSA_CUTOFF',
            'bulimia_nervosa_cutoff': 'CASI_BULIMIA_NERVOSA_CUTOFF',
            'substance_use_cutoff': 'CASI_SUBSTANCE_USE_CUTOFF',
        }

    class CSIMapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'adhd_i_severity': 'CSI_ADHD-I_SEVERITY',
            'adhd_i_cutoff': 'CSI_ADHD-I_CUTOFF',
            'adhd_h_severity': 'CSI_ADHD-H_SEVERITY',
            'adhd_h_cutoff': 'CSI_ADHD-H_CUTOFF',
            'adhd_c_severity': 'CSI_ADHD-C_SEVERITY',
            'adhd_c_cutoff': 'CSI_ADHD-C_CUTOFF',
            'odd_severity': 'CSI_ODD_SEVERITY',
            'odd_cutoff': 'CSI_ODD_ CUTOFF',
            'cd_severity': 'CSI_CD_SEVERITY',
            'cd_cutoff': 'CSI_CD_CUTOFF',
            'gad_severity': 'CSI_GAD_SEVERITY',
            'gad_cutoff': 'CSI_GAD_CUTOFF',
            'specific_phobia_severity':
                'CSI_SPECIFIC_PHOBIA_SEVERITY',
            'specific_phobia_cutoff':
                'CSI_SPECIFIC_PHOBIA_CUTOFF',
            'obsessions_severity': 'CSI_OBSESSIONS_SEVERITY',
            'obsessions_cutoff': 'CSI_OBSESSIONS_CUTOFF',
            'compulsions_severity': 'CSI_COMPULSIONS_SEVERITY',
            'compulsions_cutoff': 'CSI_COMPULSIONS_CUTOFF',
            'disturbing_events_severity':
                'CSI_DISTURBING_EVENTS_SEVERITY',
            'disturbing_events_cutoff':
                'CSI_DISTURBING_EVENTS_CUTOFF',
            'motor_tics_severity': 'CSI_MOTOR_TICS_SEVERITY',
            'motor_tics_cutoff': 'CSI_MOTOR_TICS_CUTOFF',
            'vocal_tics_severity': 'CSI_VOCAL_TICS_SEVERITY',
            'vocal_tics_cutoff': 'CSI_VOCAL_TICS_CUTOFF',
            'schizophrenia_severity': 'CSI_SCHIZOPHRENIA_SEVERITY',
            'schizophrenia_cutoff': 'CSI_SCHIZOPHRENIA_CUTOFF',
            'mdd_severity':  'CSI_MDD_SEVERITY',
            'mdd_cutoff':  'CSI_MDD_CUTOFF',
            'dysthymic_severity': 'CSI_DYSTHYMIC_SEVERITY',
            'dysthymic_cutoff': 'CSI_DYSTHYMIC_CUTOFF',
            'autistic_severity': 'CSI_AUTISTIC_SEVERITY',
            'autistic_cutoff': 'CSI_AUTISTIC_CUTOFF',
            'asperger_severity': 'CSI_ASPERGER_SEVERITY',
            'asperger_cutoff': 'CSI_ASPERGER_CUTOFF',
            'social_phobia_severity': 'CSI_SOCIAL_PHOBIA_SEVERITY',
            'social_phobia_cutoff': 'CSI_SOCIAL_PHOBIA_CUTOFF',
            'separation_severity': 'CSI_SEPARATION_SEVERITY',
            'separation_cutoff': 'CSI_SEPARATION_CUTOFF',
            'enuresis_severity': 'CSI_ENURESIS_SEVERITY',
            'enuresis_cutoff': 'CSI_ENURESIS_CUTOFF',
            'encopresis_severity': 'CSI_ENCOPRESIS_SEVERITY',
            'encopresis_cutoff': 'CSI_ENCOPRESIS_CUTOFF',
        }

    class WIAT2Mapper(ABIDE2Mapper):

        header = {
            'participant_id': 'SUB_ID',
            'word': 'A_WORD_T',
            'numerical': 'A_NUMERICAL_T',
            'spelling': 'A_SPELLING_T',
            'total_composite': 'A_TOTAL_COMPOSITE_S',
        }

    PHENOTYPES = {
        'adir': ADIRMapper,
        'adir_a1': ADIRa1Mapper,
        'adir_a2': ADIRa2Mapper,
        'adir_a3': ADIRa3Mapper,
        'adir_a4': ADIRa4Mapper,
        'adir_b1': ADIRb1Mapper,
        'adir_b2': ADIRb2Mapper,
        'adir_b3': ADIRb3Mapper,
        'adir_c1': ADIRc1Mapper,
        'adir_c2': ADIRc2Mapper,
        'adir_c3': ADIRc3Mapper,
        'adir_c4': ADIRc4Mapper,
        'adir_d': ADIRdMapper,
        'ados_2': ADOS2Mapper,
        'ados_generic': ADOSGenericMapper,
        'srs': SRSMapper,
        'rbsr6': RBSR6Mapper,
        'rbsr5': RBSR5Mapper,
        'masc': MASCMapper,
        'brief': BRIEFMapper,
        'cbcl_6-18': CBCLOver6Mapper,
        'cbcl_1.5-5': CBCLUnder5Mapper,
        'bdi': BDIMapper,
        'vmi': VMIMapper,
        'celf_9-21': CELFOver9Mapper,
        'celf_5-8': CELFUnder8Mapper,
        'basc': BASCMapper,
        'cprs': CPRSMapper,
        'casi': CASIMapper,
        'csi': CSIMapper,
        'wiat_2': WIAT2Mapper,
        'vineland': VinelandMapper,
    }

    def make_phenotypes(self):
        for base in self.PHENOTYPES:
            yield from self._make_phenotype(base)

    def _make_phenotype(self, base):
        mapper: TableMapper = self.PHENOTYPES[base]
        input_paths = list(self.src.glob('*.csv'))
        output_path = self.root / 'phenotype' / f'{base}.tsv'
        input_json_path = self.TPLDIR / 'phenotype' / f'{base}.json'
        output_json_path = self.root / 'phenotype' / f'{base}.json'

        def action_tsv(opath):

            def yield_rows():
                for input_path in input_paths:
                    with open(input_path, newline='') as textio:
                        csvio = csv.reader(textio)
                        yield from mapper.remap(csvio)

            write_tsv(yield_rows(), opath)

        if self.json != 'only':
            yield Action(input_paths, output_path, action_tsv, input="path")
        if self.json != 'no':
            yield CopyJSON(input_json_path, output_json_path)
