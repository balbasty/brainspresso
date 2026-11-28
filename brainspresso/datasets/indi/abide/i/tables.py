"""
Participants and phenotype tables of ABIDE-I.

The phenotypic CSV files (`phenotypic_<SITE>.csv` of NITRC and of the
bucket, one per site and sample; `Phenotypic_V1_0b.csv` of NITRC-IR, all
sites) share their columns. Each table is a `Table` of
`datasets.indi.abide.tables` (shared with ABIDE-II): columns are read by
name in each file, a column that a file lacks is `n/a` in its rows.
"""
from __future__ import annotations

from logging import getLogger

from brainspresso.datasets.indi.abide.i.sites import TemplateError, parse_site
from brainspresso.datasets.indi.abide.tables import (
    NANS,
    WIDTH,
    Table,
    participant_label,
)

lg = getLogger(__name__)

__all__ = [
    "NANS",
    "PARTICIPANTS",
    "PHENOTYPES",
    "WIDTH",
    "Table",
    "participant_label",
]


def _site(value: str) -> str:
    """Site of a `SITE_ID` (`MAX_MUN` -> `MaxMun`)."""
    try:
        return parse_site(value)[0]
    except TemplateError:
        return value


def _sample(value: str) -> str:
    """Sample of a `SITE_ID` (`UCLA_1` -> `UCLA1`, `CALTECH` -> `Caltech`)."""
    try:
        site, sample = parse_site(value)
    except TemplateError:
        return value
    return f"{site}{sample or ''}"


# ----------------------------------------------------------------------
#   Participants
# ----------------------------------------------------------------------

# NOTE
#   There are two different ages in the csv: AGE_AT_SCAN and
#   AGE_AT_MPRAGE. Only the first one is kept.

IQ_TYPES = {
    'DAS_II_SA': 'DAS-II-SA',
    'WISC': 'WISC',
    'WISC_III': 'WISC-III',
    'WISC_III_DUTCH': 'WISC-III-DUTCH',
    'WISC_IV_FULL': 'WISC-IV_FULL',
    'WISC_IV_4_SUBTESTS': 'WISC-IV_SUB',
    'WAIS_III': 'WAIS-III',
    'HAWIK_IV': 'HAWIK-IV',
}
YES_NO = {'0': 'N', '1': 'Y'}

PARTICIPANTS = Table(
    columns={
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
    },
    values={
        'site': _site,
        'sample': _sample,
        'sex': {'1': 'M', '2': 'F'},
        'handedness': {'Ambi': 'B'},
        'group': {'1': 'A', '2': 'C'},
        'fiq_type': IQ_TYPES,
        'viq_type': IQ_TYPES,
        'piq_type': IQ_TYPES,
        'med_status': YES_NO,
        'off_stimulant': YES_NO,
        'eye_status': {'1': 'O', '2': 'C'},
    },
)

# ----------------------------------------------------------------------
#   Phenotypes (`phenotype/<name>.tsv`)
# ----------------------------------------------------------------------

PHENOTYPES: dict[str, Table] = {
    'adir': Table(
        columns={
            'participant_id': 'SUB_ID',
            'social': 'ADI_R_SOCIAL_TOTAL_A',
            'verbal': 'ADI_R_VERBAL_TOTAL_BV',
            'rrb': 'ADI_RRB_TOTAL_C',
            'onset': 'ADI_R_ONSET_TOTAL_D',
            'reliable': 'ADI_R_RSRCH_RELIABLE',
        },
        values={'reliable': YES_NO},
    ),
    'ados': Table(
        columns={
            'participant_id': 'SUB_ID',
            'module': 'ADOS_MODULE',
            'total': 'ADOS_TOTAL',
            'comm': 'ADOS_COMM',
            'social': 'ADOS_SOCIAL',
            'stereo_behav': 'ADOS_STEREO_BEHAV',
            'reliable': 'ADOS_RSRCH_RELIABLE',
        },
        values={'reliable': YES_NO},
    ),
    'ados_gotham': Table(
        columns={
            'participant_id': 'SUB_ID',
            'soc_affect': 'ADOS_GOTHAM_SOCAFFECT',
            'rrb': 'ADOS_GOTHAM_RRB',
            'total': 'ADOS_GOTHAM_TOTAL',
            'severity': 'ADOS_GOTHAM_SEVERITY',
        },
    ),
    'srs': Table(
        columns={
            'participant_id': 'SUB_ID',
            'version': 'SRS_VERSION',
            'raw_total': 'SRS_RAW_TOTAL',
            'awareness': 'SRS_AWARENESS',
            'cognition': 'SRS_COGNITION',
            'communication': 'SRS_COMMUNICATION',
            'motivation': 'SRS_MOTIVATION',
            'mannerisms': 'SRS_MANNERISMS',
        },
        values={'version': {'1': 'C', '2': 'A'}},
    ),
    'vineland': Table(
        columns={
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
        },
        values={'informant': {'1': 'P', '2': 'S'}},
    ),
    'wisc_iv': Table(
        columns={
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
        },
    ),
}
