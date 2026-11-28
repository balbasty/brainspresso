"""
Participants and phenotype tables of ABIDE-II.

Read from the phenotypic CSV files of NITRC (`ABIDEII-<SITE>_<sample>.csv`,
one per site and sample, every column), else from the composite file
(`ABIDEII_Composite_Phenotypic.csv`). Each table is a `Table` of
`datasets.indi.abide.tables` (shared with ABIDE-I): columns are read by
name in each file (without case or whitespace), a column that a file
lacks is `n/a` in its rows, missing values (`-9999` included) are `n/a`.

The column names are those of the legacy roaster, from the ABIDE-II data
legend; they could not be checked against the files (NITRC login): the
spellings of a few columns (typos of the legend) are accepted both ways.
"""
from __future__ import annotations

import re
from logging import getLogger

from brainspresso.datasets.indi.abide.ii.sites import sample_label, site_label
from brainspresso.datasets.indi.abide.tables import Table

lg = getLogger(__name__)

__all__ = ["CSV_GLOB", "COMPOSITE", "PARTICIPANTS", "PHENOTYPES"]

#: Phenotypic files of the sites (NITRC)
CSV_GLOB = "ABIDEII-*.csv"

#: Phenotypic file of all sites (read if no site file is found)
COMPOSITE = "ABIDEII_Composite_Phenotypic.csv"

YES_NO = {'0': 'N', '1': 'Y'}

#: Column that tells the baseline row of a subject listed twice (the
#: longitudinal samples, `*_Long.csv`: baseline and follow-up)
BASELINE = 'AGE_AT_SCAN'

#: Handedness categories (`participants.json`: L, R, B)
HANDEDNESS = {'r': 'R', 'right': 'R', 'l': 'L', 'left': 'L',
              'ambi': 'B', 'mixed': 'B', 'b': 'B'}

_warned: set = set()


def handedness(value: str) -> str:
    """
    Handedness category (`participants.json`: L, R, B).

    Parameters
    ----------
    value : str
        `R`, `L`, `Ambi`, `Mixed` (any case)

    Returns
    -------
    str
        `R`, `L` or `B`; other values as they are, numbers included (a
        code that the legend of the legacy roaster does not give: warned
        once, kept until the codes are checked)
    """
    out = HANDEDNESS.get(value.strip().lower())
    if out is not None:
        return out
    if re.fullmatch(r"-?\d+(\.\d*)?", value.strip()) and \
            "handedness" not in _warned:
        _warned.add("handedness")
        lg.warning(f"HANDEDNESS_CATEGORY: numeric codes ({value!r}) are "
                   f"not known: kept as they are")
    return value


def _flat(value: str) -> str:
    """Key of a test name (lower case, letters and digits only)."""
    return re.sub(r"[^0-9a-z]", "", value.lower())


IQ_TYPES = {
    'DAS-School': 'DAS-S',
    'DAS-PreSchool': 'DAS-PS',
    'DAS-II school age': 'DAS-2-SA',
    'DAS-II early year': 'DAS-2-EY',
    'KBIT-2': 'KBIT-2',
    'Raven': 'RAVEN',
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
_IQ_TYPES = {_flat(k): v for k, v in IQ_TYPES.items()}


def iq_type(value: str) -> str:
    """
    Name of an IQ test (`participants.json` levels).

    Parameters
    ----------
    value : str
        Name in the CSV file (any case, spaces and punctuation)

    Returns
    -------
    str
        `WISC-IV` -> `WISC-4`, `das-ii school age` -> `DAS-2-SA`; other
        values as they are
    """
    return _IQ_TYPES.get(_flat(value), value)


# ----------------------------------------------------------------------
#   Participants
# ----------------------------------------------------------------------

# NOTE
#   There are two different ages in the csv: AGE_AT_SCAN and
#   AGE_AT_MPRAGE. Only the first one is kept.

PARTICIPANTS = Table(
    columns={
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
        'med_status': 'CURRENT_MED_STATUS',
        'med_name': 'CURRENT_MEDICATION_NAME',
        'off_stimulant': 'OFF_STIMULANTS_AT_SCAN',
        'eye_status': 'EYE_STATUS_AT_SCAN',
        'bmi': 'BMI',
    },
    values={
        'site': site_label,
        'sample': sample_label,
        'sex': {'1': 'M', '2': 'F'},
        'handedness': handedness,
        'group': {'1': 'A', '2': 'C'},
        'fiq_type': iq_type,
        'viq_type': iq_type,
        'piq_type': iq_type,
        'med_status': YES_NO,
        'off_stimulant': YES_NO,
        'eye_status': {'1': 'O', '2': 'C'},
    },
    baseline=BASELINE,
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
            'nonverbal': 'ADI_R_NONVERBAL_TOTAL_BV',
            'rrb': 'ADI_R_RRB_TOTAL_C',
            'onset': 'ADI_R_ONSET_TOTAL_D',
            'reliable': 'ADI_R_RSRCH_RELIABLE',
        },
        values={'reliable': YES_NO},
        baseline=BASELINE,
    ),
    'adir_a1': Table(
        columns={
            'participant_id': 'SUB_ID',
            'direct_gaze': 'ADI_R_A1_DIRECT_GAZE',
            'social_smile': 'ADI_R_A1_SOCIAL_SMILE',
            'facial_expressions': 'ADI_R_A1_FACIAL_EXPRESSIONS',
            'total': 'ADI_R_A1_TOTAL',
        },
        baseline=BASELINE,
    ),
    'adir_a2': Table(
        columns={
            'participant_id': 'SUB_ID',
            'imaginative_play': 'ADI_R_A2_IMAGINATIVE_PLAY',
            'interest_in_children': 'ADI_R_A2_INTEREST_IN_CHILDREN',
            'response_to_approaches': 'ADI_R_A2_RESPONSE_TO_APPROACHES',
            'group_play': 'ADI_R_A2_GROUP_PLAY',
            'higher': 'ADI_R_A2_HIGHER',
            'friendships': 'ADI_R_A2_FRIENDSHIPS',
            'total': 'ADI_R_A2_TOTAL',
        },
        baseline=BASELINE,
    ),
    'adir_a3': Table(
        columns={
            'participant_id': 'SUB_ID',
            'showing_directing_attention':
                'ADI_R_A3_SHOWING_DIRECTING_ATTENTION',
            'offering_to_share': 'ADI_R_A3_OFFERING_TO_SHARE',
            'seeking_share_enjoyment': 'ADI_R_A3_SEEKING_SHARE_ENJOYMENT',
            'total': 'ADI_R_A3_Total',
        },
        baseline=BASELINE,
    ),
    'adir_a4': Table(
        columns={
            'participant_id': 'SUB_ID',
            'use_others_body': 'ADI_R_A4_USE_OTHERS_BODY',
            'offering_comfort': 'ADI_R_A4_OFFERING_COMFORT',
            'quality_social_overtures': 'ADI_R_A4_QUALITY_SOCIAL_OVERTURES',
            'inappropriate_facial_expressions':
                'ADI_R_A4_INAPPROPRIATE_FACIAL_EXPRESSIONS',
            'appropriate_social_responses':
                'ADI_R_A4_APPROPRIATE_SOCIAL_RESPONSES',
            'total': 'ADI_R_A4_Total',
        },
        baseline=BASELINE,
    ),
    'adir_b1': Table(
        columns={
            'participant_id': 'SUB_ID',
            'pointing_express_interest': 'ADI_R_B1_POINTING_EXPRESS_INTEREST',
            'nodding': 'ADI_R_B1_NODDING',
            'head_shaking': 'ADI_R_B1_HEAD_SHAKING',
            'conventional_gestures': 'ADI_R_B1_CONVENTIONAL_GESTURES',
            'total': 'ADI_R_B1_TOTAL',
        },
        baseline=BASELINE,
    ),
    'adir_b2': Table(
        columns={
            'participant_id': 'SUB_ID',
            'social_verbalization': 'ADI_R_B2_SOCIAL_VERBALIZATION',
            'reciprocal_conversation': 'ADI_R_B2_RECIPROCAL_CONVERSATION',
            'total': 'ADI_R_B2_TOTAL',
        },
        baseline=BASELINE,
    ),
    'adir_b3': Table(
        columns={
            'participant_id': 'SUB_ID',
            'stereotyped_utterances': 'ADI_R_B3_STEREOTYPED_UTTERANCES',
            'inappropriate_questions': 'ADI_R_B3_INAPPROPRIATE_QUESTIONS',
            'pronominal_reversal': 'ADI_R_B3_PRONOMINAL_REVERSAL',
            'neologisms': 'ADI_R_B3_NEOLOGISMS',
            'total': 'ADI_R_B3_Total',
        },
        baseline=BASELINE,
    ),
    'adir_c1': Table(
        columns={
            'participant_id': 'SUB_ID',
            'unusual_preoccupations': 'ADI_R_C1_UNUSUAL_PREOCCUPATIONS',
            'circumscribed_interests': 'ADI_R_C1_CIRCUMSCRIBED_INTERESTS',
            'total': 'ADI_R_C1_TOTAL',
        },
        baseline=BASELINE,
    ),
    'adir_c2': Table(
        columns={
            'participant_id': 'SUB_ID',
            'verbal_rituals': 'ADI_R_C2_VERBAL_RITUALS',
            'compulsions': 'ADI_R_C2_COMPULSIONS',
            'total': 'ADI_R_C2_TOTAL',
        },
        baseline=BASELINE,
    ),
    'adir_c3': Table(
        columns={
            'participant_id': 'SUB_ID',
            'hand_finger_mannerisms': 'ADI_R_C3_HAND_FINGER_MANNERISMS',
            'higher': 'ADI_R_C3_HIGHER',
            'other_complex_mannerisms': 'ADI_R_C3_OTHER_COMPLEX_MANNERISMS',
            'total': 'ADI_R_C3_TOTAL',
        },
        baseline=BASELINE,
    ),
    'adir_c4': Table(
        columns={
            'participant_id': 'SUB_ID',
            'repetitive_use_objects': 'ADI_R_C4_REPETITIVE_USE_OBJECTS',
            'higher': 'ADI_R_C4_HIGHER',
            'unusual_sensory_interests': 'ADI_R_C4_UNUSUAL_SENSORY_INTERESTS',
            'total': 'ADI_R_C4_TOTAL',
        },
        baseline=BASELINE,
    ),
    'adir_d': Table(
        columns={
            'participant_id': 'SUB_ID',
            'age_parent_noticed': 'ADI_R_D_AGE_PARENT_NOTICED',
            'age_first_single_words': 'ADI_R_D_AGE_FIRST_SINGLE_WORDS',
            'age_first_phrases': 'ADI_R_D_AGE_FIRST_PHRASES',
            'age_when_abnormality': 'ADI_R_D_AGE_WHEN_ABNORMALITY',
            'interviewer_judgement': 'ADI_R_D_INTERVIEWER_JUDGMENT',
        },
        baseline=BASELINE,
    ),
    'ados_2': Table(
        columns={
            'participant_id': 'SUB_ID',
            'module': 'ADOS_MODULE',
            'social_affect': 'ADOS_2_SOCAFFECT',
            'rrb': 'ADOS_2_RRB',
            'total': 'ADOS_2_TOTAL',
            'severity': 'ADOS_2_SEVERITY_TOTAL',
            'reliable': 'ADOS_RSRCH_RELIABLE',
        },
        values={'reliable': YES_NO},
        baseline=BASELINE,
    ),
    'ados_generic': Table(
        columns={
            'participant_id': 'SUB_ID',
            'module': 'ADOS_MODULE',
            'total': 'ADOS_G_TOTAL',
            'comm': 'ADOS_G_COMM',
            'social': 'ADOS_G_SOCIAL',
            'stereo_behav': 'ADOS_G_STEREO_BEHAV',
            'creativity': 'ADOS_G_CREATIVITY',
            'reliable': 'ADOS_RSRCH_RELIABLE',
        },
        values={'reliable': YES_NO},
        baseline=BASELINE,
    ),
    'srs': Table(
        columns={
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
        },
        values={'version': {'1': 'C', '2': 'A'}},
        baseline=BASELINE,
    ),
    'rbsr_6': Table(
        columns={
            'participant_id': 'SUB_ID',
            'stereotyped': 'RBSR_6SUBSCALE_STEREOTYPED',
            'self_injurious': 'RBSR_6SUBSCALE_SELF-INJURIOUS',
            'compulsive': 'RBSR_6SUBSCALE_COMPULSIVE',
            'ritualistic': 'RBSR_6SUBSCALE_RITUALISTIC',
            'sameness': 'RBSR_6SUBSCALE_SAMENESS',
            'restricted': 'RBSR_6SUBSCALE_RESTRICTED',
            'total': 'RBSR_6SUBSCALE_TOTAL',
        },
        baseline=BASELINE,
    ),
    'rbsr_5': Table(
        columns={
            'participant_id': 'SUB_ID',
            'stereotyped': 'RBSR_5SUBSCALE_STEREOTYPIC',
            'self_injurious': 'RBSR_5SUBSCALE_SELF-INJURIOUS',
            'compulsive': 'RBSR_5SUBSCALE_COMPULSIVE',
            'ritualistic': 'RBSR_5SUBSCALE_RITUALISTIC',
            'restricted': 'RBSR_5SUBSCALE_RESTRICTED',
            'total': 'RBSR_5SUBSCALE_TOTAL',
        },
        baseline=BASELINE,
    ),
    'masc': Table(
        columns={
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
        },
        baseline=BASELINE,
    ),
    'brief': Table(
        columns={
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
        },
        values={'version': {'1': 'C', '2': 'A'}},
        baseline=BASELINE,
    ),
    'cbcl_6-18': Table(
        columns={
            'participant_id': 'SUB_ID',
            'activities': 'CBCL_6-18_ACTIVITIES_T',
            'social': 'CBCL_6-18_SOCIAL_T',
            'school': 'CBCL_6-18_SCHOOL_T',
            'total_competence': 'CBCL_6-18_TOTAL_COMPETENCE_T',
            'anxious': 'CBCL_6-18_ANXIOUS_T',
            'withdrawn': 'CBCL_6-18_WITHDRAWN_T',
            'somatic_complaint':
                ('CBCL_6-18_SOMATIC_COMPLAINT_T',
                 'CBCL_6-18_SOMATIC_COMPAINT_T'),
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
        },
        baseline=BASELINE,
    ),
    'cbcl_1p5-5': Table(
        columns={
            'participant_id': 'SUB_ID',
            'emotion': 'CBCL_1.5-5_EMOTION_T',
            'anxious': 'CBCL_1.5-5_ANXIOUS_T',
            'somatic': ('CBCL_1.5-5_SOMATIC_T', 'CBCL_1.5-5_SOMANTIC_T'),
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
        },
        baseline=BASELINE,
    ),
    'bdi': Table(
        columns={
            'participant_id': 'SUB_ID',
            'edition': 'BDI_EDITION',
            'total': 'BDI_TOTAL',
        },
        baseline=BASELINE,
    ),
    'vmi': Table(
        columns={
            'participant_id': 'SUB_ID',
            'edition': 'VMI_EDITION',
            'total': 'VMI_VMI_S',
        },
        baseline=BASELINE,
    ),
    'celf_9-21': Table(
        columns={
            'participant_id': 'SUB_ID',
            'edition': 'CELF_EDITION',
            'core': 'CELF_9-21_CORE_S',
            'receptive': 'CELF_9-21_RECEPTIVE_S',
            'expressive': 'CELF_9-21_EXPRESSIVE_S',
        },
        baseline=BASELINE,
    ),
    'celf_5-8': Table(
        columns={
            'participant_id': 'SUB_ID',
            'edition': 'CELF_EDITION',
            'core': 'CELF_5-8_CORE_S',
            'receptive': 'CELF_5-8_RECEPTIVE_S',
            'expressive': 'CELF_5-8_EXPRESSIVE_S',
        },
        baseline=BASELINE,
    ),
    'basc': Table(
        columns={
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
        },
        baseline=BASELINE,
    ),
    'cprs': Table(
        columns={
            'participant_id': 'SUB_ID',
            'opp': 'CPRS_OPP',
            'cog_inatt': 'CPRS_COG-INATT',
            'hyperact': 'CPRS_HYPERACT',
            'anx_shy': 'CPRS_ANX_SHY',
            'perfect': 'CPRS_PERFECT',
            'social_prob': 'CPRS_SOCIAL_PROB',
            'psycho_somatic': 'CPRS_PSYCHO_SOMATIC',
            'conn_adhd': 'CPRS_CONN_ADHD',
            'rest_impul': ('CPRS_REST_IMPULS', 'CPRS_REST_IMPUL'),
            'emot_lability': 'CPRS_EMOT_LABILITY',
            'conn_gi_total': 'CPRS_CONN_GI_TOTAL',
            'dsm_iv_inatt': 'CPRS_DSM_IV_INATT',
            'dsm_iv_hyper_impul': 'CPRS_DSM_IV_HYPER_IMPUL',
            'dsm_iv_total': 'CPRS_DSM_IV_TOTAL',
            'inatt_symptoms': 'CPRS_INATT_SYMPTOMS',
            'hyper_impul_symptoms': 'CPRS_HYPER_IMPUL_SYMPTOMS',
        },
        baseline=BASELINE,
    ),
    'casi': Table(
        columns={
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
            'schizoid_personality_cutoff': 'CASI_SCHIZOID_PERSONALITY_CUTOFF',
            'manic_episode_cutoff': 'CASI_MANIC_EPISODE_CUTOFF',
            'anorexia_nervosa_cutoff': 'CASI_ANOREXIA_NERVOSA_CUTOFF',
            'bulimia_nervosa_cutoff': 'CASI_BULIMIA_NERVOSA_CUTOFF',
            'substance_use_cutoff': 'CASI_SUBSTANCE_USE_CUTOFF',
        },
        baseline=BASELINE,
    ),
    'csi': Table(
        columns={
            'participant_id': 'SUB_ID',
            'adhd_i_severity': 'CSI_ADHD-I_SEVERITY',
            'adhd_i_cutoff': 'CSI_ADHD-I_CUTOFF',
            'adhd_h_severity': 'CSI_ADHD-H_SEVERITY',
            'adhd_h_cutoff': 'CSI_ADHD-H_CUTOFF',
            'adhd_c_severity': 'CSI_ADHD-C_SEVERITY',
            'adhd_c_cutoff': 'CSI_ADHD-C_CUTOFF',
            'odd_severity': 'CSI_ODD_SEVERITY',
            'odd_cutoff': 'CSI_ODD_CUTOFF',
            'cd_severity': 'CSI_CD_SEVERITY',
            'cd_cutoff': 'CSI_CD_CUTOFF',
            'gad_severity': 'CSI_GAD_SEVERITY',
            'gad_cutoff': 'CSI_GAD_CUTOFF',
            'specific_phobia_severity': 'CSI_SPECIFIC_PHOBIA_SEVERITY',
            'specific_phobia_cutoff': 'CSI_SPECIFIC_PHOBIA_CUTOFF',
            'obsessions_severity': 'CSI_OBSESSIONS_SEVERITY',
            'obsessions_cutoff': 'CSI_OBSESSIONS_CUTOFF',
            'compulsions_severity': 'CSI_COMPULSIONS_SEVERITY',
            'compulsions_cutoff': 'CSI_COMPULSIONS_CUTOFF',
            'disturbing_events_severity': 'CSI_DISTURBING_EVENTS_SEVERITY',
            'disturbing_events_cutoff': 'CSI_DISTURBING_EVENTS_CUTOFF',
            'motor_tics_severity': 'CSI_MOTOR_TICS_SEVERITY',
            'motor_tics_cutoff': 'CSI_MOTOR_TICS_CUTOFF',
            'vocal_tics_severity': 'CSI_VOCAL_TICS_SEVERITY',
            'vocal_tics_cutoff': 'CSI_VOCAL_TICS_CUTOFF',
            'schizophrenia_severity': 'CSI_SCHIZOPHRENIA_SEVERITY',
            'schizophrenia_cutoff': 'CSI_SCHIZOPHRENIA_CUTOFF',
            'mdd_severity': 'CSI_MDD_SEVERITY',
            'mdd_cutoff': 'CSI_MDD_CUTOFF',
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
        },
        baseline=BASELINE,
    ),
    'wiat_2': Table(
        columns={
            'participant_id': 'SUB_ID',
            'word': 'A_WORD_T',
            'numerical': 'A_NUMERICAL_T',
            'spelling': 'A_SPELLING_T',
            'total_composite': 'A_TOTAL_COMPOSITE_S',
        },
        baseline=BASELINE,
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
            'dailylvng':
                ('VINELAND_DAILYLIVING_STANDARD',
                 'VINELAND_DAILYLVNG_STANDARD'),
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
        },
        values={'informant': {'1': 'P', '2': 'S', '3': 'O'}},
        baseline=BASELINE,
    ),
}
