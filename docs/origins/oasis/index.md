# OASIS

The *Open Access Series of Imaging Studies* (OASIS) is a series of neuroimaging
datasets released by the Knight Alzheimer Disease Research Center at
Washington University in St. Louis, to make MRI data on normal ageing and
Alzheimer's disease freely available. The releases differ in design and scope:

| Dataset | Design | Subjects | Ages | Modalities | Access |
|---------|--------|---------:|------|------------|--------|
| [OASIS-1](oasis1.md) | Cross-sectional | 416 | 18–96 | T1w | Open |
| [OASIS-2](oasis2.md) | Longitudinal | 150 | 60–96 | T1w | Open |
| [OASIS-3](oasis3.md) | Longitudinal, multimodal | 1378 | 42–95 | MRI, PET, CT | Application and NITRC account |

All three use four-digit subject labels (`sub-0001`), so `--sub 1:10` selects
the first ten subjects.

## Access and terms

OASIS-1 and OASIS-2 are downloaded from the WUSTL servers without an account.
OASIS-3 is hosted on [NITRC-IR](https://www.nitrc.org/ir) and requires access
to be requested on the OASIS website first. Its credentials are passed as for
any NITRC source (see [Credentials](../../guide/harvest.md#credentials)).

The data use terms are on the [OASIS website](https://sites.wustl.edu/oasisbrains/).
They ask that publications acknowledge OASIS as the source of the data:

> Data were provided [in part] by OASIS [insert appropriate OASIS source info].

Each dataset page gives the funding information and the paper to cite.

## Conversion

The images of OASIS-1 and OASIS-2 are distributed in the Analyze 7.5 format,
whose orientation is ambiguous. brainspresso converts them to NIfTI with the
orientation documented for each image type.
