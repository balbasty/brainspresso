# OASIS-2

OASIS-2 is a longitudinal collection of 150 right-handed subjects aged 60 to
96, each scanned on two or more visits at least one year apart, for a total of
373 imaging sessions. Seventy-two subjects remained non-demented throughout
the study, 64 were demented at their first visit, and 14 converted from
non-demented to demented during the study. Each session has three or four
T1-weighted MPRAGE scans.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx oasis2`, extra `brainspresso[oasis2]` for roast |
| Folder | `OASIS-2/` |
| Population | 150 adults, 60–96 years, longitudinal, with and without dementia |
| Modalities | T1w (3–4 runs per session) |
| Access | Open, no registration |
| Host | WUSTL |
| Website | <https://sites.wustl.edu/oasisbrains/home/oasis-2/> |

</div>

## Contents

| Key | BIDS files |
|-----|------------|
| `raw` | `sub-<label>/ses-<n>/anat/sub-<label>_ses-<n>_run-<n>_T1w.nii.gz` |
| `meta` | `participants.tsv`, and a `sessions.tsv` per subject with the visit, age, education, socio-economic status, MMSE, CDR and brain volumes |

The session label is the MR number of the visit (`OAS2_0001_MR2` becomes
`sub-0001/ses-2`). The data is distributed in two *parts* of about 75 subjects
each, and `--parts` restricts harvest and roast to one of them. Roast also
accepts `--sessions`.

## Examples

```shell
bx oasis2 harvest /data --parts 1
bx oasis2 roast /data --subs 1 2 --sessions 1
```

## How to cite

Acknowledge OASIS as described in the [data use terms](index.md#access-and-terms),
with the following funding: P50 AG05681, P01 AG03991, P01 AG026276, R01
AG021910, P20 MH071616, U24 RR021382.

!!! references "References"

    1. Marcus DS, Fotenos AF, Csernansky JG, Morris JC, Buckner RL.
       *Open Access Series of Imaging Studies (OASIS): longitudinal MRI data in
       nondemented and demented older adults.*
       J Cogn Neurosci **22**(12):2677-2684, 2010.
       [doi:10.1162/jocn.2009.21407](https://doi.org/10.1162/jocn.2009.21407)

    ??? quote "BibTeX"

        ```bibtex
        @article{marcus2010open,
          author  = {Marcus, Daniel S. and Fotenos, Anthony F. and Csernansky, John G. and Morris, John C. and Buckner, Randy L.},
          title   = {Open Access Series of Imaging Studies ({OASIS}): longitudinal {MRI} data in nondemented and demented older adults},
          journal = {Journal of Cognitive Neuroscience},
          volume  = {22},
          number  = {12},
          pages   = {2677--2684},
          year    = {2010},
          doi     = {10.1162/jocn.2009.21407},
        }
        ```
