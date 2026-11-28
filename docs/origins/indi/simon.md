# SIMON

SIMON (*Single Individual volunteer for Multiple Observations across
Networks*) follows one healthy man scanned in about 70 sessions over more than
fifteen years, from age 29 to 46, on many scanners at different sites. It
quantifies how MRI measurements vary between scanners and over time within the
same brain, which makes it useful to calibrate multi-site studies.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx simon` |
| Folder | `SIMON/` |
| Population | 1 healthy adult, about 70 sessions, many sites |
| Modalities | T1w, T2w, PD, FLAIR, DWI, resting-state fMRI |
| Derivatives | Resting-state fMRI preprocessed with NIAK (`--folders derivatives`) |
| Size | 9 GB |
| Access | Open, no registration |
| Host | `s3://fcp-indi` |
| Website | <https://fcon_1000.projects.nitrc.org/indi/retro/SIMON.html> |

</div>

## Contents

A single subject, with one BIDS session per scanning session. The NIAK outputs
are written to `derivatives/niak/` when `--folders derivatives` is given, with a
dataset description that links them to the raw data.

## Examples

```shell
bx simon info --derivatives
bx simon harvest /data --suffixes T1w --dry-run
bx simon harvest /data --folders derivatives
```

## How to cite

!!! references "References"

    1. Duchesne S, Dieumegarde L, Chouinard I, Farokhian F, Badhwar A, Bellec P, et al.
       *Structural and functional multi-platform MRI series of a single human
       volunteer over more than fifteen years.*
       Sci Data **6**:245, 2019.
       [doi:10.1038/s41597-019-0262-8](https://doi.org/10.1038/s41597-019-0262-8)

    ??? quote "BibTeX"

        ```bibtex
        @article{duchesne2019structural,
          author  = {Duchesne, Simon and Dieumegarde, Louis and Chouinard, Isabelle and Farokhian, Farnaz and Badhwar, Amanpreet and Bellec, Pierre and T{\'e}trault, Pascal and Descoteaux, Maxime and Bor{\'e}, Arnaud and Houde, Jean-Christophe and Beaulieu, Christian and Potvin, Olivier},
          title   = {Structural and functional multi-platform {MRI} series of a single human volunteer over more than fifteen years},
          journal = {Scientific Data},
          volume  = {6},
          pages   = {245},
          year    = {2019},
          doi     = {10.1038/s41597-019-0262-8},
        }
        ```
