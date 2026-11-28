# MPI-LEMON

The *Leipzig Study for Mind-Body-Emotion Interactions* (LEMON), run at the
Max Planck Institute for Human Cognitive and Brain Sciences, characterised 227
healthy adults in two age groups, 20–35 and 59–77 years. Its full release
combines MRI, EEG, cognitive tests, emotion questionnaires and peripheral
physiology. The INDI bucket, and `bx mpi-lemon`, cover the MRI part.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx mpi-lemon` |
| Folder | `MPI-LEMON/` |
| Population | 227 healthy adults, 20–35 and 59–77 years, 1 site |
| Modalities | MP2RAGE, T2w, FLAIR, T2*w (GRE), DWI, field maps, resting-state fMRI |
| Size | 132 GB |
| Access | Open, no registration |
| Host | `s3://fcp-indi` |
| Website | <https://fcon_1000.projects.nitrc.org/indi/retro/MPI_LEMON.html> |

</div>

## Contents

* `anat`: MP2RAGE (`acq-mp2rage_T1w`, `T1map` and the inversion images),
  T2w, FLAIR (`acq-highres`, `acq-lowres`), and the magnitude and phase of a
  gradient-echo scan (`part-mag_T2starw`, `part-phase_T2starw`).
* `dwi`, `fmap` (phase difference, and spin-echo pairs for the BOLD and DWI
  scans), `func/*_task-rest_acq-AP_bold`.
* `participants.tsv`: sex and age group, in 5-year bins. Exact ages are not
  public.

## Curation

* The gradient-echo images, named `acq-{mag,phase}_GRE` on the bucket, are
  renamed `part-{mag,phase}_T2starw`, since `GRE` is not a BIDS suffix.
* The MP2RAGE inversion images get the `MP2RAGE` suffix.
* Sex, coded 1 and 2 in the source table, becomes `F` and `M`.
* The sidecars are completed from protocol templates.

## Examples

```shell
bx mpi-lemon info
bx mpi-lemon harvest /data --datatype anat --suffix MP2RAGE T1map --dry-run
bx mpi-lemon harvest /data --datatype func dwi fmap
```

## How to cite

!!! references "References"

    1. Babayan A, Erbey M, Kumral D, Reinelt JD, Reiter AMF, Röbbig J, et al.
       *A mind-brain-body dataset of MRI, EEG, cognition, emotion, and peripheral
       physiology in young and old adults.*
       Sci Data **6**:180308, 2019.
       [doi:10.1038/sdata.2018.308](https://doi.org/10.1038/sdata.2018.308)

    ??? quote "BibTeX"

        ```bibtex
        @article{babayan2019mind,
          author  = {Babayan, Anahit and Erbey, Miray and Kumral, Deniz and Reinelt, Janis D and Reiter, Andrea M F and R{\"o}bbig, Josefin and Schaare, H Lina and Uhlig, Marie and Anwander, Alfred and Bazin, Pierre-Louis and Horstmann, Annette and Lampe, Leonie and Nikulin, Vadim V and Okon-Singer, Hadas and Preusser, Sven and Pampel, Andr{\'e} and Rohr, Christiane S and Sacher, Julia and Th{\"o}ne-Otto, Angelika and Trapp, Sabrina and Nierhaus, Till and Altmann, Denise and Arelin, Katrin and Bl{\"o}chl, Maria and Bongartz, Edith and Breig, Patric and Cesnaite, Elena and Chen, Sufang and Cozatl, Roberto and Czerwonatis, Saskia and others},
          title   = {A mind-brain-body dataset of {MRI}, {EEG}, cognition, emotion, and peripheral physiology in young and old adults},
          journal = {Scientific Data},
          volume  = {6},
          pages   = {180308},
          year    = {2019},
          doi     = {10.1038/sdata.2018.308},
        }
        ```
