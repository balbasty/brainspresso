# Origins

An *origin* is a public dataset that brainspresso can download and, in
most cases, convert to BIDS. Each origin is a command of `bx`
(`bx ixi`, `bx abide1`, ...), and its page describes the original study, the
data it contains, how to obtain access, and what to cite.

Origins are grouped by collection: OpenNeuro, INDI, OASIS, and datasets that
belong to no collection.

**Status** says how far brainspresso goes with each dataset:

* *converted*: downloaded, then converted to BIDS by brainspresso;
* *hosted*: already in BIDS on its host; harvest places it directly, and roast
  curates the metadata;
* *download only*: not yet converted.

## OpenNeuro

| Command | Dataset | Status | Access |
|---------|---------|--------|--------|
| [`openneuro`](openneuro.md) | Any dataset of the OpenNeuro archive | hosted | Open |

## INDI

The [International Neuroimaging Data-sharing Initiative](indi/index.md).

| Command | Dataset | Population | Status | Access |
|---------|---------|------------|--------|--------|
| [`abide1`](indi/abide1.md) | Autism Brain Imaging Data Exchange I | ASD and controls | converted | Open (S3), NITRC |
| [`abide2`](indi/abide2.md) | Autism Brain Imaging Data Exchange II | ASD and controls | converted | NITRC |
| [`acpi`](indi/acpi.md) | Addiction Connectome Preprocessed Initiative | Substance use | hosted | Open |
| [`adhd200`](indi/adhd200.md) | ADHD-200 | ADHD and controls | download only | Open |
| [`cobre`](indi/cobre.md) | COBRE | Schizophrenia and controls | converted | NITRC |
| [`corr`](indi/corr.md) | Consortium for Reliability and Reproducibility | Healthy, test-retest | download only | Open (S3), NITRC |
| [`fcon1000`](indi/fcon1000.md) | 1000 Functional Connectomes Project | Mostly healthy | hosted | Open |
| [`hbn`](indi/hbn.md) | Healthy Brain Network | Children and adolescents | hosted | Open |
| [`mpi-lemon`](indi/mpi-lemon.md) | MPI-Leipzig Mind-Brain-Body: LEMON | Healthy, young and old | hosted | Open |
| [`natview`](indi/natview.md) | Naturalistic viewing, simultaneous EEG-fMRI | Healthy | hosted | Open |
| [`nki-rs`](indi/nki-rs.md) | NKI Rockland Sample | Community lifespan sample | hosted | Open |
| [`sald`](indi/sald.md) | Southwest University Adult Lifespan Dataset | Healthy, lifespan | hosted | Open |
| [`simon`](indi/simon.md) | SIMON: one individual, ~70 sessions | Healthy, single subject | hosted | Open |
| [`slim`](indi/slim.md) | Southwest University Longitudinal Imaging Multimodal | Healthy young adults | hosted | Open |

## OASIS

The [Open Access Series of Imaging Studies](oasis/index.md).

| Command | Dataset | Population | Status | Access |
|---------|---------|------------|--------|--------|
| [`oasis1`](oasis/oasis1.md) | OASIS-1, cross-sectional | Adult lifespan, dementia | converted | Open |
| [`oasis2`](oasis/oasis2.md) | OASIS-2, longitudinal | Older adults, dementia | converted | Open |
| [`oasis3`](oasis/oasis3.md) | OASIS-3, longitudinal, MRI and PET | Normal ageing, Alzheimer's disease | converted | Application, NITRC |

## Other datasets

| Command | Dataset | Population | Status | Access |
|---------|---------|------------|--------|--------|
| [`bullitt`](other/bullitt.md) | Designed Database of MR Brain Images of Healthy Volunteers | Healthy adults | converted | Open |
| [`gsp`](other/gsp.md) | Brain Genomics Superstruct Project | Healthy young adults | download only | Dataverse account |
| [`ixi`](other/ixi.md) | Information eXtraction from Images | Healthy adults | converted | Open |
| [`unc-wisconsin`](other/unc-wisconsin.md) | UNC-Wisconsin Rhesus Macaque Neurodevelopment Database | Rhesus macaques | converted | Open |

## Using a dataset

The data remains under the license and terms of use of its provider.
brainspresso does not change them. Before publishing results, check the
terms on the provider's website and cite the references given on the dataset's
page.
