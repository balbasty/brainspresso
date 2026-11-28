# OpenNeuro

[OpenNeuro](https://openneuro.org) is an open archive of neuroimaging datasets
in BIDS, covering MRI, PET, MEG, EEG and iEEG. Each dataset has its own
accession number (`ds000224`), versioned snapshots, a DOI and a license.
`bx openneuro` can download any public OpenNeuro dataset, in full or in part.

<div class="glance" markdown>

| | |
|---|---|
| Command | `bx openneuro <accession>` |
| Folder | `OpenNeuro/<accession>/` |
| Modalities | Whatever the dataset contains |
| Access | Open; a token for private datasets |
| Hosts | OpenNeuro GraphQL API; the public `s3://openneuro.org` bucket |
| License | Set by each dataset, usually CC0 |

</div>

## Selecting data

There are no keys. Data is selected with the
[BIDS filters](../guide/selection.md#bids-filters), and the metadata that
applies to the selected files is added automatically.

```shell
# What is in a dataset, and what would a selection download?
bx openneuro info ds000224
bx openneuro harvest ds000224 --datatype anat --dry-run

# Anatomical scans of two subjects, from snapshot 1.0.4
bx openneuro harvest ds000224 /data --tag 1.0.4 --sub MSC01 MSC02 --datatype anat

# Participants selected on participants.tsv
bx openneuro harvest ds000224 /data --suffix T1w T2w --where "age>=30" "gender==M"
```

`--tag` selects a snapshot; the default is the latest one. `info` lists the
available snapshots. `--folders` also downloads the dataset's own
`derivatives/`, `sourcedata/`, `code/` or `stimuli/` folders, which are placed
next to the raw dataset.

## Sources

Files are listed through the OpenNeuro GraphQL API, which pins the exact
snapshot and provides SHA256 checksums for annexed files. If the API cannot be
reached, the listing falls back to the `s3://openneuro.org` bucket. The bucket
only holds the latest snapshot, and its checksums are the MD5 sums of the S3
ETags, so the fallback is not used when `--tag` is given. `--source graphql` or
`--source s3` chooses one route explicitly.

Private datasets need an API token, created on the OpenNeuro website and passed
with `--token`.

## Metadata

OpenNeuro datasets use the [hybrid placement](../guide/harvest.md#datasets-hosted-in-bids)
of hosted datasets: images go directly to their place, and metadata is kept as
a pristine copy in `sourcedata/openneuro/` before being repaired and trimmed
into the dataset. `bx openneuro roast ds000224 /data` repeats that step
without accessing the network, for example after editing or deleting files.

## How to cite

Cite each dataset you use with the DOI of its snapshot, shown on its OpenNeuro
page and stored as `DatasetDOI` in its `dataset_description.json`. The same
file may also give `HowToAcknowledge` and `ReferencesAndLinks`. If you use
OpenNeuro itself, also cite the paper describing the archive.

!!! references "References"

    1. Markiewicz CJ, Gorgolewski KJ, Feingold F, Blair R, Halchenko YO, Miller E, et al.
       *The OpenNeuro resource for sharing of neuroscience data.*
       eLife **10**:e71774, 2021.
       [doi:10.7554/eLife.71774](https://doi.org/10.7554/eLife.71774)

    ??? quote "BibTeX"

        ```bibtex
        @article{markiewicz2021openneuro,
          author  = {Markiewicz, Christopher J. and Gorgolewski, Krzysztof J. and Feingold, Franklin and Blair, Ross and Halchenko, Yaroslav O. and Miller, Eric and Hardcastle, Nell and Wexler, Joe and Esteban, Oscar and Goncalves, Mathias and Jwa, Anita and Poldrack, Russell},
          title   = {The {OpenNeuro} resource for sharing of neuroscience data},
          journal = {eLife},
          volume  = {10},
          pages   = {e71774},
          year    = {2021},
          doi     = {10.7554/eLife.71774},
        }
        ```
