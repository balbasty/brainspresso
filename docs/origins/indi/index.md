# INDI

The [International Neuroimaging Data-sharing Initiative](https://fcon_1000.projects.nitrc.org/)
(INDI), started by the 1000 Functional Connectomes Project in 2009, releases
datasets contributed by laboratories around the world. Most of them are
mirrored on the public `fcp-indi` S3 bucket, and the INDI page of each project
gives its terms of use.

INDI groups its projects by how the data was collected: *retrospective*
(`retro`) projects share data that had already been acquired, *prospective*
(`pro`) projects share data as it is acquired, and the *enhanced* NKI Rockland
Sample has a category of its own. `bx <dataset> info` shows the category.

## Two kinds of INDI datasets

* **Ported datasets** ([ABIDE-I](abide1.md), [ABIDE-II](abide2.md),
  [COBRE](cobre.md)) are distributed as archives and tables, and converted to
  BIDS by code specific to each dataset.
  [ADHD-200](adhd200.md) and [CoRR](corr.md) are in the same group, but are
  only harvested for now.
* **Hosted projects** ([ACPI](acpi.md), [FCON1000](fcon1000.md),
  [HBN](hbn.md), [MPI-LEMON](mpi-lemon.md), [NATVIEW](natview.md),
  [NKI-RS](nki-rs.md), [SALD](sald.md), [SIMON](simon.md), [SLIM](slim.md))
  are already in BIDS, or close to it, on the bucket. They are downloaded with
  the [hybrid placement](../../guide/harvest.md#datasets-hosted-in-bids) used
  for OpenNeuro, and accept the same
  [BIDS filters](../../guide/selection.md#bids-filters). Only the selected
  subjects are listed, so a small selection starts quickly.

## Curation of the hosted projects

The bucket copies are rarely valid BIDS, so each project is curated:

* file names with typos or entities out of order are fixed;
* missing dataset descriptions, READMEs and sidecars are generated;
* sidecars are completed from protocol templates, which add the keys that the
  bucket's sidecars lack but never change a key the bucket gives;
* common metadata defects are repaired, as for OpenNeuro.

Each roasted dataset's `README` lists what was changed. The curation of each
project is described on its page.

## Multi-site projects

Multi-site projects (FCON1000, HBN, ACPI) can be harvested in two ways:

* `--layout study` (default) or `--layout flat`: one BIDS dataset with every
  site. Labels that are not unique across sites get a site prefix
  (`AnnArbor_a/sub-04111` becomes `sub-AnnArborA04111`), and
  `participants.tsv` has a `site` column.
* `--layout sites`: one BIDS dataset per site (`FCON1000/<site>/`), with the
  labels used on the bucket and site-level metadata. Each site dataset uses the
  study layout, or the flat one with `--layout sites:flat`. This follows
  option 1 of the BIDS recommendations for multi-site studies. The parent
  folder is not a BIDS dataset: it holds a README and the merged
  `participants.tsv`, which is what `--where` queries.

With either layout, `--sites` and `--exclude-sites` select sites, and `--subs`
accepts both forms of the labels.

## Access

Hosted projects are downloaded anonymously from the bucket. The original
archives of the ported datasets, and the ABIDE derivatives, are on NITRC, which
requires a free NITRC account. INDI asks users to register with NITRC and with
the 1000 Functional Connectomes Project before using the data, including the
copies on the bucket.

```shell
export NITRC_USER=myname
export NITRC_PASS=...
bx abide2 harvest /data
```

See [Credentials](../../guide/harvest.md#credentials) for the other ways to
pass them.
