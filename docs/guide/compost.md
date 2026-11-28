# Compost

`compost` deletes data from a local dataset. It has two modes.

## Deleting the source data

Without filters, compost deletes `sourcedata/` once its content has been
roasted.

```shell
bx ixi compost /data --dry-run          # list what would be deleted
bx ixi compost /data --keep "*.xls"     # keep some source files
bx oasis1 compost /data --nested        # also derivatives/*/sourcedata/
```

Compost refuses to run if it finds no BIDS subject, since the dataset has then
probably not been roasted. `--force` overrides this check.

For datasets hosted in BIDS, the pristine metadata in `sourcedata/openneuro/`
or `sourcedata/fcp-indi/` is kept: it is small, and roast needs it.
`--sourcedata` deletes it too, and the next harvest downloads it again.

## Deleting BIDS data outside a filter

With [BIDS filters](selection.md#bids-filters) or subject options, compost
deletes the BIDS data that does **not** match them. The filters describe what
is kept: after composting, the dataset holds what a harvest with the same
filters would have downloaded.

```shell
bx ixi compost /data --exclude-subs 012 145      # delete two subjects
bx ixi compost /data --suffixes T1w T2w          # keep only T1w and T2w
bx abide1 compost /data --where "age>=18" --derivatives
bx openneuro compost ds000224 /data --datatypes anat
```

In this mode the source data is kept, unless `--sourcedata` is also given.

* Metadata that still applies to a kept file is kept, and dataset-level files
  are never deleted.
* `participants.tsv`, `*_sessions.tsv` and `*_scans.tsv` are rewritten to list
  only what is left (`--no-trim` to keep them as they are).
* In a dataset hosted in BIDS, the remaining metadata is roasted again, so that
  field maps lose the `IntendedFor` targets that were deleted.
* `--derivatives` applies the subject and session filters to `derivatives/*/`
  as well.

## Confirmation

Compost asks for confirmation before deleting anything. `--list` prints the
files first, and `--yes` skips the question. `--yes` and `--force` are never
read from a configuration file.
