# Roast

`roast` converts the source data of a dataset into a raw BIDS dataset: images
are converted to NIfTI where needed, sidecars are written from the acquisition
parameters, and demographics become `participants.tsv` and `phenotype/` tables.

```shell
bx oasis2 roast /data
bx oasis2 roast /data --subs 1:20 --keys T1w
```

Roast reads only what is in `sourcedata/` and does not access the network. It
converts what has been harvested. A missing input is an error only when the
selection explicitly asks for it, for example with `--keys`.

`--jobs` runs the conversion in several processes.

## What roast writes

The details depend on the dataset and are described on each
[origin](../origins/index.md) page. Some rules are common to all of them:

* `participants.tsv` lists only the subjects present on disk.
  `participant_id` always includes the `sub-` prefix, and missing values are
  written `n/a`. Rows that cannot be parsed are skipped with a warning.
* Text files are written in UTF-8, with LF line endings.
* Derived data distributed with the dataset (segmentations, registered images,
  FreeSurfer outputs) is written to `derivatives/<name>/`.
* Files that BIDS does not describe are listed in `.bidsignore`.
* The roasters are tested in CI by running them on synthetic data and
  checking the result with the
  [BIDS validator](https://github.com/bids-standard/bids-validator).

For [datasets hosted in BIDS](harvest.md#datasets-hosted-in-bids), roast only
processes the metadata: it renders the pristine copies in
`sourcedata/<source>/` into the BIDS dataset. Running it again after editing or
deleting files restores the metadata without downloading anything.

## Existing files

`--if-exists` takes the same values as for harvest, plus `update`:

| Value | Behaviour |
|-------|-----------|
| `skip` (default) | Leave existing files alone. Tables are still trimmed to what is on disk. |
| `update` | Rewrite the sidecars and metadata files whose content would change, and keep the ones you edited, with a warning |
| `different` | Rewrite every file whose content would change, edits included |
| `refresh` | Rewrite a file whose modification time differs from its sources' |
| `overwrite` | Rewrite everything |
| `error` | Stop at the first existing file |

`update` is the safe way to pick up a newer version of brainspresso: roast
records a hash of each file it writes, so it can tell its own files from those
you have edited. A file it has no record of is backed up to
`.brainspresso/replaced/` before being replaced. Backups are numbered (`.1`,
`.2`, ...) and never overwritten.

## Re-running a roast

Roast splits its work into *shards*, usually one per archive. A shard runs
again only if its inputs, its templates, or the outputs that the current
selection expects have changed. Narrowing `--subs` or toggling an unrelated key
therefore does not re-read every archive, and adding a subject only re-runs
the shards that contain it. A shard that was interrupted is recorded as failed
and runs again next time.
