# Harvest

`harvest` downloads the source data of a dataset into `<DS>/sourcedata/`, in
the form it has on its host: archives, folders of images, spreadsheets.

```shell
bx oasis1 harvest /data --discs 1 2 --dry-run
bx oasis1 harvest /data --discs 1 2
```

`--dry-run` prints the files that would be downloaded and exits. `--list`
prints them before downloading.

## Sources

Most datasets can be downloaded from more than one host. Each host is a
*source*, and `bx <dataset> info` lists which keys each source provides and in
which order they are tried:

```console
$ bx abide1 info
...
  source s3: meta, raw
  source nitrc: meta, raw
  source xnat: derivatives, meta, raw
```

`--source` overrides the defaults. With several sources, they are tried in the
given order:

```shell
bx abide1 harvest /data --source nitrc
bx ixi harvest /data --source imperial
```

The download options are shared by every source. Their defaults depend on the
source, and `--help` shows them:

| Option | Meaning |
|--------|---------|
| `--jobs`     | Number of parallel downloads |
| `--packet`   | Chunk size of each download (e.g. `8MB`) |
| `--insecure` | Do not verify TLS certificates |

!!! warning "TLS verification on NITRC"

    The NITRC sources do not verify TLS certificates by default, because
    NITRC's certificate chain is often incomplete. This also applies to the
    login request. Pass `--no-insecure` to verify certificates.

## Credentials

Sources that need an account have their own panel in `--help`, named after the
source. Credentials are looked up in this order:

1. the command-line options (`--user`, `--password`, `--token`);
2. environment variables;
3. an interactive prompt, when standard input is a terminal.

| Service | Options | Environment variables |
|---------|---------|-----------------------|
| NITRC and NITRC-IR (XNAT) | `--user`, `--password` | `NITRC_USER`, `NITRC_PASS` (or `XNAT_USER`, `XNAT_PASS`) |
| Dataverse | `--token` | `DATAVERSE_TOKEN` |
| Girder (data.kitware.com) | `--token` (optional) | `GIRDER_TOKEN` |
| OpenNeuro | `--token` (private datasets only) | none |

Secrets are never read from [configuration files](config.md).

Creating an account and accepting the data-use terms of a dataset is done on
the provider's website, outside brainspresso. Each [origin](../origins/index.md)
page says what is required.

## Existing files

`--if-exists` decides what happens to a file that is already on disk:

| Value | Behaviour |
|-------|-----------|
| `skip` (default) | Leave it alone. Pristine metadata copies in `sourcedata/` still follow the remote dataset. |
| `different` | Replace it if its size, checksum or content differs from the remote file |
| `refresh` | Replace it if its modification time differs |
| `overwrite` | Replace every file |
| `error` | Stop at the first existing file |

Downloads are written to a partial file and renamed when complete, so an
interrupted harvest leaves no truncated files. Running the same command again
resumes the partial downloads.

## Datasets hosted in BIDS

OpenNeuro datasets and most INDI projects are already in BIDS on their host.
For these, harvest uses a *hybrid placement*:

* Images and recordings are downloaded directly to their place in the raw BIDS
  dataset.
* Metadata (sidecars, tables, `events.tsv`, `bval`/`bvec`, `README`,
  descriptions) is downloaded to `sourcedata/<source>/` as an untouched
  *pristine* copy, then roasted into the BIDS dataset.

Roasting the metadata repairs common defects: numbers stored as text, invalid
JSON, slice timing in milliseconds, sessions tables without `session_id`, field
maps whose `IntendedFor` is a tag rather than a path. The pristine copies are
never modified. `--no-repair` keeps their content as is.

The tables (`participants.tsv`, `*_sessions.tsv`, `*_scans.tsv`) are rewritten
to list only what was downloaded, and field maps lose the `IntendedFor` targets
that were not. `--no-trim` keeps them complete. The original tables are kept in
`.brainspresso/orig/`.

Every harvest of a hosted dataset is recorded in `.brainspresso/manifest.json`,
with the snapshot, the filters, the files, their URLs and checksums.
