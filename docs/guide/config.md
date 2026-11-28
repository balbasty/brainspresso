# Configuration files

Most options can be given defaults in TOML files, so that the command line only
specifies what differs. Values are read from the following places, with
later ones taking precedence:

1. the built-in defaults;
2. `~/.brainspresso/config.toml`, the user file (`$BRAINSPRESSO_CONFIG_HOME`
   replaces `~/.brainspresso`);
3. `<root>/.brainspresso/config.toml`, in the root folder of all datasets;
4. `<root>/<DATASET>/.brainspresso/config.toml`, in the dataset folder (or the
   `--out` folder);
5. files given with `bx --config FILE`, in order (before the dataset name;
   repeatable);
6. `BRAINSPRESSO_OPT_*` environment variables;
7. the command line.

The root folder is the `path` argument if given, else a `path` set in the user
file, `--config` or the environment, else `$BDP_PATH`, else the current
directory.

## Sections

Within a file, the most specific section wins:

| Section | Applies to |
|---------|-----------|
| `[defaults]` | every command of every dataset |
| `[<command>]` | one command of every dataset, e.g. `[roast]` |
| `[<dataset>]` | every command of one dataset, e.g. `[ixi]` |
| `[<dataset>.<command>]` | one command of one dataset, e.g. `[abide1.roast]` |

Datasets are named as on the command line, case-insensitively. OpenNeuro takes
one more level for the accession: `[openneuro.ds000224.harvest]` applies to one
dataset, `[openneuro.harvest]` to all of them.

Keys are the option names without the leading `--`. Both `exclude-keys` and
`exclude_keys` are accepted, and `no-<flag> = true` means `<flag> = false`.

```toml
# ~/.brainspresso/config.toml
[defaults]
path = "/data/bids"        # root of all datasets
level = "info"

[harvest]
jobs = 4
if-exists = "different"

[roast]
jobs = 2

[ixi]
keys = ["T1w", "T2w"]      # harvest and roast of IXI

[abide1.roast]
exclude-keys = ["rest"]

[openneuro.ds000224.harvest]
tag = "1.0.4"
datatype = ["anat"]
```

Files ending in `.yaml` or `.yml` are also accepted when
[PyYAML](https://pypi.org/project/PyYAML/) is installed.

## Environment variables

Each key can also be set with an environment variable:

* `BRAINSPRESSO_OPT_<KEY>` for `[defaults]`;
* `BRAINSPRESSO_OPT_<SECTION>__<KEY>` otherwise, with `__` between the parts
  of a section and `_` in place of `-`.

Names are case-insensitive.

```shell
export BRAINSPRESSO_OPT_HARVEST__JOBS=4
export BRAINSPRESSO_OPT_ABIDE1__ROAST__EXCLUDE_KEYS="rest anat"
export BRAINSPRESSO_OPT_COMPOST__WHERE='"age >= 60" sex==F'
```

Several values are separated by spaces. A value that contains spaces is
quoted, and the quotes are removed. Only whole values can be quoted: `"a b"c`
is two values, `a b` and `c`. Backslashes and quotes inside a value are kept
(`sub-0\d*`, `C:\data`, `O'Brien`).

## What cannot be configured

Some options are never read from a configuration file:

* confirmations of deletions: `--yes`, `--force`, `--ignore-runs`;
* secrets: `--password`, `--token` (use their environment variables or the
  prompt);
* `--config` itself, and deprecated options.

`path` and `out` are read together: if the command line gives either one,
neither is read from a file. A root or dataset file cannot set them, since they
are used to find that file, and `out` is only allowed in the section of a
single dataset.

An unknown section or key, or a value of the wrong type, is an error that names
the file and the section. A value in `[defaults]` or `[<command>]` that is not
valid for a particular dataset, such as `layout = "sites"` for IXI, is ignored
for that dataset.

## Inspecting the configuration

```shell
bx config show                         # the values of every file
bx config show ixi harvest --jobs 2    # every option of a command, and
                                       # where its value comes from
bx config paths ixi                    # the files looked up, and which exist
```
