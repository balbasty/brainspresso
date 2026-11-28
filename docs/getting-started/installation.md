# Installation

brainspresso requires Python 3.10 or later. It is not yet on PyPI; install it
from GitHub:

```shell
pip install "brainspresso @ git+https://github.com/balbasty/brainspresso"
```

This installs three equivalent commands: `brainspresso`, `bx`, and
`python -m brainspresso`. The rest of this documentation uses `bx`.

## Optional dependencies

A few datasets need extra packages to read their source files. They are
grouped into *extras* named after the dataset:

| Extra           | Installs   | Needed to roast |
|-----------------|------------|-----------------|
| `ixi`           | `xlrd`     | IXI (demographics in `.xls`) |
| `oasis1`, `oasis2` | `openpyxl` | OASIS-1, OASIS-2 (demographics in `.xlsx`) |
| `unc-wisconsin` | `pynrrd`   | UNC-Wisconsin (images in NRRD) |
| `all`           | all of the above | every dataset |

```shell
pip install "brainspresso[ixi] @ git+https://github.com/balbasty/brainspresso"
pip install "brainspresso[all] @ git+https://github.com/balbasty/brainspresso"
```

Harvesting never needs an extra. If one is missing, `bx <dataset> roast --help`
says which.

[PyYAML](https://pypi.org/project/PyYAML/) is optional: when installed,
[configuration files](../guide/config.md) can also be written in YAML.

## Checking the installation

```shell
bx --help          # lists the supported datasets
bx ixi --help      # describes one dataset and its commands
bx ixi info        # lists its keys and sources
```

## Where datasets are stored

Every command takes the root folder of your datasets as its first positional
argument (`bx ixi harvest /data`). Each dataset gets its own folder below it
(`/data/IXI/`). When the argument is omitted, brainspresso uses `$BDP_PATH`,
then the current directory. A default `path` can also be set in a
[configuration file](../guide/config.md).
