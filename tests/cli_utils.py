"""Run `bx` commands in tests."""
from brainspresso.cli import main


def bx(*args):
    """Run `bx *args` (fails the test on a non-zero exit status)."""
    try:
        main([str(a) for a in args])
    except SystemExit as e:
        assert not e.code, e.code


def flags(**options) -> list[str]:
    """Command-line flags of keyword options (`subs=["1"]` -> `--sub 1`)."""
    out = []
    for key, value in options.items():
        name = "--" + key.replace("_", "-")
        if value is True:
            out.append(name)
        elif value is False:
            out.append("--no-" + key.replace("_", "-"))
        elif value is None:
            continue
        elif isinstance(value, (list, tuple, set, frozenset)):
            out += [name, *map(str, value)]
        else:
            out += [name, str(value)]
    return out


class Harvest:
    """`bx <dataset> harvest`, called like the old `download` functions."""

    def __init__(self, dataset: str, module=None):
        self.dataset = dataset
        self.module = module

    def download(self, path, **options):
        """`bx <dataset> harvest <path> <flags>`."""
        bx(self.dataset, "harvest", path, *flags(**options))

    def __getattr__(self, name):
        return getattr(self.module, name)
