"""
Dump the flags of every dataset command as JSON.

```
python scripts/cli_snapshot.py > tests/data/cli_flags.json
```

For each `bx <dataset> <command>`: every flag (and positional
parameter) with its choices and default. `tests/test_cli.py` compares
the current CLI with the snapshot taken before step 4 (every flag of
the snapshot must still exist, with the same default and at least the
same choices, except for the documented exceptions).
"""
import enum
import json
import sys
import typing


def choices(hint) -> list | None:
    """Values of the Literal/Enum types found in a type hint."""
    out = []

    def walk(h):
        origin = typing.get_origin(h)
        if origin is typing.Literal:
            out.extend(str(v) for v in typing.get_args(h))
        elif isinstance(h, type) and issubclass(h, enum.Enum):
            out.extend(str(m.value) for m in h)
        elif origin is typing.Annotated:
            walk(typing.get_args(h)[0])
        else:
            for arg in typing.get_args(h) or ():
                walk(arg)

    walk(hint)
    return sorted(set(out)) if out else None


def normalize(value):
    """JSON-able default."""
    if isinstance(value, enum.Enum):
        return normalize(value.value)
    if isinstance(value, (list, tuple, set, frozenset)):
        return sorted(normalize(v) for v in value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, type) and issubclass(value, enum.Enum):
        return sorted(str(m.value) for m in value)
    return str(value)


def flags(command) -> dict:
    """Flags of one command app."""
    out = {}
    for arg in command.assemble_argument_collection(parse_docstring=True):
        if not arg.show or not arg.names:
            continue
        default = arg.field_info.default
        if default is arg.field_info.empty:
            default = None
        name = arg.names[0]
        out[name] = {
            "choices": choices(arg.hint),
            "default": normalize(default),
            "names": sorted(arg.names),
        }
    return out


def snapshot(app) -> dict:
    """Flags of every `bx <dataset> <command>`."""
    out = {}
    for name in sorted(app):
        if name.startswith("-"):
            continue
        dataset = app[name]
        commands = {}
        for cmd in sorted(dataset):
            if cmd.startswith("-"):
                continue
            commands[cmd] = flags(dataset[cmd])
        out[name] = commands
    return out


if __name__ == "__main__":
    from brainspresso.cli import app
    json.dump(snapshot(app), sys.stdout, indent=1, sort_keys=True)
    print()
