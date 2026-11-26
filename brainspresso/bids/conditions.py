import fnmatch
import os
import re
from functools import reduce
from pathlib import Path
from typing import Iterator


class NotACondition:

    class Error(TypeError):
        ...

    def __init__(self, *args) -> None:
        raise self.Error("Not a condition:", *args)


class NotAGlob(NotACondition):

    class Error(NotACondition.Error):
        ...

    def __init__(self, *args) -> None:
        raise self.Error("Not a glob:", *args)


class NotARegex(NotACondition):

    class Error(NotACondition.Error):
        ...

    def __init__(self, *args) -> None:
        raise self.Error("Not a regex:", *args)


class Condition:

    def __call__(self, value: str | Path) -> bool:
        """Check whether a path fullfills the condition."""
        return NotImplemented


class Regexish(Condition):

    def __new__(cls, *args) -> "Globish":
        if cls is not Regexish:
            return super().__new__(cls, *args)
        elif len(args) > 1:
            return And(*map(Regexish, args))
        elif len(args) == 0:
            raise ValueError("At least one argument required")
        elif isinstance(args, Condition):
            return args[0]
        else:
            return Regex(args[0])


class Regex(Regexish):

    def __init__(self, pattern: re.Pattern | str) -> None:
        if isinstance(pattern, str):
            pattern = re.compile(pattern)
        self.pattern = pattern

    def __str__(self) -> str:
        return str(self.pattern)

    def __repr__(self) -> str:
        return f"Regex({str(self)})"

    def __call__(self, value: str | Path) -> bool:
        try:
            next(self.pattern.finditer(str(value)))
            return True
        except StopIteration:
            return False


class Globish(Condition):

    def __new__(cls, *args) -> "Globish":
        if cls is not Globish:
            return super().__new__(cls, *args)
        elif len(args) > 1:
            return OrGlob(*map(Globish, args))
        elif len(args) == 0:
            raise ValueError("At least one argument required")
        elif isinstance(args, Condition):
            return args[0]
        else:
            return Glob(args[0])


class Glob(Globish):

    def __init__(self, pattern: str) -> None:
        if pattern[:1] == os.sep:
            root = Path("/")
            pattern = pattern[1:]
        else:
            root = Path(".")
        self.pattern = pattern
        self.root = root

    def __str__(self) -> str:
        return str(self.root / self.pattern)

    def __repr__(self) -> str:
        return f"Glob({str(self)})"

    def __call__(self, value: str | Path) -> bool:
        return bool(fnmatch.filter([value], self.pattern))

    def __iter__(self) -> Iterator[Path]:
        """Iterate over paths in the glob."""
        yield from self.root.glob(self.pattern)

    def __truediv__(self, other: str | Path | Globish) -> Globish:
        if isinstance(other, str):
            other = Path(other)
        if isinstance(other, Path):
            return Glob()



class And(Condition):

    def __init__(self, *cond: Condition | re.Pattern) -> None:
        cond = [
            Regex(c) if isinstance(c, re.Pattern) else
            Glob(c)
            for c in cond
        ]
        if isinstance(cond, re.Pattern):
            cond = Regex(cond)
        self.cond = cond

    def __call__(self, value: str | Path) -> bool:
        return all(cond(value) for cond in self.cond)


class Or(Condition):

    def __init__(self, *cond: Condition | re.Pattern) -> None:
        cond = [
            c if isinstance(c, Condition) else
            Regex(c) if isinstance(c, re.Pattern) else
            Glob(c) if isinstance(c, str) else
            NotACondition(c)
            for c in cond
        ]
        if isinstance(cond, re.Pattern):
            cond = Regex(cond)
        self.cond = cond

    def __call__(self, value: str | Path) -> bool:
        return any(cond(value) for cond in self.cond)


class OrGlob(Or, Globish):

    def __init__(self, *cond) -> None:
        super().__init__(*cond)
        if not all(isinstance(cond, (Glob, OrGlob)) for cond in self.cond):
            raise NotAGlob.Error("All conditions must be globs")

    def __iter__(self) -> Iterator[Path]:
        """Iterate over the union of all glob patterns."""
        yield from set(path for cond in self.cond for path in cond)


class AndGlob(And, Globish):

    def __init__(self, *cond) -> None:
        super().__init__(*cond)
        if not all(isinstance(cond, (Glob, AndGlob)) for cond in self.cond):
            raise NotAGlob.Error("All conditions must be globs")

    def __iter__(self) -> Iterator[Path]:
        """Iterate over the intersection of all glob patterns."""
        yield from reduce(set.intersection, map(set, self.cond))


class PathKey(Condition):

    def __init__(self, cond: str | re.Pattern | )
