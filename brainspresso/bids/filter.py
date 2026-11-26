import re
from glob import glob as iterglob
from pathlib import Path
from typing import Iterator

from .conditions import Globish, Regexish, Condition


PathLike = str | Path
DatasetLike = str | Path | re.Pattern | list[str | Path | re.Pattern]
GlobLike = Globish | str | list[str]
RegexLike = Regexish | str | re.Pattern | list[str | re.Pattern]
ConditionLike = Condition | GlobLike | RegexLike


class Filter:
    """
    Iterate over files in a dataset, keeping only those that match
    a set of conditions.
    """

    def __init__(self, dataset: DatasetLike, **kwargs) -> None:
        """

        Parameters
        ----------
        dataset : [list of] str | Path | re.Pattern
            Dataset(s) to parse. May contain glob or regex patterns.

        Other Parameters
        ----------------
        root : PathLike
            Root of the file system. Required if `dataset` is a `re.Pattern`.
        glob : GlobLike
            A glob pattern for the files to keep.
        regex : RegexLike
            A regular expression pattern for the files to keep.
        modality : ConditionLike
            The name of a BIDS modality (or glob or regex pattern)
        suffix : ConditionLike
            A BIDS suffix (or glob or regex pattern)
        ext : ConditionLike
            A file extension, with leading dot (or glob or regex pattern)

        Note
        ----
        On top of the keyword arguments listed above, all BIDS keys (in
        lower case: `"sub"`, `"ses"` or `"desc"`) and JSON fields (in
        camel case: `"RepetitionTime"` or `"FlipAngle"`) can be used as
        keyword arguments.
        """
        root = Path(kwargs.pop("root", "."))

        # --- Preprocess datasets --------------------------------------
        _dataset, dataset = dataset, []
        if isinstance(_dataset, (str, Path, re.Pattern)):
            _dataset = [_dataset]
        _dataset = list(_dataset)
        for d in _dataset:
            if isinstance(d, re.Pattern):
                # TODO: detect non-regex prefix and use as root
                for path in root.glob("**"):
                    if re.fullmatch(str(path)):
                        dataset.append(path)
            elif isinstance(d, Path):
                dataset.append(d)
            elif isinstance(d, str):
                for path in iterglob(d):
                    dataset.append(Path(d))

        self.datasets = dataset

        # --- Preprocess general conditions ----------------------------
        glob = kwargs.pop("glob", None)
        if glob:
            glob = Globish(glob)
        else:
            glob = Globish("**/*")
        self.glob = glob

        regex = kwargs.pop("regex", None)
        if regex:
            regex = Regexish(regex)
        else:
            regex = (lambda x: True)
        self.regex = regex

        # --- Preprocess specific conditions ---------------------------
        self.condition = (lambda x: True)

    def __iter__(self) -> Iterator[Path]:
        """
        Yields
        ------
        path : Path
            A valid path
        """
        for dataset in self.datasets:
            for path in (dataset / self.glob):
                if not self.regex(path):
                    continue
                if not self.condition(path):
                    continue
                yield path
