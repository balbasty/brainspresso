import json
import re
import warnings
from fnmatch import fnmatch
from os import PathLike as AbstractPath
from types import TracebackType
from typing import TYPE_CHECKING, IO

from .wrapped_path import WrappedPath

if TYPE_CHECKING:
    from pandas import DataFrame

try:
    from upath import UPath
    Path = UPath
except ImportError:
    UPath = None
    from pathlib import Path

try:
    import pandas as pd
except ImportError:
    pd = None

StrOrBytes = str | bytes
StrOrBytesOrPath = StrOrBytes | AbstractPath
FileLike = StrOrBytesOrPath | IO
TextFileLike = StrOrBytesOrPath | IO[str]
BinaryFileLike = StrOrBytesOrPath | IO[bytes]


REGISTERED_PATH_TYPES = {}


class BidsEntities(dict):
    """Container for BIDS entities."""
    ...


class JSONContent(dict):
    """Container for JSON file content."""

    def write_to(self, file: TextFileLike) -> None:
        """Write content to a JSON file."""
        if not hasattr(file, "write"):
            with Path(file).open("w", encoding="utf-8") as f:
                return self.write_to(f)
        json.dump(self, file, indent=4)

    @classmethod
    def load_from(cls, file: TextFileLike) -> None:
        """Load content from a JSON file."""
        if not hasattr(file, "read"):
            with Path(file).open("r", encoding="utf-8") as f:
                return cls.load_from(f)
        return cls(json.load(file))


class BidsSidecarContent(JSONContent):
    """Container for BIDS sidecar file content."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._inherited = {}

    def keys(self):
        return super().keys() | self._inherited.keys()

    def __getitem__(self, key):
        if key in self:
            return super().__getitem__(key)
        elif key in self._inherited:
            return self._inherited[key]
        else:
            raise KeyError(key)

    def write_to(self, file: StrOrBytesOrPath) -> None:
        """Write content to a JSON file."""
        content_to_write = dict(super().items())
        if content_to_write or Path(file).exists():
            return JSONContent(content_to_write).write_to(file)


class BidsDatasetDescriptionContent(dict):
    """Container for BIDS dataset_description.json content."""
    ...


class BidsPath(WrappedPath):
    """Base class for BIDS paths (files or directories)."""

    def __new__(cls, *args, **kwargs):
        path = super().__new__(cls, *args, **kwargs)
        for pattern, path_type in REGISTERED_PATH_TYPES.items():
            if (
                (isinstance(pattern, re.Pattern) and pattern.match(path.name))
                or
                (isinstance(pattern, str) and fnmatch(path.name, pattern))
                or
                (callable(pattern) and pattern(path))
            ):
                cls = path_type
                break
        return super().__new__(cls, *args, **kwargs)

    @property
    def extension(self) -> str:
        """Return the file extension, including .nii.gz for NIfTI files."""
        if "." in self.name:
            return ".".join(self.name.split(".")[1:])
        else:
            return ""

    def with_extension(self, extension: str) -> "BidsPath":
        """Return a new BidsPath with the given file extension."""
        return type(self)(self.parent / (self.basename + extension))

    @property
    def basename(self) -> str:
        """Return the file name without extensions."""
        return self.name.split(".")[0]

    def with_basename(self, basename: str) -> "BidsPath":
        """Return a new BidsPath with the given file extension."""
        return type(self)(self.parent / (basename + self.extension))

    @property
    def entities(self) -> BidsEntities:
        """Return BIDS entities as a dictionary."""
        parts = self.basename.split("_")
        entities = BidsEntities()
        for part in parts:
            if "-" in part:
                key, *value = part.split("-")
                entities[key] = "-".join(value)
        return entities

    def with_entities(
        self,
        entities: dict | None = None,
        keep_existing: bool = False,
        **extra_entities,
    ) -> "BidsPath":
        """Return a new BidsPath with the given file extension."""
        if keep_existing:
            old_entities = self.entities
            old_entities.update(extra_entities)
            extra_entities = old_entities

        for key, value in (entities or {}).items():
            extra_entities.setdefault(key, value)

        basename = "_".join(
            f"{key}-{value}" for key, value in extra_entities.items()
        )
        if self.suffix:
            basename += f"_{self.suffix}"

        return type(self)(self.parent / (basename + self.extension))

    @property
    def subject(self) -> str | None:
        """Return the subject ID."""
        return self.entities.get("sub", None)

    def with_subject(self, subject: str) -> "BidsPath":
        """Return a new BidsPath with the given subject ID."""
        prev_name = f"sub-{self.subject}"
        new_name = f"sub-{subject}"
        parts = list(self.parts)
        if prev_name in parts:
            parts[parts.index(prev_name)] = new_name
            return type(self)(Path(*parts)).with_subject(subject)

        return self.with_entities(keep_existing=True, sub=subject)

    @property
    def session(self) -> str | None:
        """Return the session ID."""
        return self.entities.get("ses", None)

    def with_session(self, session: str) -> "BidsPath":
        """Return a new BidsPath with the given session ID."""
        prev_name = f"ses-{self.session}"
        new_name = f"ses-{session}"
        parts = list(self.parts)
        if prev_name in parts:
            parts[parts.index(prev_name)] = new_name
            return type(self)(Path(*parts)).with_session(session)

        return self.with_entities(keep_existing=True, ses=session)

    @property
    def suffix(self) -> str | None:
        """
        Return the BIDS suffix.

        !!! warning "This is different from the file extension"
        """
        if "_" in self.name:
            return self.basename.split("_")[-1]
        return None

    def with_suffix(self, suffix: str) -> "BidsPath":
        """Return a new BidsPath with the given BIDS suffix."""
        basename = self.basename.split("_")
        if "-" not in basename[-1]:
            basename = basename[:-1]
        basename.append(suffix)
        basename = "_".join(basename)
        return type(self)(self.parent / (basename + self.extension))

    @property
    def dataset(self) -> "BidsPath" | None:
        """Return the root dataset directory."""
        path = self
        while not (path / "dataset_description.json").exists():
            if path.resolve() == Path("/"):
                return None
            path = path.parent
        return BidsPath(path)


class BidsDirectory(BidsPath):
    """Class for BIDS directories."""

    def __init__(self, *args, **kwargs):
        if_missing: str = kwargs.pop("if_missing", "ignore")
        super().__init__(*args, **kwargs)
        self._post_init(if_missing=if_missing)

    def _post_init(self, if_missing: str = "ignore") -> None:
        if not self.is_dir():
            if if_missing == "raise":
                raise NotADirectoryError(f"{self} is not a directory")
            elif if_missing == "warn":
                warnings.warn(f"{self} is not a directory")
            elif if_missing == "create":
                self.mkdir(parents=True, exist_ok=True)
            elif if_missing == "ignore":
                pass
            else:
                raise ValueError(f"Invalid value for if_missing: {if_missing}")


class BidsDataset(BidsDirectory):
    """Class for BIDS datasets."""

    def __init__(self, *args, **kwargs):
        if_missing: str = kwargs.get("if_missing", "ignore")
        super().__init__(*args, **kwargs)
        self._post_init(if_missing=if_missing)

    def _post_init(self, if_missing: str = "ignore") -> None:
        if not (self / "dataset_description.json").exists():
            if if_missing == "raise":
                raise NotADirectoryError(f"{self} is not a BIDS dataset")
            elif if_missing == "warn":
                warnings.warn(f"{self} is not a BIDS dataset")
            elif if_missing == "create":
                (self / "dataset_description.json").touch()
            elif if_missing == "ignore":
                pass
            else:
                raise ValueError(f"Invalid value for if_missing: {if_missing}")


class BidsFile(BidsPath):
    """Class for BIDS files."""

    def __init__(self, *args, **kwargs):
        if_missing: str = kwargs.pop("if_missing", "ignore")
        super().__init__(*args, **kwargs)
        self._post_init(if_missing=if_missing)

    def _post_init(self, if_missing: str = "ignore") -> None:
        if not self.is_file():
            if if_missing == "raise":
                raise NotADirectoryError(f"{self} is not a BIDS file")
            elif if_missing == "warn":
                warnings.warn(f"{self} is not a BIDS file")
            elif if_missing == "create":
                self.parent.mkdir(parents=True, exist_ok=True)
                self.touch()
            elif if_missing == "ignore":
                pass
            else:
                raise ValueError(f"Invalid value for if_missing: {if_missing}")


class BidsJSONFile(BidsFile):
    """
    A BIDS JSON file.

    Examples
    --------
    >>> sidecar = BidsJSONFile("sub-01_task-rest_bold.json")
    >>>
    >>> print(sidecar.content)                    # load content
    >>> sidecar.content["RepetitionTime"] = 2.0   # modify content in memory
    >>> sidecar.write_content()                   # write changes back to file
    >>>
    >>> # Or use as a context manager (changes are saved on exit)
    >>> with BidsJSONFile("sub-01_task-rest_bold.json") as content:
    ...     content["RepetitionTime"] = 2.0
    """

    ContentType = JSONContent

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.extension == ".json":
            raise ValueError(f"{self} is not a JSON sidecar file")
        self._content = None

    @property
    def content(self) -> BidsSidecarContent:
        """Return the content of the sidecar file as a dictionary."""
        if self._content is None:
            if self.exists():
                self._content = self.ContentType.load_from(self)
            else:
                self._content = {}
            parent = self.parent
            while True:
                for sibling in parent.iterdir():
                    if (
                        sibling.is_file()
                        and sibling.extension == ".json"
                        and sibling.stem == self.stem
                        and sibling != self
                    ):
                        inherited_content = BidsSidecarContent.load_from(sibling)
                        self._content._inherited.update(inherited_content)

        return self._content

    @content.setter
    def content(self, value: dict) -> None:
        if not isinstance(value, dict):
            raise ValueError("Content must be a dictionary")
        self._content = self.ContentType(value)

    def write_content(self) -> None:
        """Write the content back to the JSON file."""
        if self._content is not None:
            self._content.write_to(self)

    def __enter__(self) -> JSONContent:
        return self.content

    def __exit__(
        self,
        exc_type: type | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.write_content()


class BidsTSVFile(BidsFile):
    """A BIDS TSV file.

    Examples
    --------
    >>> tsv = BidsTSVFile("participants.tsv")
    >>>
    >>> print(tsv.content)                   # load content
    >>> tsv.content['sub-0001', 'age'] = 70  # modify content in memory
    >>> tsv.write_content()                  # write changes back to file
    >>>
    >>> # Or use as a context manager (changes are saved on exit)
    >>> with BidsTSVFile("participants.tsv") as content:
    ...     content['sub-0001', 'age'] = 70
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.extension == ".tsv":
            raise ValueError(f"{self} is not a TSV sidecar file")
        self._content = None

    @property
    def content(self) -> DataFrame:
        """Return the content of the sidecar file as a dictionary."""
        if self._content is None:
            if pd is None:
                raise ImportError("pandas is required to read TSV files")
            self._content = pd.read_csv(str(self), sep="\t", index_col=0)
        return self._content

    @content.setter
    def content(self, value: DataFrame) -> None:
        if pd is None:
            raise ImportError("pandas is required to set TSV file content")
        if not isinstance(value, pd.DataFrame):
            raise ValueError("Content must be a pandas DataFrame")
        self._content = value

    def write_content(self) -> None:
        if self._content is not None:
            self._content.to_csv(str(self), sep="\t", index=True, na_rep="n/a")

    def __enter__(self) -> DataFrame:
        return self.content

    def __exit__(
        self,
        exc_type: type | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.write_content()


class BidsDatasetDescriptionFile(BidsJSONFile):
    """A BIDS dataset_description.json file."""
    ...


class BidsSidecarFile(BidsJSONFile):
    """A BIDS sidecar JSON file."""
    ...


class BidsTableSidecarFile(BidsSidecarFile):
    """A BIDS sidecar JSON file for a TSV file."""
    ...


class BidsParticipantsFile(BidsTSVFile):
    """A BIDS participants.tsv file."""
    ...


class BidsParticipantsSidecarFile(BidsTableSidecarFile):
    """A BIDS participants.json file."""
    ...


class BidsSessionsFile(BidsTSVFile):
    """A BIDS sessions.tsv file."""
    ...


class BidsSessionsSidecarFile(BidsTableSidecarFile):
    """A BIDS sessions.json file."""
    ...


class BidsSamplesFile(BidsTSVFile):
    """A BIDS samples.tsv file."""
    ...


class BidsSamplesSidecarFile(BidsTableSidecarFile):
    """A BIDS samples.json file."""
    ...


class BidsScansFile(BidsTSVFile):
    """A BIDS scans.tsv file."""
    ...


class BidsScansSidecarFile(BidsTableSidecarFile):
    """A BIDS scans.json file."""
    ...


class BidsPhenotypeFile(BidsTSVFile):
    """A BIDS <phenotype>.tsv file."""
    ...


class BidsPhenotypeSidecarFile(BidsTableSidecarFile):
    """A BIDS <phenotype>.json file."""
    ...


class BidsCodeDirectory(BidsDirectory):
    """A BIDS code/ directory."""
    ...


REGISTERED_PATH_TYPES.update({
    "dataset_description.json":
        BidsDatasetDescriptionFile,
    "participants.json":
        BidsParticipantsSidecarFile,
    "participants.tsv":
        BidsParticipantsFile,
    "sessions.json":
        BidsSessionsSidecarFile,
    "sessions.tsv":
        BidsSessionsFile,
    "samples.json":
        BidsSamplesSidecarFile,
    "samples.tsv":
        BidsSamplesFile,
    "scans.json":
        BidsScansSidecarFile,
    "*_scans.json":
        BidsScansSidecarFile,
    "scans.tsv":
        BidsScansFile,
    "*_scans.tsv":
        BidsScansFile,
    (lambda x: x.parent.name == "phenotype" and x.suffix == ".json"):
        BidsPhenotypeSidecarFile,
    (lambda x: x.parent.name == "phenotype" and x.suffix == ".tsv"):
        BidsPhenotypeFile,
    (lambda x: x.name == "code"):
        BidsCodeDirectory,
    "*.json":
        BidsSidecarFile,
    "*.tsv":
        BidsTSVFile,
})
