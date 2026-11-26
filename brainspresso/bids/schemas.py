import json
import re
from abc import ABC, abstractmethod
from fnmatch import fnmatch
from os import PathLike
from pathlib import Path
from typing import (
    Annotated, Callable, GenericAlias, Literal, T, TypeVar, get_origin, get_args
)

try:
    from uritools import isuri
except ImportError:
    isuri = None


def ensure_path(obj: T) -> PathLike:
    if isinstance(obj, PathLike):
        return obj
    else:
        return Path(obj)


def ensure_validator(obj: T) -> "Validator":
    if isinstance(obj, Validator):
        return obj
    elif isinstance(obj, type):
        return HintValidator(obj)
    elif isinstance(obj, re.Pattern):
        return WrappedValidator(lambda x: bool(obj.match(x)))
    elif isinstance(obj, str):
        return WrappedValidator(lambda x: fnmatch(x, obj))
    elif isinstance(obj, dict):
        return JSONSchema(obj)
    elif callable(obj):
        return WrappedValidator(obj)
    else:
        raise TypeError(f"Cannot convert {obj!r} to Validator.")


class Validator(ABC):
    """
    Base class for validators.

    A validator is a callable that takes a value and returns True if the value
    is valid according to some criteria, and False otherwise.
    """

    @abstractmethod
    def __call__(self, value: T) -> bool:
        return NotImplemented


class WrappedValidator(Validator):
    """A validator that wraps a callable."""

    def __init__(self, callable: Callable[[T], bool]) -> None:
        self.callable = callable

    def __call__(self, value: T) -> bool:
        return self.callable(value)


class HintValidator(Validator):
    """A validator that validates a value against a type hint."""

    def __init__(self, hint: type) -> None:
        self.hint = hint

    def __call__(self, value: T) -> bool:
        if get_origin(self.hint) is Annotated:
            cls, *args = get_args(self.hint)
        else:
            cls, *args = (self.hint,)
        if not isinstance(cls, type):
            return False
        for arg in args:
            if get_origin(arg) is Literal:
                if value not in get_args(arg):
                    return False
            if isinstance(arg, Validator):
                if not arg(value):
                    return False
        return True


class Version(tuple):
    ...


VersionSpecifier = GenericAlias(
    type("VersionSpecifier", (), {}),
    (TypeVar("Spec"), )
)
VersionSwitch = GenericAlias(
    type("VersionSpecifier", (), {}),
    (TypeVar("Cond"), TypeVar("A"), TypeVar("B"))
)


class Schema(Validator):
    """Base class for validators that validate structured data."""
    ...


class DirectorySchema(dict, Schema):
    """A validator that validates a directory against a schema."""

    def __call__(self, value: T) -> bool:
        value = ensure_path(value)
        if not value.is_dir():
            return False

        # Check that each fuke in the directory is valid
        for file in value.iterdir():
            valid = False
            for key, schema in self.items():
                key_validator = ensure_validator(key)
                if key_validator(file):
                    schema_validator = ensure_validator(schema)
                    if schema_validator(file):
                        valid = True
                        break
            if not valid:
                return False

        # All checks passed
        return True


class FileSchema(Schema):
    """A validator that validates a file."""

    def __call__(self, value: T) -> bool:
        value = ensure_path(value)
        return value.is_file()


class JSONFileSchema(HintValidator, FileSchema):
    """A validator that validates a JSON file against a schema."""

    def __init__(self, schema: type | dict | "JSONSchema") -> None:
        if isinstance(schema, dict):
            schema = JSONSchema(schema)
        if isinstance(schema, JSONSchema):
            schema = Annotated[dict[str, T], schema]
        super().__init__(schema)

    def __call__(self, value: T) -> bool:
        value = ensure_path(value)
        content = json.loads(value.read_text())
        return super().__call__(content)


class JSONSchema(dict, Schema):
    """A validator that validates a JSON object against a schema."""

    def __call__(self, obj: T) -> bool:
        if not isinstance(obj, dict):
            return False

        # Check that each key-value pair in the object is valid
        for key, value in obj.items():
            valid = False
            for schema_key, schema_hint in self.items():
                key_validator = ensure_validator(schema_key)
                if key_validator(key):
                    schema_validator = ensure_validator(schema_hint)
                    if schema_validator(value):
                        valid = True
                        break
            if not valid:
                return False

        # Check that all required keys are present
        for schema_key, schema_hint in self.items():
            if get_origin(schema_hint) is Annotated:
                if Required in get_args(schema_hint):
                    present = False
                    for key in obj.keys():
                        key_validator = ensure_validator(schema_key)
                        if key_validator(key):
                            present = True
                            break
            if not present:
                return False

        # All checks passed
        return True


RequirementLevel = type("RequirementLevel", (), {})
Required = type("Required", (RequirementLevel,), {})
Recommended = type("Recommended", (RequirementLevel,), {})
Optional = type("Optional", (RequirementLevel,), {})
RequiredIf = GenericAlias(
    type("RequiredIf", (RequirementLevel,)),
    (TypeVar("Cond"), )
)
RecommendedIf = GenericAlias(
    type("RecommendedIf", (RequirementLevel,)),
    (TypeVar("Cond"), )
)


URI = Annotated[str, Validator(isuri)] if isuri else str

SourceDatasetKey = Annotated[str, Literal["URL", "DOI", "Version"]]

Container = Annotated[
    dict[str, T],
    JSONSchema({
        "Type": Annotated[str, Required],
        "Tag": Annotated[str, Required],
        "URI": Annotated[URI, Recommended],
    })
]

GeneratedBy = Annotated[
    dict[str, T],
    JSONSchema({
        "Name": Annotated[str, Required],
        "Version": Annotated[str, Recommended],
        "Description": Annotated[str, Optional],
        "CodeURL": Annotated[URI, Optional],
        "Container": Annotated[Container, Optional],
    })
]

DatasetDescription = Annotated[
    dict[str, T],
    JSONSchema({
        "Name": Annotated[
            str,
            Required
        ],
        "BIDSVersion": Annotated[
            str,
            Required
        ],
        "HEDVersion": Annotated[
            str | list[str],
            Recommended,
            VersionSpecifier[">=1.4.1"]
        ],
        "DatasetLinks": Annotated[
            dict[str, str],
            Optional,
            VersionSpecifier[">=1.8.0"]
        ],
        "DatasetType": Annotated[
            str,
            VersionSwitch[
                VersionSpecifier[">=1.10.1"],
                Literal["raw", "derivative", "study"],
                Literal["raw", "derivative"]
            ],
            Recommended,
            VersionSpecifier[">=1.4.0"]
        ],
        "License": Annotated[
            str,
            Recommended
        ],
        "Authors": Annotated[
            list[str],
            VersionSwitch[
                VersionSpecifier[">=1.10.1"],
                RecommendedIf[
                    (lambda schema: not (schema["CITATION.cff"]).exists())
                ],
                Optional
            ]
        ],
        "Keywords": Annotated[
            list[str],
            Optional,
            VersionSpecifier[">=1.10.1"]
        ],
        "Acknowledgements": Annotated[
            str,
            Optional
        ],
        "HowToAcknowledge": Annotated[
            str,
            Optional
        ],
        "GeneratedBy": Annotated[
            list[GeneratedBy],
            Optional
        ],
        "SourceDatasets": Annotated[
            list[dict[SourceDatasetKey, T]],
            Optional
        ],
    })
]


BIDSDatasetSchema = DirectorySchema({
    "dataset_description.json": JSONFileSchema(DatasetDescription),
    "README": FileSchema(),
})
