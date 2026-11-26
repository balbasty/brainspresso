
from os import PathLike as AbstractPath, stat_result
from typing import IO, Iterator

try:
    from upath import UPath
    Path = UPath
except ImportError:
    UPath = None
    from pathlib import Path


BasePath = UPath or AbstractPath
StrOrBytes = str | bytes
StrOrBytesOrPath = StrOrBytes | AbstractPath
FileLike = StrOrBytesOrPath | IO
TextFileLike = StrOrBytesOrPath | IO[str]
BinaryFileLike = StrOrBytesOrPath | IO[bytes]


class WrappedPath(BasePath):
    """A wrapper around PathLike objects to provide Path-like behavior."""

    def __init__(self, path: StrOrBytesOrPath) -> None:
        self._path = Path(path)

    def __fspath__(self) -> str:
        return self._path.__fspath__()

    def __str__(self) -> str:
        return str(self._path)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({str(self._path)!r})"

    def absolute(self) -> "WrappedPath":
        return type(self)(self._path.absolute())

    @property
    def anchor(self) -> str:
        return self._path.anchor

    def as_posix(self) -> str:
        return self._path.as_posix()

    def as_uri(self) -> str:
        return self._path.as_uri()

    def chmod(self, mode: int, *, follow_symlinks: bool = True) -> None:
        self._path.chmod(mode, follow_symlinks=follow_symlinks)

    def cwd(self) -> "WrappedPath":
        return type(self)(self._path.cwd())

    @property
    def drive(self) -> str:
        return self._path.drive

    def exists(self) -> bool:
        return self._path.exists()

    def expanduser(self) -> "WrappedPath":
        return type(self)(self._path.expanduser())

    def glob(self, pattern: str) -> Iterator["WrappedPath"]:
        return (type(self)(p) for p in self._path.glob(pattern))

    def group(self) -> str:
        return self._path.group()

    def hardlink_to(self, target: StrOrBytesOrPath) -> None:
        self._path.hardlink_to(target)

    def home(self) -> "WrappedPath":
        return type(self)(self._path.home())

    def is_absolute(self) -> bool:
        return self._path.is_absolute()

    def is_block_device(self) -> bool:
        return self._path.is_block_device()

    def is_char_device(self) -> bool:
        return self._path.is_char_device()

    def is_dir(self) -> bool:
        return self._path.is_dir()

    def is_fifo(self) -> bool:
        return self._path.is_fifo()

    def is_file(self) -> bool:
        return self._path.is_file()

    def is_mount(self) -> bool:
        return self._path.is_mount()

    def is_relative_to(self, *other: StrOrBytesOrPath) -> bool:
        return self._path.is_relative_to(*other)

    def is_reserved(self) -> bool:
        return self._path.is_reserved()

    def is_socket(self) -> bool:
        return self._path.is_socket()

    def is_symlink(self) -> bool:
        return self._path.is_symlink()

    def iterdir(self) -> Iterator["WrappedPath"]:
        return (type(self)(p) for p in self._path.iterdir())

    def joinpath(self, *args: StrOrBytesOrPath) -> "WrappedPath":
        return type(self)(self._path.joinpath(*args))

    def lchmod(self, mode: int) -> None:
        self._path.lchmod(mode)

    def link_to(self, target: StrOrBytesOrPath) -> None:
        self._path.link_to(target)

    def lstat(self) -> stat_result:
        return self._path.lstat()

    def match(self, pattern: str) -> bool:
        return self._path.match(pattern)

    def mkdir(
        self,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        self._path.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    @property
    def name(self) -> str:
        return self._path.name

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> IO:
        return self._path.open(
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    def owner(self) -> str:
        return self._path.owner()

    @property
    def parent(self) -> "WrappedPath":
        return type(self)(self._path.parent)

    @property
    def parents(self) -> Iterator["WrappedPath"]:
        return type(self.parents)(self)

    @property
    def parts(self) -> tuple[str, ...]:
        return self._path.parts

    def read_bytes(self) -> bytes:
        return self._path.read_bytes()

    def read_text(
        self,
        encoding: str | None = None,
        errors: str | None = None
    ) -> str:
        return self._path.read_text(encoding=encoding, errors=errors)

    def readlink(self) -> str:
        return self._path.readlink()

    def relative_to(self, *other: StrOrBytesOrPath) -> "WrappedPath":
        return type(self)(self._path.relative_to(*other))

    def rename(self, target: StrOrBytesOrPath) -> "WrappedPath":
        return type(self)(self._path.rename(target))

    def replace(self, target: StrOrBytesOrPath) -> "WrappedPath":
        return type(self)(self._path.replace(target))

    def resolve(self, strict: bool = False) -> "WrappedPath":
        return type(self)(self._path.resolve(strict=strict))

    def rglob(self, pattern: str) -> Iterator["WrappedPath"]:
        return (type(self)(p) for p in self._path.rglob(pattern))

    def rmdir(self) -> None:
        self._path.rmdir()

    @property
    def root(self) -> "WrappedPath":
        return type(self)(self._path.root)

    def samefile(self, other: StrOrBytesOrPath) -> bool:
        return self._path.samefile(other)

    def stat(self) -> stat_result:
        return self._path.stat()

    @property
    def stem(self) -> str:
        return self._path.stem

    @property
    def suffix(self) -> str:
        return self._path.suffix

    @property
    def suffixes(self) -> list[str]:
        return self._path.suffixes

    def symlink_to(
        self,
        target: StrOrBytesOrPath,
        target_is_directory: bool = False
    ) -> None:
        self._path.symlink_to(
            target,
            target_is_directory=target_is_directory
        )

    def touch(
        self,
        mode: int = 0o666,
        exist_ok: bool = True,
    ) -> None:
        self._path.touch(mode=mode, exist_ok=exist_ok)

    def unlink(self, missing_ok: bool = False) -> None:
        self._path.unlink(missing_ok=missing_ok)

    def with_name(self, name: str) -> "WrappedPath":
        return type(self)(self._path.with_name(name))

    def with_stem(self, stem: str) -> "WrappedPath":
        return type(self)(self._path.with_stem(stem))

    def with_suffix(self, suffix: str) -> "WrappedPath":
        return type(self)(self._path.with_suffix(suffix))

    def write_bytes(self, data: bytes) -> int:
        return self._path.write_bytes(data)

    def write_text(
        self,
        data: str,
        encoding: str | None = None,
        errors: str | None = None
    ) -> int:
        return self._path.write_text(data, encoding=encoding, errors=errors)
