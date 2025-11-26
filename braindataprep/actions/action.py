import datetime
import os
import time
import traceback
from logging import getLogger
from os.path import lexists
from enum import Enum as _Enum
from typing import Iterable, Iterator, Generator, Literal, Callable, IO
from pathlib import Path
from types import GeneratorType

from braindataprep.utils.digests import sort_digests, get_digest
from braindataprep.actions.file import File, Files

lg = getLogger(__name__)

IfExistsChoice = Literal['skip', 'overwrite', 'different', 'refresh', 'error']
FilenameLike = str | os.PathLike
FileLike = FilenameLike | IO
FilenamesLike = Iterable[FilenameLike] | FilenameLike
FilesLike = Iterable[FileLike] | FileLike


class IfExists:
    """
    This class both:
    - holds the set of singleton values (as an Enum)
    - defines constant (such as the default value)
    - serves as a context manager to override whichever value was set

    ```python
    action = Action(..., ifexists='overwrite')
    with IfExists('refresh'):
        yield from action
    ```
    """

    Choice = Literal['skip', 'overwrite', 'different', 'refresh', 'error']

    class Enum(_Enum):
        SKIP = S = 1
        OVERWRITE = O = 2       # noqa: E741
        DIFFERENT = D = 3
        REFRESH = R = 4
        ERROR = E = 5

    # Expose values
    SKIP = Enum.SKIP
    OVERWRITE = Enum.OVERWRITE
    DIFFERENT = Enum.DIFFERENT
    REFRESH = Enum.REFRESH
    ERROR = Enum.ERROR

    # Set (class attribute) default
    default: Enum = DIFFERENT
    current: Enum | None = None

    @classmethod
    def from_any(cls, x: int | Choice | Enum | None) -> Enum:
        """Return the singleton representation of a value"""
        if x is None:
            return cls.default
        elif isinstance(x, str):
            return getattr(cls.Enum, x[0].upper())
        else:
            return cls.Enum(x)

    def __init__(self, value: Choice | Enum) -> None:
        self.value = self.from_any(value)
        self._prev = None

    def __enter__(self) -> None:
        self._prev = self.default
        type(self).current = self.value

    def __exit__(self, exc_type, exc_val, exc_tb):
        type(self).current = self._prev
        self._prev = None


class Action:
    """
    This object represents an action that generates a file in the tree.

    The file written by the action is given the latest "mtime" of all
    its sources, from which it is derived.

    ```python
    action = Action(
        src='path/to/orig.ext',
        dst='path/to/derived.ext',
        action=lambda out: copy('path/to/orig.ext', out)
        input='path',
    )
    ```

    If status updates are required, the action shoulf be run by
    iterating over it, with each iteration yielding a status dictionary:
    ```python
    for status in action:
        print(status)
    ```

    If status updates are not needed, the action can simply be called:
    ```python
    action()  # or action.run()
    ```

    """

    def __init__(
        self,
        src: FilenamesLike | None,
        dst: FilenamesLike,
        action: Callable[[FilesLike], GeneratorType | None],
        *,
        mode: str = 'wb',
        input: Literal['file', 'path', 'str'] = 'file',
        ifexists: IfExists.Choice = 'different',
        size: int | None = None,
        mtime: datetime.datetime | None = None,
        digests: dict[str, str] | None = None,
    ) -> None:
        """
        Parameters
        ----------
        src : [sequence of] str | Path | None
            Input files
        dst : [sequence of] str | Path
            Output file
        action : Callable[[IO|str|Path],Generator|None]
            Function that takes an opened file object as input and writes
            out a processed file. May or may not be a generator.

        Other Parameters
        ----------------
        mode : {'r', 'w', 'a', '+', 'b', 't'}
            An IO mode.
            If neither 't' nor 'b' is defined, 'b' mode is used by default.
            This differs from python's default behaviour.
        input : {'file', 'path', 'str'}
            Whether the `action` should be given a file object, a
            Path or a string.
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh'}
            Behaviour if destination file already exists
        size : int
            Expected output size, in bytes
        mtime : datetime
            Expected last-modified time
        digests : dict | None
            Expected digest(s) of the file.
            Keys are algorithm names (e.g. "sha256") and values are the
            digests.
        """
        self.src = src or []
        self.dst = dst
        self.action = action
        self.input = input
        if mode == "b" or mode == "t" or mode == "":
            mode = "w" + mode
        if "b" not in mode and "t" not in mode:
            mode = mode + "b"
        self.mode = mode
        self.size = size
        self.mtime = mtime
        if digests:
            digests = sort_digests(digests)
        self.digests = digests
        self.ifexists = IfExists.from_any(ifexists)

    def run(self) -> None:
        """Run the action"""
        for _ in self:
            pass

    def __call__(self) -> None:
        """Run the action (`run` alias)"""
        return self.run()

    def _should_overwrite(self) -> Generator[dict, None, bool]:
        dst = self.dst
        if isinstance(dst, (str, Path)):
            dst = [dst]
        dst = list(map(Path, dst))

        exists = list(map(lexists, dst))

        # Use value set in environment (if there is one)
        ifexists = IfExists.current or self.ifexists
        if IfExists.current:
            lg.debug(f'IfExists from context: {IfExists.current!r}')
        else:
            lg.debug(f'IfExists from object: {self.ifexists!r}')

        if ifexists is IfExists.ERROR and any(exists):
            dst = [f for f, e in zip(dst, exists) if e]
            lg.error(f'File {dst[0]!s} already exists: error')
            raise FileExistsError(f'File {self.dst!r} already exists')

        elif ifexists is IfExists.SKIP and all(exists):
            lg.info(f'File {dst[0]!s} already exists: skip')
            yield {'status': 'skipped', 'message': 'already exists'}
            return False

        elif ifexists is IfExists.OVERWRITE and any(exists):
            dst = [d for d, e in zip(dst, exists) if e]
            lg.info(f'File {dst[0]!s} already exists: overwrite')
            return True

        elif ifexists is IfExists.DIFFERENT:

            # does not exist -> different
            if not all(exists):
                dst = [d for d, e in zip(dst, exists) if e]
                lg.info(f'File {dst[0]!s} does not exist; reprocessing')
                return True

            # different size -> different
            is_different = [
                (
                    self.size is not None and
                    self.size != os.stat(dst1.resolve()).st_size
                )
                for dst1 in dst
            ]
            if any(is_different):
                dst = [f for f, d in zip(dst, is_different) if d]
                lg.info(
                    f'Size of {dst[0]!s} does not match expected size; '
                    f'reprocessing'
                )
                return True

            # different checksum -> different
            if self.digests:
                checkalgo, checksum = next(iter(self.digests.items()))
                is_different = [
                    (
                        get_digest(dst1, checkalgo) != checksum
                    )
                    for dst1 in dst
                ]
                if any(is_different):
                    dst = [f for f, d in zip(dst, is_different) if d]
                    lg.info(
                        f'Checksum of {dst[0]!s} does not match expected '
                        f'checksum; reprocessing'
                    )
                    return True

            # all identical -> skip
            lg.info(f'File {dst[0]!s} is identical: skip')
            yield {'status': 'skipped', 'message': 'already exists'}
            return False

        elif ifexists is IfExists.REFRESH:

            # does not exist -> different
            if not all(exists):
                dst = [d for d, e in zip(dst, exists) if e]
                lg.info(f'File {dst[0]!s} does not exist; reprocessing')
                return True

            # NOTE: It's unlikely we can get an expected output size
            #       I am simply checking whether the mtime of the output
            #       file matches the (most recent) mtime of the input
            #       file(s).
            if self.mtime is None:
                lg.warning(
                    f'{dst[0]!s} - no mtime in the record, '
                    f'reprocessing'
                )
                return True
            # if self.size is None:
            #     lg.warning(
            #         f'{self.dst!r} - no size in the record, '
            #         f'reprocessing'
            #     )
            #     return True
            same_time = [
                datetime.datetime.fromtimestamp(
                    os.stat(dst1.resolve()).st_mtime
                ).astimezone(datetime.timezone.utc) == self.mtime
                for dst1 in dst
            ]
            # same_size = [
            #     os.stat(dst1.resolve()).st_size == self.size
            #     for dst1 in dst
            # ]
            if all(same_time):  # and all(same_size):
                yield {'status': 'skipped', 'message': 'already exists'}
                return False

            dst = [f for f, s in zip(dst, same_time) if not s]
            lg.info(f'File {dst[0]!s} is recent enough: skip')

        return True

    def __iter__(self) -> Iterator[dict]:
        try:
            # Protect source files for reading and perform action
            src = self.src
            if isinstance(src, (str, Path)):
                src = [src]
            with Files(*[File(src1, "r") for src1 in src]):
                yield from self._iter()
        except Exception as e:
            lg.error(str(e) + traceback.format_exc())
            yield {'status': 'error', 'message': str(e)}

    def _iter(self) -> Iterator[dict]:
        dst = self.dst
        if isinstance(dst, (str, Path)):
            dst = [dst]
        dst = list(map(Path, dst))

        # --------------------------------------------------------------
        # Read (most recent) mtime from source(s)
        # --------------------------------------------------------------
        if self.mtime is None:
            src = self.src
            if isinstance(src, (str, Path)):
                src = [src]
            if src:
                self.mtime = max([
                    datetime.datetime.fromtimestamp(
                        os.stat(Path(src1).resolve()).st_mtime
                    ).astimezone(datetime.timezone.utc)
                    for src1 in src
                    if Path(src1).exists()
                ])

        # --------------------------------------------------------------
        # If file exists, select replacement strategy
        # --------------------------------------------------------------
        if not (yield from self._should_overwrite()):
            return

        # --------------------------------------------------------------
        # Perform action
        # --------------------------------------------------------------
        try:
            if self.digests:
                checksum, checkalgo = next(iter(self.digests.items()))
            else:
                checksum = checkalgo = None

            def get_tmp_files(*paths: Path) -> Files:
                return Files(*[File(path, self.mode) for path in paths])

            with get_tmp_files(*dst) as tmp_files:

                # Action input is an opened file-object
                if self.input == 'file':
                    files = [tmp_file.open() for tmp_file in tmp_files]
                    try:
                        action = self.action(*files)
                        if isinstance(action, GeneratorType):
                            yield from action
                    finally:
                        for f in files:
                            f.close()

                # Action input is a path to a file
                else:
                    files = [tmp_file.safename for tmp_file in tmp_files]
                    if self.input == 'str':
                        files = map(str, files)
                    action = self.action(*files)
                    if isinstance(action, GeneratorType):
                        yield from action

            # ----------------------------------------------------------
            # success! -> a few checks then break out of trials loop
            # ----------------------------------------------------------

            if checksum:
                if isinstance(checksum, str):
                    checksum = [checksum] * len(dst)

                for dst1, checksum1 in zip(dst, checksum):
                    outchecksum = get_digest(dst1, checkalgo)

                    if outchecksum != checksum1:
                        msg = (
                            f'{checkalgo}: '
                            f'output {outchecksum} != {checksum1}'
                        )
                        yield {
                            'checksum': 'differs',
                            'status': 'error',
                            'message': msg,
                        }
                        lg.debug(f'{dst1!s} is different: {msg}.')
                        return
                    else:
                        yield {'checksum': 'ok'}
                        lg.debug(
                            f'Verified that {dst1!s} has correct '
                            f'{checkalgo}: {outchecksum}'
                        )

            else:
                yield {'checksum': '-'}

            yield {'status': 'setting mtime'}
            atime = time.time()
            mtime = self.mtime.timestamp() if self.mtime else atime
            for dst1 in dst:
                os.utime(dst1, (atime, mtime))

            yield {'status': 'done'}

        # ----------------------------------------------------------
        # An exception was raised
        # ----------------------------------------------------------

        # When `requests` raises a ValueError, it's because the caller
        # provided invalid parameters (e.g., an invalid URL), and so
        # retrying won't change anything.
        except Exception as e:
            lg.error(str(e) + traceback.format_exc())
            yield {'status': 'error', 'message': str(e)}


class WrapAction(Action):
    """
    An action whose actor takes the source files as input on top of the
    destination path.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.wrapped = self.action
        self.action = lambda fp: self.wrapped(self.src, fp)
