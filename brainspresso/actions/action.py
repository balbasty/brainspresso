"""
Actions: units of work that write files in the tree.

```python
WriteJSON({"Name": "x"}, "x.json")()                     # run it
for status in CopyBytes(src, dst):                       # with statuses
    print(status)
```

An `Action` declares its outputs (`dst`, or a `Claim` when they are only
known once it has run), reads its sources under shared locks and writes
its outputs under exclusive locks, to temporary files renamed into place
(`actions.file`). `IfExists` decides what to do with outputs that
already exist. The engine (`actions.engine`) runs actions that its
producers yield, through the acceptance policy of a stage layer.
"""
import datetime
import filecmp
import os
import time
import traceback
from contextlib import ExitStack
from dataclasses import dataclass
from enum import Enum as _Enum
from logging import getLogger
from os.path import lexists
from pathlib import Path
from types import GeneratorType
from typing import IO, Callable, Generator, Iterable, Iterator, Literal

from brainspresso.actions.file import File, Files
from brainspresso.utils.digests import get_digest, sort_digests

lg = getLogger(__name__)

IfExistsChoice = Literal['skip', 'overwrite', 'different', 'refresh', 'error']
FilenameLike = str | os.PathLike
FileLike = FilenameLike | IO
FilenamesLike = Iterable[FilenameLike] | FilenameLike
FilesLike = Iterable[FileLike] | FileLike
#: Kind of the outputs of an action: an opaque label, set by the action or
#: by the layer that runs it (e.g. `bids.kinds.Kind` for roast)
Kind = str

__all__ = [
    "Action", "Claim", "Fail", "FileLike", "FilenameLike", "FilenamesLike",
    "FilesLike", "IfExists", "IfExistsChoice", "Kind", "WrapAction",
]


@dataclass(frozen=True)
class Claim:
    """
    Outputs that are only known once an action has run.

    An output directory (or a list of them, e.g. the `func/` and `perf/`
    folders of a session), optionally followed by the known start of the
    file names (`<dir>/sub-01_ses-2_acq-mprage`). Matched with
    `Selection.keeps(..., partial=True)` by roast: the claim can be
    selected if any of its paths can.

    Parameters
    ----------
    folder : path | list[path]
        Output folder(s)
    fragment : str
        Known start of the file names (in each folder)
    """

    folder: Path | tuple[Path, ...]
    fragment: str = ''

    def __post_init__(self):
        """Normalize `folder` (a path, or a tuple of several)."""
        folder = self.folder
        if not isinstance(folder, (str, os.PathLike)):
            folder = tuple(map(Path, folder))
            if not folder:
                raise ValueError('A claim needs at least one folder')
            if len(folder) == 1:
                folder = folder[0]
        else:
            folder = Path(folder)
        object.__setattr__(self, 'folder', folder)

    @property
    def folders(self) -> tuple[Path, ...]:
        """Claimed folders, as a tuple."""
        folder = self.folder
        return folder if isinstance(folder, tuple) else (folder,)

    @property
    def paths(self) -> tuple[Path, ...]:
        """Claimed paths (one per folder)."""
        return tuple(f / self.fragment if self.fragment else f
                     for f in self.folders)

    @property
    def path(self) -> Path:
        """First claimed path."""
        return self.paths[0]


class IfExists:
    """
    What to do with outputs that already exist.

    This class:

    - holds the set of singleton values (as an Enum)
    - defines constants (such as the default value)
    - serves as a context manager to override whichever value was set

    Values: "skip" (if all outputs exist), "overwrite", "different"
    (if the size, digest or, with `Action.compare`, the content differs),
    "refresh" (if the mtime differs from that of the sources), "error".

    ```python
    action = Action(..., ifexists='overwrite')
    with IfExists('refresh'):
        yield from action
    ```
    """

    Choice = Literal['skip', 'overwrite', 'different', 'refresh', 'error']

    class Enum(_Enum):
        """Singleton values."""

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
        """
        Return the singleton representation of a value.

        Parameters
        ----------
        x : int | str | Enum | None
            A value (a string is matched on its first letter; None: the
            default)

        Returns
        -------
        Enum
            The singleton
        """
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
        """Override the value of every action, until exit."""
        # restore the previous override on exit (not the default, which
        # would leave an override in place for the whole process)
        self._prev = type(self).current
        type(self).current = self.value

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore the previous override."""
        type(self).current = self._prev
        self._prev = None


class Action:
    """
    An action that generates a file (or several) in the tree.

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

    Outputs
    -------
    An action declares its outputs either as `dst` (one path, or a list
    of paths written together, e.g. DWI nii + bval + bvec) or as a
    `claim` (`Claim(folder, fragment)`) when they are only known once
    it has run (members of an archive, dcm2niix, FreeSurfer trees). A
    claim action is run with `dst=None`: `action()` takes no argument,
    and yields (besides status dictionaries) the concrete actions that
    write each output, which the engine (`actions.engine`) then accepts
    and runs like any other (so that `IfExists`, atomic writes and
    counters apply). A claim never writes files itself. Once run,
    `outputs` lists what was written (or found already there).

    * `kind` is an opaque label of the outputs, used by the acceptance
      policy of the layer that runs the action (`actions.engine.Policy`;
      roast computes it from the names of the outputs with
      `bids.kinds.default_kind` when it is not given, for its
      sidecar policy, `--sidecars`, `--no-sidecars`, `--sidecars-only`);
    * `tag` groups actions in the progress display;
    * `staging=True` marks intermediate files (e.g. a FreeSurfer tree
      unpacked to `derivatives/<fs>/sourcedata/`, input of later
      actions), for the acceptance policy (roast does not apply its
      JSON policy to them, nor to the sub-actions of a staging claim);
    * `compare=True` (outputs that are cheap to make, e.g. tables):
      under `IfExists` "different", without an expected size or digest,
      the outputs are written to temporary files and replace the
      existing ones only if their content differs.

    Sources are read under shared locks, outputs written under exclusive
    locks (`actions.file`). A source that is also an output (an update
    in place, e.g. `WrapAction(path, path, fix)`) is only locked as an
    output: the actor reads the current file under the exclusive lock.
    """

    def __init__(
        self,
        src: FilenamesLike | None,
        dst: FilenamesLike | None,
        action: Callable[[FilesLike], GeneratorType | None],
        *,
        mode: str = 'wb',
        input: Literal['file', 'path', 'str'] = 'file',
        ifexists: IfExists.Choice | None = None,
        size: int | None = None,
        mtime: datetime.datetime | None = None,
        digests: dict[str, str] | None = None,
        claim: Claim | None = None,
        kind: Kind | None = None,
        tag: str | None = None,
        staging: bool = False,
        compare: bool | None = None,
    ) -> None:
        """
        Build an action.

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
            Behaviour if destination file(s) already exist (default:
            that of the policy of the engine, else 'different'). With
            several
            outputs, it applies to the set (e.g. 'skip' if all exist).
        size : int
            Expected output size, in bytes
        mtime : datetime
            Expected last-modified time
        digests : dict | None
            Expected digest(s) of the file.
            Keys are algorithm names (e.g. "sha256") and values are the
            digests.
        claim : Claim | None
            Outputs only known once run (instead of `dst`)
        kind : str | None
            Kind of the outputs (default: set by the layer that runs
            the action, e.g. roast's `bids.kinds.default_kind`)
        tag : str | None
            Progress group (default: that of the producer that yields
            the action)
        staging : bool
            Intermediate files (see `Outputs`)
        compare : bool | None
            Compare regenerated outputs under "different" (see
            `Outputs`; None: not given, False unless the layer that
            runs the action sets it)
        """
        if (dst is None) == (claim is None):
            raise ValueError('An action has either `dst` or a `claim`')
        self.src = src or []
        self.dst = dst
        self.claim = claim
        self.tag = tag
        self.kind: Kind | None = kind
        self.kind_given = kind is not None
        self.staging = staging
        self.compare_given = compare is not None
        self.compare = bool(compare)
        self.outputs: list[Path] | None = None
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
        self.ifexists_given = ifexists is not None
        self.ifexists = IfExists.from_any(ifexists)

    @property
    def declared(self) -> list[Path]:
        """Declared outputs (`dst` as a list; empty for a claim)."""
        dst = self.dst
        if dst is None:
            return []
        if isinstance(dst, (str, os.PathLike)):
            dst = [dst]
        return list(map(Path, dst))

    def run(self) -> None:
        """Run the action."""
        for _ in self:
            pass

    def __call__(self) -> None:
        """Run the action (`run` alias)."""
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
                dst = [d for d, e in zip(dst, exists) if not e]
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

            # cheap outputs (e.g. tables): write them and compare
            if not self.digests and self.size is None and self.compare:
                return _COMPARE

            # all identical -> skip
            lg.info(f'File {dst[0]!s} is identical: skip')
            yield {'status': 'skipped', 'message': 'already exists'}
            return False

        elif ifexists is IfExists.REFRESH:

            # does not exist -> different
            if not all(exists):
                dst = [d for d, e in zip(dst, exists) if not e]
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
        """Run the action, yielding statuses (and a claim's actions)."""
        try:
            # Protect source files for reading and perform action
            with Files(*[File(src1, "r") for src1 in self._read_sources()]):
                if self.claim is not None:
                    yield from self._iter_claim()
                    return
                outcome = None
                for status in self._iter():
                    outcome = status.get('status', outcome)
                    yield status
                self.outputs = (
                    self.declared if outcome in ('done', 'skipped') else []
                )
        except Exception as e:
            lg.error(str(e) + traceback.format_exc())
            yield {'status': 'error', 'message': str(e)}

    def _read_sources(self) -> list[FilenameLike]:
        """
        List the sources to lock for reading (those that are not outputs).

        An action that updates a file in place (a source that is also
        a destination) reads it under the exclusive lock of its output,
        which a shared lock taken first would forbid (lock upgrade).
        """
        src = self.src
        if isinstance(src, (str, os.PathLike)):
            src = [src]
        outputs = {_same_file_key(dst) for dst in self.declared}
        return [s for s in src if _same_file_key(s) not in outputs]

    def _iter_claim(self) -> Iterator["dict | Action"]:
        """Run a claim action: statuses and sub-actions."""
        if self.outputs is None:
            self.outputs = []
        out = self.action()
        if out is not None and not isinstance(out, Iterator):
            raise TypeError(
                'A claim yields its sub-actions; it returned '
                f'{type(out).__name__}'
            )
        for item in out or ():
            if not isinstance(item, (Action, dict)):
                raise TypeError(
                    'A claim yields actions or status dictionaries, '
                    f'not {type(item).__name__}'
                )
            yield item
        yield {'status': 'done'}

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
        overwrite = yield from self._should_overwrite()
        if not overwrite:
            return
        compare = overwrite is _COMPARE

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
                    # `File.open()` returns a context manager: the
                    # underlying file is only opened when entered.
                    with ExitStack() as stack:
                        files = [
                            stack.enter_context(tmp_file.open())
                            for tmp_file in tmp_files
                        ]
                        action = self.action(*files)
                        if isinstance(action, GeneratorType):
                            yield from action

                # Action input is a path to a file
                else:
                    files = [tmp_file.safename for tmp_file in tmp_files]
                    if self.input == 'str':
                        files = map(str, files)
                    action = self.action(*files)
                    if isinstance(action, GeneratorType):
                        yield from action

                if compare and all(
                    filecmp.cmp(tmp_file.safename, dst1, shallow=False)
                    for tmp_file, dst1 in zip(tmp_files, dst)
                ):
                    for tmp_file in tmp_files:
                        tmp_file.discard = True

            if compare and all(f.discard for f in tmp_files.files):
                lg.info(f'File {dst[0]!s} is identical: skip')
                yield {'status': 'skipped', 'message': 'identical'}
                return

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


def _keep_all(path) -> bool:
    return True


def _same_file_key(path: FilenameLike) -> str:
    """Absolute path, with the links of its folder resolved (as locks)."""
    path = os.path.abspath(path)
    folder, name = os.path.split(path)
    return os.path.join(os.path.realpath(folder), name)


# `_should_overwrite`: write to a temporary file, keep it if different
_COMPARE = 'compare'


class Fail(Action):
    """
    An action that only reports an error (e.g. a missing input).

    The engine counts and displays it. It has no outputs: no policy
    hides it (neither the selection nor the JSON policy of roast).

    Parameters
    ----------
    message : str
        The error
    name : str
        Name shown in the display
    tag : str
        Progress group
    """

    def __init__(self, message: str, *, name: str = '', tag: str = None):
        super().__init__([], [], _keep_all, tag=tag)
        self.message = message
        self.name = name

    def __iter__(self) -> Iterator[dict]:
        """Report the error."""
        lg.error(self.message)
        self.outputs = []
        yield {'status': 'error', 'message': self.message}


class WrapAction(Action):
    """
    An action whose actor also takes the source files.

    `action(src, dst)` instead of `action(dst)`; same arguments as
    `Action`.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.wrapped = self.action
        self.action = lambda fp: self.wrapped(self.src, fp)
