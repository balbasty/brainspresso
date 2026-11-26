"""
Actions: stage-agnostic units of work that write files, and their engine.

* `action`: `Action` (outputs `dst` or a `Claim`, opaque `kind`, `tag`,
  `staging`, `compare`), `Fail`, `WrapAction`, `IfExists`;
* `writers`: `WriteJSON`, `WriteTSV`, `CopyBytes`... (`Action`s);
* `archive`: `ExtractMember`, `ExtractMembers` (members of archives);
* `file`: `File`, the files that actions read and write
  (locked, temporary file renamed into place);
* `engine`: drives producers of actions and shards (`Shard`, `Policy`,
  `Context`, `LocalExecutor`, `run`);
* `state`: per-task state files (`<state>/tasks/<id>.json`).

Stage layers build on it: `brainspresso.roast` (convert sources to
BIDS), grind (preprocessing, `docs/design/architecture.md` §7).
"""
from . import action        # noqa: F401
from . import file          # noqa: F401
from . import writers       # noqa: F401
from . import engine        # noqa: F401
from . import state         # noqa: F401

from .action import *       # noqa: F401, F403
from .file import *         # noqa: F401, F403
from .writers import *      # noqa: F401, F403
