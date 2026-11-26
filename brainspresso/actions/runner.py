"""
Deprecated: moved to `actions.engine` and `brainspresso.roast`.

A thin re-export, kept so that code written against the old path keeps
working; import from the new modules instead.
"""
from brainspresso.actions.engine import (  # noqa: F401
    OUTCOMES, POLL_INTERVAL, WRITE_INTERVAL, LocalExecutor, Shard,
)
from brainspresso.actions.engine import Engine as _Engine  # noqa: F401
from brainspresso.actions.engine import _MainReporter, _Reporter  # noqa
from brainspresso.actions.engine import _ShardTask, _TaskState  # noqa
from brainspresso.roast.context import (  # noqa: F401
    JsonPolicy, RoastContext, as_list, make_context,
)
from brainspresso.roast.run import (  # noqa: F401
    RoastReport, Roaster, roast_dataset, run_roast,
)
