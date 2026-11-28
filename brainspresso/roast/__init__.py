"""
Roast: convert source data into a BIDS dataset.

The roast layer of the action engine (`actions.engine`):

* `context`: `RoastContext` (layout, key selection, sidecars, templates,
  source folder), `make_context`, `as_list`;
* `policy`: `RoastPolicy`, the acceptance policy (selection, JSON
  policy, category folders, kinds from `bids.kinds`, `IfExists` of
  tables and tracked files: pure `skip`, the edit test of `update`);
* `run`: `run_roast` (engine, then trim and `finish_roast`) and
  `roast_dataset` (the `roast()` of a dataset);
* `helpers`: `copy_templates`, `subject_shards`, `scope` (with `SEEN`:
  what of a roast can change the outputs of a shard).

```python
def roast_ixi(ctx):
    yield from copy_templates(ctx, ["README", "dataset_description.json"])
    yield from subject_shards(ctx, labels, roast_subject)

roast_dataset(roast_ixi, root, KEYS, keys=["T1w"], jobs=4)
```
"""
from brainspresso.actions.engine import LocalExecutor, Shard
from brainspresso.roast.context import (
    JsonPolicy,
    RoastContext,
    as_list,
    make_context,
)
from brainspresso.roast.helpers import copy_templates, scope, subject_shards
from brainspresso.roast.policy import SEEN, RoastPolicy
from brainspresso.roast.run import (
    Roaster,
    RoastReport,
    roast_dataset,
    run_roast,
)

__all__ = [
    "JsonPolicy", "LocalExecutor", "RoastContext", "RoastPolicy",
    "RoastReport", "Roaster", "SEEN", "Shard", "as_list", "copy_templates",
    "make_context", "roast_dataset", "run_roast", "scope", "subject_shards",
]
