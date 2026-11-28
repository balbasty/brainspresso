"""
Roast: convert source data into a BIDS dataset.

The roast layer of the action engine (`actions.engine`):

* `context`: `RoastContext` (layout, key selection, sidecars, templates,
  source folder), `make_context`, `as_list`;
* `policy`: `RoastPolicy`, the acceptance policy (selection, JSON
  policy, category folders, kinds from `bids.kinds`, `IfExists` of
  tables and tracked files: pure `skip`, the edit test of `update`);
* `run`: `run_roast` (engine, then trim and `finish_roast`),
  `roast_dataset` (context and run) and `python_api` (the `roast()` of
  a dataset, with the arguments of the command line);
* `helpers`: `copy_templates`, `subject_shards`, `archive_shards`,
  `scope` (with `SEEN`: what of a roast can change the outputs of a
  shard), `attempt` (a reader's error as a `Fail`), `named_keys` and
  `missing_input` (a missing input is an error only when it is named);
* `archives`: reading source archives (`reading`, `ArchiveError`, the
  spools of ABIDE-II);
* `tables`: source tables read (`read_csv`, `read_xlsx`, `read_xls`),
  cells cleaned (`clean`), roasted tables written (`table_action`);
* `sidecars`: sidecars from templates (`template_config`, sidecar
  templates in a shard's `config`; `copy_sidecar`, `read_template`,
  `bold_sidecar` and the TR policy, `repetition_time`).

Converters of image formats are `brainspresso.convert`.

```python
def roast_ixi(ctx):
    yield from copy_templates(ctx, ["README", "dataset_description.json"])
    yield from subject_shards(ctx, labels, roast_subject)

roast_dataset(roast_ixi, root, KEYS, keys=["T1w"], jobs=4)
```
"""
from brainspresso.actions.engine import LocalExecutor, Shard
from brainspresso.roast import tables
from brainspresso.roast.context import (
    JsonPolicy,
    RoastContext,
    as_list,
    make_context,
)
from brainspresso.roast.helpers import (
    attempt,
    copy_templates,
    missing_input,
    named_keys,
    scope,
    subject_shards,
)
from brainspresso.roast.policy import SEEN, RoastPolicy
from brainspresso.roast.run import (
    Roaster,
    RoastReport,
    python_api,
    roast_dataset,
    run_roast,
)

__all__ = [
    "JsonPolicy", "LocalExecutor", "RoastContext", "RoastPolicy",
    "RoastReport", "Roaster", "SEEN", "Shard", "as_list", "attempt",
    "copy_templates", "make_context", "missing_input", "named_keys",
    "python_api", "roast_dataset", "run_roast", "scope", "subject_shards",
    "tables",
]
