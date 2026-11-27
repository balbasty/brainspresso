"""
INDI collection: datasets hosted by INDI (`fcon_1000.projects.nitrc.org`).

* The curated projects of `s3://fcp-indi`, harvested as BIDS: one
  subpackage each (`fcon1000/`, `slim/`, `nki_rs/`, ...), whose
  `dataset.py` defines an `engine.Project` (exposed as `bx <project>` by
  `dataset.INDIDataset`, catalog entry `dataset.project_dataset`), next
  to its `templates/` (curated files, `_fill.json`).
* `engine.py`, `harvest.py`, `dataset.py`: the machinery they share.
* Datasets with their own harvest and roast: `abide/i/`, `abide/ii/`,
  `adhd200/`, `corr/`, `cobre/`.
"""
