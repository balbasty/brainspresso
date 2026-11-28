"""
Acceptance policy of roast: selection, JSON policy, category folders.

`RoastPolicy` is the `actions.engine.Policy` that `run_roast` gives the
engine:

* An action runs if any of its outputs may be selected
  (`ctx.keeps(path)`: `ctx.selection.keeps` on the path relative to the
  raw dataset, the subject/session filters only outside of it; outputs
  in the folder of a category that is not wanted, e.g.
  `derivatives/<pipeline>/`, are dropped), and if its `kind` passes the
  JSON policy (`--sidecars no` drops sidecars, `--sidecars only` data;
  tables and `meta` files (bval/bvec) always pass; so do `staging`
  actions, whose outputs are inputs of later actions).
* The kind of an action that does not set one is that of its outputs
  (`bids.kinds.default_kind`); outputs of several kinds (`[nii, json]`)
  need an explicit `kind`, else the action is an error (`Rejected`).
* An action without outputs (`Fail`) always runs. A claim action runs
  unless none of its claimed paths can be selected
  (`keeps(path, partial=True) is False`).
* `IfExists`: the action's own, else the roast's (`ctx.ifexists`), by
  what the action writes (`prepare`):

  * a **pristine table** (kind "table", every output in
    `<state>/orig/`) is regenerated and compared (`Action.compare`)
    under `skip`, `update` and `different`, so it is replaced only if
    its content changed (e.g. after a new harvest); the visible table
    is trimmed from it after every roast (`bids.harvest.trim_originals`);
  * a **tracked file** (a sidecar or a visible table-kind file such as
    `README` or `participants.json`: kind "sidecar" or "table", one
    declared output, no claim, not staging, no `IfExists` of its own):
    `skip` leaves it alone once it exists (pure skip: no hash read, no
    warning); `different` compares it with what roast writes and
    replaces it if it differs (edits included); `update` does the same,
    but keeps a file the user edited (its SHA-256 differs from the one
    recorded in the manifest) if it differs from the render, with a
    warning (one that holds the render is adopted: a stale hash, e.g.
    of a killed roast, is no edit), and backs up a file without a
    recorded hash that differs to `<state>/replaced/<path>`
    (`bids.harvest.MIGRATION_RULE`) before it replaces it; a tracked
    file that holds what roast writes gets its hash recorded (manifest
    origin "converted", `Report.digests`), whatever the mode;
  * other tables (several outputs): compared under `update` and
    `different`; anything else: the mode as given (`update` is
    `different`).

  An action that sets `compare=False` is left alone under `different`
  and `update` when it exists.
* Outputs in `<state>/orig/` (pristine tables) are selected as the
  table they are the pristine copy of (`RoastContext.keeps`).
"""
from __future__ import annotations

import hashlib
from logging import getLogger
from pathlib import Path

from brainspresso.actions.action import Action, IfExists
from brainspresso.actions.engine import Policy, Rejected
from brainspresso.bids import harvest
from brainspresso.bids.kinds import kinds_of
from brainspresso.bids.layout import ORIG_DIR
from brainspresso.roast.context import RoastContext

lg = getLogger(__name__)

__all__ = ["RoastPolicy"]


class RoastPolicy(Policy):
    """
    Selection, JSON policy and kinds of the actions of a roast.

    Parameters
    ----------
    track : bool
        Track the sidecars and visible table-kind files (their hashes,
        the edit test of `update`); hosted roasts record their own
        hashes (`roast.hosted`) and turn it off
    """

    def __init__(self, track: bool = True):
        self.track = track

    def accept(self, action: Action, ctx: RoastContext, *,
               staging: bool = False) -> bool:
        """
        Decide whether an action of a roast runs (see the module).

        It also sets the kind of an action that has none.

        Parameters
        ----------
        action : Action
            The action
        ctx : RoastContext
            Context of the roast
        staging : bool
            Staging action (or sub-action of a staging claim): exempt
            from the JSON policy

        Returns
        -------
        bool
            Whether it runs

        Raises
        ------
        Rejected
            If its outputs are of several kinds and it has no `kind`
        """
        if staging:
            # (a sub-action of a staging claim is staging too: `prepare`)
            action.staging = True
        if action.claim is not None:
            return not all(ctx.keeps(p, partial=True) is False
                           for p in action.claim.paths)
        declared = action.declared
        if not declared:
            return True             # no output (`Fail`): nothing hides it
        if not action.kind_given:
            kinds = kinds_of(declared)
            if len(kinds) > 1:
                raise Rejected(
                    f"outputs of several kinds ({', '.join(sorted(kinds))})"
                    f": the action needs an explicit `kind`")
            action.kind = kinds.pop()
        if not staging and not ctx.accepts(action.kind):
            return False
        return not all(ctx.keeps(o) is False for o in declared)

    def prepare(self, action: Action, ctx: RoastContext) -> None:
        """
        Set `IfExists`, `compare`, `track` and `backup` of an action.

        See the module.

        Parameters
        ----------
        action : Action
            The action (accepted)
        ctx : RoastContext
            Context of the roast
        """
        table = action.claim is None and action.kind == "table"
        declared = action.declared
        pristine = table and bool(declared) and all(
            _pristine(ctx, p) for p in declared)
        if table and not action.compare_given:
            action.compare = True
        if action.ifexists_given:
            return
        mode = IfExists.from_any(ctx.ifexists)
        if pristine:
            if mode in (IfExists.SKIP, IfExists.UPDATE):
                mode = IfExists.DIFFERENT
            action.ifexists = mode
            return
        tracked = action.claim is None and not action.staging and \
            len(declared) == 1 and action.kind in ("sidecar", "table")
        if not tracked:
            action.ifexists = IfExists.DIFFERENT \
                if mode is IfExists.UPDATE else mode
            return
        action.track = self.track
        action.ifexists = mode
        if mode is IfExists.SKIP:
            return
        if mode in (IfExists.DIFFERENT, IfExists.UPDATE) and \
                not action.compare_given:
            action.compare = True
        if mode is not IfExists.UPDATE:
            return
        action.ifexists = IfExists.DIFFERENT
        if not self.track:
            return
        dst = declared[0]
        state = _state(ctx, dst)
        if state == "unknown" and harvest.MIGRATION_RULE == "replace":
            # replaced (if it differs) with a backup: `Action.backup`
            action.backup = ctx.layout.replaced(dst)
        elif state == "edited":
            # compared first: a file that holds the render (its hash was
            # not recorded, e.g. a killed roast) is adopted, not kept
            action.keep_edit = (
                f"{dst} was edited since brainspresso wrote it: kept "
                f"(--if-exists different or overwrite replaces it)")
        elif state == "unknown":            # MIGRATION_RULE "keep"
            action.keep_edit = (
                f"{dst} has no recorded hash (an older roast, or edited) "
                f"and differs from what brainspresso writes: kept "
                f"(--if-exists different or overwrite replaces it)")

    def fingerprint(self, ctx: RoastContext) -> dict:
        """
        Describe the selection and JSON policy (input signature).

        Parameters
        ----------
        ctx : RoastContext
            Context of the roast

        Returns
        -------
        dict
            `{"json": ..., "selection": ...}`
        """
        selection = getattr(ctx.selection, "fingerprint", None)
        return {
            "json": ctx.json,
            "selection": selection() if selection else repr(ctx.selection),
        }


def _pristine(ctx: RoastContext, path) -> bool:
    """Whether an output is a pristine table (in `<state>/orig/`)."""
    try:
        Path(path).relative_to(Path(ctx.layout.state) / ORIG_DIR)
    except ValueError:
        return False
    return True


def _state(ctx: RoastContext, path) -> str:
    """
    State of a tracked file, by hash.

    Returns
    -------
    {"absent", "ours", "edited", "unknown"}
        "ours": its SHA-256 is the recorded one; "edited": another;
        "unknown": no hash recorded (an older roast)
    """
    path = Path(path)
    if not path.is_file():
        return "absent"
    recorded = ctx.recorded(path)
    if recorded is None:
        return "unknown"
    try:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return "unknown"
    return "ours" if digest == recorded else "edited"
