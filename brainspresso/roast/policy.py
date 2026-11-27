"""
Acceptance policy of roast: selection, JSON policy, category folders.

`RoastPolicy` is the `actions.engine.Policy` that `run_roast` gives the
engine:

* An action runs if any of its outputs may be selected
  (`ctx.keeps(path)`: `ctx.selection.keeps` on the path relative to the
  raw dataset, the subject/session filters only outside of it; outputs
  in the folder of a category that is not wanted, e.g.
  `derivatives/<pipeline>/`, are dropped), and if its `kind` passes the
  JSON policy (`--no-sidecars` drops sidecars, `--sidecars-only` data;
  tables and `meta` files (bval/bvec) always pass; so do `staging`
  actions, whose outputs are inputs of later actions).
* The kind of an action that does not set one is that of its outputs
  (`bids.kinds.default_kind`); outputs of several kinds (`[nii, json]`)
  need an explicit `kind`, else the action is an error (`Rejected`).
* An action without outputs (`Fail`) always runs. A claim action runs
  unless none of its claimed paths can be selected
  (`keeps(path, partial=True) is False`).
* `IfExists`: the action's own, else the roast's (`ctx.ifexists`),
  except that under `skip` tables (kind "table", e.g. the pristine
  copies in `<state>/orig/`) use `different`; tables are also
  regenerated and compared (`Action.compare`), so they are replaced only
  if their content changed (e.g. after a new harvest).
* Outputs in `<state>/orig/` (pristine tables) are selected as the
  table they are the pristine copy of (`RoastContext.keeps`).
"""
from __future__ import annotations

from brainspresso.actions.action import Action, IfExists
from brainspresso.actions.engine import Policy, Rejected
from brainspresso.bids.kinds import kinds_of
from brainspresso.roast.context import RoastContext

__all__ = ["RoastPolicy"]


class RoastPolicy(Policy):
    """Selection, JSON policy and kinds of the actions of a roast."""

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
        Set `IfExists` and `compare` of an accepted action.

        Parameters
        ----------
        action : Action
            The action
        ctx : RoastContext
            Context of the roast
        """
        table = action.claim is None and action.kind == "table"
        if not action.ifexists_given:
            policy = IfExists.from_any(ctx.ifexists)
            if policy is IfExists.SKIP and table:
                policy = IfExists.DIFFERENT
            action.ifexists = policy
        if table and not action.compare_given:
            action.compare = True

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
