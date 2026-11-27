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
* `IfExists`: the action's own, else the roast's (`ctx.ifexists`),
  except that under `skip` tables (kind "table", e.g. the pristine
  copies in `<state>/orig/`) use `different`; tables are also
  regenerated and compared (`Action.compare`), so they are replaced only
  if their content changed (e.g. after a new harvest).
* Outputs in `<state>/orig/` (pristine tables) are selected as the
  table they are the pristine copy of (`RoastContext.keeps`).

Scope of a shard
----------------
A shard that declares `scope=roast.scope(keys=..., subs=...)` is signed
by its inputs and `config` only (`actions.engine`); `RoastPolicy.scope`
describes the run as far as it matters to the shard:

```
{"fixed": {"json": ..., "base": <selection minus subjects>,
           "declared": {"keys": [...] | None, "subs": [...] | "seen" | None}},
 "keys": [wanted keys among the declared ones],
 "mode": "all" | "exclude" | "keys" | None,
 "subs": {"kept": [declared subjects kept]}
       | {"subs": [...], "exclude": [...]}}
```

A recorded scope **covers** a new one (`covers`: the new run writes no
more than the recorded one did) if `fixed` is equal, the new keys are
among the old ones, the new key mode keeps no more (`all` > `exclude` >
`keys`, `bids.keys` rules 2, 6, 3-5; None: the declared keys hold no
BIDS key, and the mode does not matter), and the new subjects are among
the old ones: declared lists by inclusion; with `SEEN` and the subjects
that the recorded run saw, every seen subject that the new filter keeps
is kept by the old one; else by the filters themselves. It rests on two
promises of the roaster: the selection decides which outputs exist,
never their content (what else changes it is in `config`), and a
selection that keeps no more writes no more. `audit` checks that the
outputs of a run are inside its declaration (keys, category folders,
subjects); `expects` limits the existence test of a skipped shard to
the outputs that the run still selects.
"""
from __future__ import annotations

import json
from pathlib import Path, PurePosixPath

from brainspresso.actions.action import Action, IfExists
from brainspresso.actions.engine import Policy, Rejected
from brainspresso.bids.keys import KeySelection, normalize
from brainspresso.bids.kinds import kinds_of
from brainspresso.roast.context import RoastContext

__all__ = ["RoastPolicy", "SEEN"]

#: `scope(subs=SEEN)`: the shard reports every subject its inputs hold
#: (`RoastContext.keeps_subject`)
SEEN = "seen"
#: Key modes, from the one that keeps the least
_MODES = {"keys": 0, "exclude": 1, "all": 2}


def _keeps(filters: dict, label: str) -> bool:
    """`Selection.keeps_subject` on recorded subject filters."""
    subs = filters.get("subs") or ()
    return (not subs or label in subs) \
        and label not in (filters.get("exclude") or ())


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

    # ------------------------------------------------------------------
    #   Scope of shards (see the module)
    # ------------------------------------------------------------------
    def scope(self, ctx: RoastContext, shard) -> dict | None:
        """
        Describe the roast as far as a shard declares it matters.

        Parameters
        ----------
        ctx : RoastContext
            Context of the roast
        shard : Producer
            The shard (`shard.scope`: `roast.scope(...)`, or None)

        Returns
        -------
        dict | None
            Its scope (see the module); None if it declares none, or if
            the selection is not a `KeySelection` (hosted roasts)
        """
        declared = getattr(shard, "scope", None)
        selection = ctx.selection
        if declared is None or not isinstance(selection, KeySelection):
            return None
        keys, subs = declared.get("keys"), declared.get("subs")
        wanted = selection.wanted
        if wanted is None:
            kept = None if keys is None else sorted(keys)
        else:
            kept = sorted(wanted if keys is None else set(wanted) & set(keys))
        bids = set(getattr(ctx.aliases, "bids", None) or ())
        if keys is not None and not set(keys) & bids:
            mode = None             # (no BIDS key: rules 2-6 do not apply)
        elif not selection.filtered:
            mode = "all"
        else:
            mode = "exclude" if selection.exclude_only else "keys"
        base = {k: v for k, v in normalize(selection.base).items()
                if k not in ("subs", "exclude_subs")}
        if isinstance(subs, list):
            kept_subs = {"kept": [s for s in subs
                                  if selection.keeps_subject(s)]}
        else:
            kept_subs = {"subs": sorted(selection.base.subs),
                         "exclude": sorted(selection.base.exclude_subs)}
        # (as it reads back from a record)
        return json.loads(json.dumps({
            "fixed": {"json": ctx.json, "base": base,
                      "declared": {"keys": None if keys is None
                                   else sorted(keys), "subs": subs}},
            "keys": kept,
            "mode": mode,
            "subs": kept_subs,
        }, default=str))

    def covers(self, old: dict, new: dict, seen: dict) -> bool:
        """
        Whether a roast of scope `new` writes no more than one of `old`.

        Parameters
        ----------
        old : dict
            A scope of the record
        new : dict
            The scope of this roast
        seen : dict
            `{"sub": [labels]}` that the recorded run saw (`SEEN`
            shards; empty if none)

        Returns
        -------
        bool
            See the module
        """
        if not isinstance(old, dict) or old.get("fixed") != new["fixed"]:
            return False
        if new["keys"] is None:
            if old.get("keys") is not None:
                return False
        elif old.get("keys") is not None and \
                not set(new["keys"]) <= set(old["keys"]):
            return False
        if new["mode"] is not None and \
                _MODES[new["mode"]] > _MODES.get(old.get("mode"), -1):
            return False
        before, after = old.get("subs") or {}, new["subs"]
        if "kept" in after:
            return set(after["kept"]) <= set(before.get("kept", ()))
        if new["fixed"]["declared"]["subs"] == SEEN and "sub" in seen:
            return all(_keeps(before, s) for s in seen["sub"]
                       if _keeps(after, s))
        # (subjects unknown: by the filters themselves)
        if not before.get("subs"):
            return all(not _keeps(after, s)
                       for s in before.get("exclude") or ())
        return bool(after.get("subs")) and all(
            _keeps(before, s) for s in after["subs"] if _keeps(after, s))

    def expects(self, path: Path, ctx: RoastContext) -> bool:
        """
        Whether a recorded output is selected by this roast.

        Parameters
        ----------
        path : Path
            The output
        ctx : RoastContext
            Context of the roast

        Returns
        -------
        bool
            `ctx.keeps(path) is not False`
        """
        return ctx.keeps(path) is not False

    def audit(self, ctx: RoastContext, scope: dict, outputs: list[str],
              seen: dict) -> str | None:
        """
        Find an output outside the declared scope of a shard.

        Parameters
        ----------
        ctx : RoastContext
            Context of the roast
        scope : dict
            Its scope (`scope`)
        outputs : list[str]
            Outputs of the run (relative to the dataset folder)
        seen : dict
            `{"sub": [labels]}` that the run reported

        Returns
        -------
        str | None
            The first output that is a raw data file of a key that is
            not declared, in the folder of categories none of which is
            declared, or of a subject that is neither declared nor seen
            (`SEEN`); None if all are inside
        """
        declared = scope["fixed"]["declared"]
        keys, subs = declared["keys"], declared["subs"]
        if isinstance(subs, list):
            subjects = set(subs)
        elif subs == SEEN:
            subjects = set(seen.get("sub") or ())
        else:
            subjects = None
        root = Path(ctx.root)
        for output in outputs:
            path = root / output
            if keys is not None:
                found = ctx._relative(path)
                if found is not None and found[0] == Path(ctx.layout.raw):
                    key = ctx.selection.key_of(found[1])
                    if key is not None and key not in keys:
                        return (f"{output} is a file of key {key!r}, which "
                                f"the shard does not declare")
                for categories in ctx._categories(path):
                    if not categories & set(keys):
                        return (f"{output} is in the folder of "
                                f"{', '.join(sorted(categories))}, which "
                                f"the shard does not declare")
            if subjects is not None:
                label = _subject(output)
                if label is not None and label not in subjects:
                    what = "seen" if subs == SEEN else "declared"
                    return (f"{output} is a file of sub-{label}, which the "
                            f"shard has not {what}")
        return None


def _subject(output: str) -> str | None:
    """Subject of the first `sub-<label>/` folder of a path, if any."""
    for part in PurePosixPath(output).parts[:-1]:
        if part.startswith("sub-"):
            return part[4:]
    return None
