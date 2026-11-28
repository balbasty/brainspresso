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
    (`Layout.replaced`) before it replaces it; a tracked
    file that holds what roast writes gets its hash recorded (manifest
    origin "converted", `Report.digests`), whatever the mode;
  * other tables (several outputs): compared under `update` and
    `different`; anything else: the mode as given (`update` is
    `different`).

  An action that sets `compare=False` is left alone under `different`
  and `update` when it exists.
* Outputs in `<state>/orig/` (pristine tables) are selected as the
  table they are the pristine copy of (`RoastContext.keeps`).

Scope of a shard
----------------
A shard that declares `scope=roast.scope(keys=..., sub=...)` is signed
by its inputs and `config` only (`actions.engine`); `RoastPolicy.scope`
describes the run as far as it matters to the shard:

```
{"fixed": {"json": ..., "base": <selection minus subjects>,
           "declared": {"keys": [...] | None, "sub": [...] | "seen" | None}},
 "keys": [wanted keys among the declared ones],
 "mode": "all" | "exclude" | "keys" | None,
 "sub": {"kept": [declared subjects kept]}
      | {"sub": [...], "exclude_sub": [...]}}
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

import hashlib
import json
from logging import getLogger
from pathlib import Path, PurePosixPath

from brainspresso.actions.action import Action, IfExists
from brainspresso.actions.engine import Policy, Rejected
from brainspresso.bids.keys import KeySelection, normalize
from brainspresso.bids.kinds import kinds_of
from brainspresso.bids.layout import ORIG_DIR
from brainspresso.roast.context import RoastContext

lg = getLogger(__name__)

__all__ = ["RoastPolicy", "SEEN"]

#: `scope(sub=SEEN)`: the shard reports every subject its inputs hold
#: (`RoastContext.keeps_subject`)
SEEN = "seen"
#: Key modes, from the one that keeps the least
_MODES = {"keys": 0, "exclude": 1, "all": 2}


def _keeps(filters: dict, label: str) -> bool:
    """`Selection.keeps_subject` on recorded subject filters."""
    sub = filters.get("sub") or ()
    return (not sub or label in sub) \
        and label not in (filters.get("exclude_sub") or ())


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
        if state == "unknown":
            # replaced (if it differs) with a backup: `Action.backup`
            action.backup = ctx.layout.replaced(dst)
        elif state == "edited":
            # compared first: a file that holds the render (its hash was
            # not recorded, e.g. a killed roast) is adopted, not kept
            action.keep_edit = (
                f"{dst} was edited since brainspresso wrote it: kept "
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
        keys, sub = declared.get("keys"), declared.get("sub")
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
                if k not in ("sub", "exclude_sub")}
        if isinstance(sub, list):
            kept_sub = {"kept": [s for s in sub
                                 if selection.keeps_subject(s)]}
        else:
            kept_sub = {"sub": sorted(selection.base.sub),
                        "exclude_sub": sorted(selection.base.exclude_sub)}
        # (as it reads back from a record)
        return json.loads(json.dumps({
            "fixed": {"json": ctx.json, "base": base,
                      "declared": {"keys": None if keys is None
                                   else sorted(keys), "sub": sub}},
            "keys": kept,
            "mode": mode,
            "sub": kept_sub,
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
        before, after = old.get("sub") or {}, new["sub"]
        if "kept" in after:
            return set(after["kept"]) <= set(before.get("kept", ()))
        if new["fixed"]["declared"]["sub"] == SEEN and "sub" in seen:
            return all(_keeps(before, s) for s in seen["sub"]
                       if _keeps(after, s))
        # (subjects unknown: by the filters themselves)
        if not before.get("sub"):
            return all(not _keeps(after, s)
                       for s in before.get("exclude_sub") or ())
        return bool(after.get("sub")) and all(
            _keeps(before, s) for s in after["sub"] if _keeps(after, s))

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

        Notes
        -----
        It cannot check every category: one without a folder of its own
        (`KeyAliases(folders=)`) is not seen in the outputs at all, and
        a folder shared by several categories passes if any of them is
        declared (OASIS-I `avg`, `tal`, `fsl` share
        `derivatives/oasis-processed/`: a declaration without `tal`
        passes). Roaster tests check those keys by running with and
        without each one (`roast_utils.check_keys_rerun`).
        """
        declared = scope["fixed"]["declared"]
        keys, sub = declared["keys"], declared["sub"]
        if isinstance(sub, list):
            subjects = set(sub)
        elif sub == SEEN:
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
                    what = "seen" if sub == SEEN else "declared"
                    return (f"{output} is a file of sub-{label}, which the "
                            f"shard has not {what}")
        return None


def _subject(output: str) -> str | None:
    """Subject of the first `sub-<label>/` folder of a path, if any."""
    for part in PurePosixPath(output).parts[:-1]:
        if part.startswith("sub-"):
            return part[4:]
    return None


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
        "unknown": no hash recorded (e.g. written by a brainspresso that
        recorded none, or by the user)
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
