"""Scope of roast shards: `roast.scope`, `RoastPolicy.scope/covers/
expects/audit`, `RoastContext.keeps_subject`."""
import pytest

from brainspresso.bids.keys import KeyAliases
from brainspresso.bids.layout import make_layout
from brainspresso.bids.select import Selection
from brainspresso.roast import SEEN, RoastPolicy, Shard, make_context, scope
from brainspresso.roast.context import RoastContext

KEYS = KeyAliases(
    {"meta": {}, "raw": {"T1w": {}, "bold": {}}, "fs": {}},
    bids={"T1w": {"datatypes": ["anat"], "suffixes": ["T1w"]},
          "bold": {"datatypes": ["func"], "suffixes": ["bold"]}},
    folders={"fs": ["derivatives/fs"]},
)
POLICY = RoastPolicy()


@pytest.fixture
def root(tmp_path):
    make_layout("study", tmp_path / "DS")
    return tmp_path / "DS"


def ctx_of(root, keys=(), exclude_keys=(), **filters):
    return make_context(root, KEYS, select_keys=keys,
                        exclude_keys=exclude_keys, **filters)


def scope_of(root, declared, **options):
    return POLICY.scope(ctx_of(root, **options), Shard(lambda c: (),
                                                       scope=declared))


def covers(root, declared, old, new, seen=None):
    return POLICY.covers(scope_of(root, declared, **old),
                         scope_of(root, declared, **new), seen or {})


IMAGES = scope(keys=("T1w", "bold"), subs=SEEN)


# ----------------------------------------------------------------------
#   Declarations
# ----------------------------------------------------------------------

def test_scope_declaration():
    assert scope() == {"keys": None, "subs": None}
    assert scope(keys="T1w", subs=SEEN) == {"keys": ["T1w"], "subs": "seen"}
    assert scope(keys=("b", "a", "a"), subs=["sub-02", "01"]) == \
        {"keys": ["a", "b"], "subs": ["01", "02"]}
    with pytest.raises(ValueError):
        scope(subs="01")


def test_no_scope_without_a_declaration_or_a_key_selection(root):
    ctx = ctx_of(root)
    assert POLICY.scope(ctx, Shard(lambda c: ())) is None
    assert POLICY.scope(ctx, lambda c: ()) is None
    hosted = RoastContext(layout=ctx.layout, selection=Selection())
    assert POLICY.scope(hosted, Shard(lambda c: (), scope=IMAGES)) is None


def test_scope_describes_the_roast(root):
    got = scope_of(root, IMAGES, exclude_keys=["meta"], subs=["01"],
                   sessions=["1"])
    assert got["fixed"]["json"] == "yes"
    assert got["fixed"]["declared"] == {"keys": ["T1w", "bold"],
                                        "subs": "seen"}
    assert got["fixed"]["base"]["sessions"] == ["1"]
    assert "subs" not in got["fixed"]["base"]
    assert "exclude_subs" not in got["fixed"]["base"]
    assert got["keys"] == ["T1w", "bold"] and got["mode"] == "all"
    assert got["subs"] == {"subs": ["01"], "exclude": []}
    got = scope_of(root, scope(keys=("T1w",), subs=["01", "02"]),
                   keys=["T1w"], exclude_subs=["02"])
    assert got["keys"] == ["T1w"] and got["mode"] == "keys"
    assert got["subs"] == {"kept": ["01"]}
    assert scope_of(root, IMAGES, exclude_keys=["bold"])["mode"] == "exclude"
    assert scope_of(root, scope(keys=["fs"]), keys=["T1w"])["mode"] is None
    assert scope_of(root, scope(), keys=["fs"])["keys"] == ["fs"]


# ----------------------------------------------------------------------
#   Coverage
# ----------------------------------------------------------------------

def test_covers_keys(root):
    full, t1w = {}, {"keys": ["T1w"]}
    assert covers(root, IMAGES, full, t1w)
    assert not covers(root, IMAGES, t1w, full)
    assert covers(root, IMAGES, full, {"exclude_keys": ["meta"]})
    assert covers(root, IMAGES, {"exclude_keys": ["meta"]}, full)
    assert covers(root, IMAGES, {"keys": ["meta", "T1w"]}, t1w)
    assert covers(root, IMAGES, t1w, {"keys": ["meta", "T1w"]})
    # every key (no declaration)
    assert covers(root, scope(), full, t1w)
    assert not covers(root, scope(), {"exclude_keys": ["meta"]}, full)


def test_covers_modes(root):
    """all > exclude > keys, for the same keys"""
    every = {"keys": ["T1w", "bold"]}
    exclude = {"exclude_keys": ["bold"]}
    keys = {"keys": ["T1w"]}
    assert covers(root, IMAGES, every, exclude)
    assert covers(root, IMAGES, exclude, keys)
    assert covers(root, IMAGES, every, keys)
    assert not covers(root, IMAGES, keys, exclude)
    assert not covers(root, IMAGES, exclude, every)


def test_mode_ignored_for_categories(root):
    """A shard of categories only: rules 2-6 do not apply to it"""
    fs = scope(keys=["fs"])
    assert covers(root, fs, {"keys": ["fs", "T1w"]}, {"exclude_keys": ["T1w"]})
    assert covers(root, fs, {"keys": ["fs"]}, {})
    assert not covers(root, fs, {"keys": ["T1w"]}, {"keys": ["fs"]})


def test_covers_declared_subjects(root):
    one = scope(keys=["T1w"], subs=["01"])
    assert covers(root, one, {}, {"subs": ["02"]})
    assert covers(root, one, {"subs": ["01"]}, {"exclude_subs": ["03"]})
    assert not covers(root, one, {"subs": ["02"]}, {"subs": ["01"]})
    assert not covers(root, one, {"exclude_subs": ["01"]}, {})


@pytest.mark.parametrize("old, new, expected", [
    # old: all subjects
    ({}, {"subs": ["01"]}, True),
    ({}, {"exclude_subs": ["01"]}, True),
    ({}, {"subs": ["01"], "exclude_subs": ["01"]}, True),
    # old: some excluded
    ({"exclude_subs": ["01"]}, {}, False),
    ({"exclude_subs": ["01"]}, {"exclude_subs": ["01", "02"]}, True),
    ({"exclude_subs": ["01"]}, {"subs": ["02"]}, True),
    ({"exclude_subs": ["01"]}, {"subs": ["01", "02"]}, False),
    ({"exclude_subs": ["01"]}, {"subs": ["01"], "exclude_subs": ["01"]},
     True),
    # old: a list
    ({"subs": ["01", "02"]}, {}, False),
    ({"subs": ["01", "02"]}, {"exclude_subs": ["03"]}, False),
    ({"subs": ["01", "02"]}, {"subs": ["02"]}, True),
    ({"subs": ["01"]}, {"subs": ["01", "02"]}, False),
    ({"subs": ["01", "02"]}, {"subs": ["02", "03"],
                              "exclude_subs": ["03"]}, True),
    # old: a list and exclusions
    ({"subs": ["01", "02"], "exclude_subs": ["02"]}, {"subs": ["02"]},
     False),
    ({"subs": ["01", "02"], "exclude_subs": ["02"]}, {"subs": ["01"]},
     True),
])
def test_covers_subject_filters(root, old, new, expected):
    """Subjects unknown (`subs=None`, or no `seen`): by the filters"""
    for declared in (scope(keys=["T1w"]), IMAGES):
        assert covers(root, declared, old, new) is expected


def test_covers_seen_subjects(root):
    """With `seen`: only the subjects that the shard holds matter"""
    seen = {"sub": ["01", "02"]}
    assert covers(root, IMAGES, {"subs": ["01"]}, {"subs": ["01", "03"]},
                  seen)
    assert not covers(root, IMAGES, {"subs": ["01"]},
                      {"subs": ["01", "02"]}, seen)
    assert covers(root, IMAGES, {"exclude_subs": ["02"]},
                  {"exclude_subs": ["03"], "subs": ["01", "03"]}, seen)
    assert not covers(root, IMAGES, {"exclude_subs": ["02"]}, {}, seen)
    assert covers(root, IMAGES, {"subs": ["01", "02"]}, {}, seen)
    # (an archive without subjects is covered by any filter)
    assert covers(root, IMAGES, {"subs": ["01"]}, {}, {"sub": []})
    # (`seen` counts for SEEN shards only)
    assert not covers(root, scope(keys=["T1w"]), {"subs": ["01"]},
                      {"subs": ["01", "03"]}, seen)


@pytest.mark.parametrize("change", [
    {"json": "no"}, {"json": "only"}, {"sessions": ["1"]},
    {"include": ["*T1w*"]},
])
def test_other_options_must_be_equal(root, change):
    assert not covers(root, IMAGES, {}, change)
    assert not covers(root, IMAGES, change, {})
    assert covers(root, IMAGES, change, change)


def test_other_declarations_never_cover(root):
    old = scope_of(root, IMAGES)
    new = scope_of(root, scope(keys=["T1w"], subs=SEEN))
    assert not POLICY.covers(old, new, {})
    assert not POLICY.covers(None, new, {})


# ----------------------------------------------------------------------
#   Expected outputs, audit, subjects seen
# ----------------------------------------------------------------------

def test_expects(root):
    ctx = ctx_of(root, keys=["T1w"], subs=["01"])
    raw = ctx.layout.raw
    assert POLICY.expects(raw / "sub-01/anat/sub-01_T1w.nii.gz", ctx)
    assert not POLICY.expects(raw / "sub-02/anat/sub-02_T1w.nii.gz", ctx)
    assert not POLICY.expects(raw / "sub-01/func/sub-01_bold.nii.gz", ctx)
    assert not POLICY.expects(ctx.layout.derivative("fs").root / "x", ctx)
    assert POLICY.expects(root / "elsewhere.txt", ctx)


def test_audit(root):
    ctx = ctx_of(root)
    raw = ctx.layout.raw.relative_to(root).as_posix()
    t1w = f"{raw}/sub-01/anat/sub-01_T1w.nii.gz"
    bold = f"{raw}/sub-02/func/sub-02_bold.nii.gz"
    fs = "derivatives/fs/sub-01/anat/x.nii.gz"
    declared = POLICY.scope(ctx, Shard(lambda c: (), scope=IMAGES))
    seen = {"sub": ["01", "02"]}
    assert POLICY.audit(ctx, declared, [t1w, bold], seen) is None
    assert "sub-02" in POLICY.audit(ctx, declared, [t1w, bold],
                                    {"sub": ["01"]})
    assert "folder of fs" in POLICY.audit(ctx, declared, [fs], seen)
    declared = POLICY.scope(ctx, Shard(lambda c: (), scope=scope(
        keys=["T1w"], subs=["01"])))
    assert "key 'bold'" in POLICY.audit(ctx, declared, [bold], {})
    assert "sub-02" in POLICY.audit(ctx, scope_of(root, scope(
        subs=["01"])), [bold], {})
    # (no declared keys or subjects: nothing to check)
    assert POLICY.audit(ctx, scope_of(root, scope()), [t1w, bold, fs],
                        {}) is None


def test_keeps_subject_reports_the_subject(root):
    ctx = ctx_of(root, subs=["01"])
    reported = []

    class Reporter:
        def seen(self, entity, value):
            reported.append((entity, value))

    ctx._reporter = Reporter()
    assert ctx.keeps_subject("sub-01") and not ctx.keeps_subject("02")
    assert reported == [("sub", "01"), ("sub", "02")]
