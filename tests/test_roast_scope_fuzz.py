"""
Differential test of shard scopes: incremental roasts (`--if-exists
skip`, shards skipped by their scope) against a fresh roast with the same
selection.

Each step draws a selection (subjects, keys, exclusions, sidecar
policy) and sometimes deletes a per-subject output first, as a compost
would; then every per-subject file of the fresh roast must be in the
incremental dataset, with the same content for text files. A reduced
version of the fuzz of the review of #38 (fixed seeds, a few steps).
"""
import json
import random
import shutil

import pytest
import test_roast_abide1_nitrc
import test_roast_abide1_xnat
import test_roast_abide2
import test_roast_bullitt
import test_roast_oasis3
from roast_utils import init_layout, make_tar, nii_bytes
from test_roast_cobre import CSV

from brainspresso.actions import engine
from brainspresso.datasets.bullitt.roaster import roast as roast_bullitt
from brainspresso.datasets.indi.abide.i.roaster import roast as roast_abide1
from brainspresso.datasets.indi.abide.ii.roaster import roast as roast_abide2
from brainspresso.datasets.indi.cobre.roaster import roast as roast_cobre
from brainspresso.datasets.oasis.i.roaster import roast as roast_oasis1
from brainspresso.datasets.oasis.ii.roaster import roast as roast_oasis2
from brainspresso.datasets.oasis.iii.roaster import roast as roast_oasis3

#: (seed, layout) of each run (seeds whose runs skip shards by their
#: scope, checked by `skipped`), and the number of steps of a run
RUNS = [(4, "flat"), (5, "study")]
STEPS = 10


def tree(root):
    """Files of a dataset, but its source data and state."""
    out = {}
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root).as_posix()
        parts = rel.split("/")
        if path.is_file() and parts[0] != "sourcedata" \
                and ".brainspresso" not in parts:
            out[rel] = path
    return out


def per_subject(rel):
    return any(part.startswith("sub-") for part in rel.split("/")[:-1])


def text(path):
    return path.suffix in (".json", ".bval", ".bvec", ".tre") \
        and not path.name.endswith(("_sessions.tsv", "_scans.tsv"))


def pick(rng, universe, p_none):
    if rng.random() < p_none:
        return None
    return sorted(rng.sample(sorted(universe),
                             rng.randint(1, len(universe))))


def attempt(roast, root, selection):
    """Roast; the type of the exception it raises, if any."""
    try:
        return roast(root, display=False, **selection), None
    except Exception as e:      # (nothing selected...: same on both)
        return None, type(e)


def fuzz(tmp_path, name, build, roast, subs, keys, seed, layout,
         extra=None):
    rng = random.Random(seed)
    template = tmp_path / "template" / name
    init_layout(template, layout)
    build(template)
    incr = tmp_path / "incr" / name
    shutil.copytree(template, incr, copy_function=shutil.copy2)
    history = []
    for step in range(STEPS):
        selection = {}
        for option, universe, p_none in (
                ("sub", subs, 0.4), ("exclude_sub", subs, 0.7),
                ("keys", keys, 0.4), ("exclude_keys", keys, 0.7)):
            value = pick(rng, universe, p_none)
            if value is not None:
                selection[option] = value
        selection["json"] = rng.choice(("yes", "yes", "yes", "no", "only"))
        if extra:
            selection.update(extra(rng))
        deleted = None
        if rng.random() < 0.3:
            files = [r for r in tree(incr) if per_subject(r)]
            if files:
                deleted = rng.choice(files)
                (incr / deleted).unlink()
        history.append((selection, deleted))
        fresh = tmp_path / f"fresh{step}" / name
        shutil.copytree(template, fresh, copy_function=shutil.copy2)
        (one, raised), (two, also) = (attempt(roast, incr, selection),
                                      attempt(roast, fresh, selection))
        assert raised is also, history
        if raised is None:
            want, have = tree(fresh), tree(incr)
            for rel, path in want.items():
                if not per_subject(rel):
                    continue
                assert rel in have, (
                    f"missing {rel} after {history}\n"
                    f"errors: {one.errors} (fresh: {two.errors})")
                if text(path):
                    assert have[rel].read_bytes() == path.read_bytes(), \
                        f"{rel} differs after {history}"
        shutil.rmtree(fresh.parent)


@pytest.fixture(autouse=True)
def skipped(monkeypatch):
    """Count the shards skipped by their scope (the fuzz must skip)."""
    counts = {"skipped": 0}
    real = engine.Engine.plan

    def plan(self, shard, skip=True):
        out = real(self, shard, skip)
        if out.skip is not None and out.scope is not None:
            counts["skipped"] += 1
        return out

    monkeypatch.setattr(engine.Engine, "plan", plan)
    yield counts


def run_fuzz(skipped, *args, **kwargs):
    fuzz(*args, **kwargs)
    assert skipped["skipped"], "no shard skipped by its scope"


@pytest.mark.parametrize("seed, layout", RUNS)
def test_cobre(tmp_path, skipped, seed, layout):

    def build(root):
        src = root / "sourcedata"
        src.mkdir(parents=True, exist_ok=True)
        (src / "COBRE_phenotypic_data.csv").write_text(CSV)
        make_tar(src / "COBRE_scan_data.tgz", {
            "COBRE/0040000/session_1/anat_1/mprage.nii.gz": nii_bytes(),
            "COBRE/0040000/session_1/rest_1/rest.nii.gz":
                nii_bytes((2, 2, 2, 3), tr=2.0),
            "COBRE/0040001/session_1/anat_1/mprage.nii.gz": nii_bytes(),
            "COBRE/0040002/session_1/rest_1/rest.nii.gz":
                nii_bytes((2, 2, 2, 3), tr=2.0),
        })

    run_fuzz(skipped, tmp_path, "COBRE", build, roast_cobre,
             [40000, 40001, 40002], ["meta", "T1w", "func"], seed, layout)


@pytest.mark.parametrize("seed, layout", RUNS)
def test_oasis1(tmp_path, skipped, seed, layout):
    openpyxl = pytest.importorskip("openpyxl")
    t = pytest.importorskip("test_roast_oasis1")

    def build(root):
        src = root / "sourcedata"
        src.mkdir(parents=True, exist_ok=True)
        t.image_disc(src)
        t.image_disc(src, disc=2, ids=(3,))
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["ID", "M/F", "Hand", "Age", "Educ", "SES", "MMSE", "CDR",
                   "eTIV", "nWBV", "ASF", "Delay"])
        for i in (1, 2, 3):
            ws.append([f"OAS1_{i:04d}_MR1", "F", "R", 74, 2, 3, 29, 0, 1344,
                       0.743, 1.306, "N/A"])
        wb.save(src / t.roaster.XLSX)
        s = "disc1/OAS1_0001_MR1"
        make_tar(src / "oasis_cs_freesurfer_disc1.tar.gz", {
            f"{s}/mri/aseg.mgz": t.mgz_bytes(),
            f"{s}/mri/orig/001.mgz": t.mgz_bytes(),
            f"{s}/stats/aseg.stats": b"# not bidsifiable",
            "disc1/OAS1_0002_MR1/mri/aseg.mgz": t.mgz_bytes(),
            "disc1/OAS1_0003_MR1/stats/aseg.stats": b"# not bidsifiable",
        })

    run_fuzz(skipped, tmp_path, "OASIS-1", build, roast_oasis1, [1, 2, 3],
             ["raw", "fs", "fs-all", "fsl", "avg", "tal", "meta"], seed,
             layout)


@pytest.mark.parametrize("seed, layout", RUNS)
def test_oasis2(tmp_path, skipped, seed, layout):
    t = pytest.importorskip("test_roast_oasis2")

    def build(root):
        src = root / "sourcedata"
        src.mkdir(parents=True, exist_ok=True)
        t.write_xlsx(src)
        t.part(src)
        t.part(src, number=2, scans=[(3, 1, 1), (3, 2, 1)])

    def extra(rng):
        if rng.random() < 0.4:
            return {"ses": rng.choice([["1"], ["2"]])}
        return {}

    run_fuzz(skipped, tmp_path, "OASIS-2", build, roast_oasis2, [1, 2, 3],
             ["raw", "meta"], seed, layout, extra=extra)


@pytest.mark.parametrize("seed, layout", RUNS)
def test_oasis3(tmp_path, skipped, seed, layout):
    t = test_roast_oasis3

    def build(root):
        src = root / "sourcedata"
        d = src / "OASIS3_data_files"
        make_tar(d / "demo.tar.gz", {"demo/demo.csv": t.DEMO.encode()})
        make_tar(d / "UDSb4.tar.gz", {"b4/cdr.csv": t.CDR.encode()})
        for sub, ses, mods in [(1, "d0129", ["T1w", "T2w", "acq-TSE_T2w"]),
                               (2, "d0000", ["T2w", "FLAIR"])]:
            folder = f"OAS3{sub:04d}_MR_{ses}"
            b = f"{folder}/anat1/NIFTI/sub-OAS3{sub:04d}_sess-{ses}"
            members = {}
            for mod in mods:
                members[f"{b}_{mod}.json"] = json.dumps(t.SIDECAR).encode()
                members[f"{b}_{mod}.nii.gz"] = nii_bytes()
            make_tar(src / folder / "anat1.tar.gz", members)
            b = f"{folder}/func1/NIFTI/sub-OAS3{sub:04d}_sess-{ses}"
            make_tar(src / folder / "func1.tar.gz", {
                f"{b}_task-restingstate_run-01_bold.json": b"{}",
                f"{b}_task-restingstate_run-01_bold.nii.gz":
                    nii_bytes((2, 2, 2, 3)),
                f"{b}_asl.nii.gz": nii_bytes((2, 2, 2, 3)),
                f"{b}_asl.json": b"{}",
                f"{b}_m0scan.nii.gz": nii_bytes(),
            })

    run_fuzz(skipped, tmp_path, "OASIS-3", build, roast_oasis3, [1, 2],
             ["meta", "pheno", "T1w", "T2w", "TSE", "FLAIR", "bold", "asl",
              "pasl"], seed, layout)


@pytest.mark.parametrize("seed, layout", RUNS)
def test_abide1_nitrc(tmp_path, skipped, seed, layout):
    run_fuzz(skipped, tmp_path, "ABIDE-1", test_roast_abide1_nitrc.make_source,
             lambda root, **kw: roast_abide1(root, source="nitrc", **kw),
             [51456, 51457, 51330], ["meta", "pheno", "T1w", "T2w", "rest"],
             seed, layout)


@pytest.mark.parametrize("seed, layout", RUNS)
def test_abide1_xnat(tmp_path, skipped, seed, layout):
    run_fuzz(skipped, tmp_path, "ABIDE-1", test_roast_abide1_xnat.make_source,
             lambda root, **kw: roast_abide1(root, source="xnat", **kw),
             [51456, 51330, 51201, 50642, 50003],
             ["meta", "pheno", "T1w", "T2w", "rest", "fs", "fs-all"],
             seed, layout)


@pytest.mark.parametrize("seed, layout", RUNS)
def test_abide2(tmp_path, skipped, seed, layout):
    def build(root):
        test_roast_abide2.make_source(root)
        b = "ABIDEII-IP_1"
        # interleaved subjects, runs, two sessions
        make_tar(root / "sourcedata" / "ABIDEII-IP_1.tar.gz", {
            f"{b}/29220/session_1/rest_1/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
            f"{b}/29220/session_1/anat_1/anat.nii.gz": nii_bytes(),
            f"{b}/29221/session_1/anat_1/anat.nii.gz": nii_bytes(),
            f"{b}/29220/session_1/rest_2/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
            f"{b}/29221/session_1/anat_2/anat.nii.gz": nii_bytes(),
            f"{b}/29221/session_2/anat_1/anat.nii.gz": nii_bytes(),
        })

    run_fuzz(skipped, tmp_path, "ABIDE-2", build, roast_abide2,
             [29001, 29002, 29100, 29220, 29221],
             ["meta", "pheno", "T1w", "rest", "dti"], seed, layout)


@pytest.mark.parametrize("seed, layout", RUNS)
def test_bullitt(tmp_path, skipped, seed, layout):
    def build(root):
        for name, data in test_roast_bullitt.SOURCE.items():
            path = root / "sourcedata" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)

    run_fuzz(skipped, tmp_path, "Bullitt", build, roast_bullitt, [1, 2, 3],
             ["meta", "flash", "mprage", "T2w", "angio", "skullstripped",
              "vessels", "raw", "derivatives"], seed, layout)
