"""
Run the BIDS validator on the roasted test fixtures, in both layouts.

Each fixture of the roast tests (the synthetic sources that their
`make_source` helpers write, or their fake servers) is harvested and
roasted into a temporary root with `--layout study` and `--layout
flat`; the validator then checks, for each roasted dataset:

* the raw dataset (`rawbids/` of a study, the root of a flat dataset);
* each derivative dataset (`derivatives/<name>/`);
* the study dataset itself (its root, `DatasetType: "study"`).

Issues are compared with the known findings (`bids_validator_known.json`,
next to this script): a known error is reported, an error that is not
known fails the check, a known finding that is no longer reported is
listed (to be removed from the file). Warnings are reported, never
fatal. Codes that only the synthetic images raise are ignored by the
validator configuration (`bids_validator_config.json`).

Usage::

    python tests/validate_bids.py [--datasets NAME ...] [--layouts ...]
        [--validator CMD] [--summary FILE] [--write-known]

Not a pytest module (no `test_` prefix): it needs the validator, which
the main test matrix does not install. CI runs it in the
`bids-validator.yaml` workflow.
"""
import argparse
import contextlib
import json
import os
import shlex
import subprocess
import sys
import tempfile
import urllib.request
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parent / "scripts")]

# (the test modules, once their folder is on the path)
import test_girder  # noqa: E402
import test_hosted  # noqa: E402
import test_roast_abide1_nitrc  # noqa: E402
import test_roast_abide1_xnat  # noqa: E402
import test_roast_abide2  # noqa: E402
import test_roast_bullitt  # noqa: E402
import test_roast_cobre  # noqa: E402
import test_roast_oasis1  # noqa: E402
import test_roast_oasis2  # noqa: E402
import test_roast_oasis3  # noqa: E402
from cli_utils import bx  # noqa: E402
from roast_utils import LAYOUTS, nii_bytes  # noqa: E402
from test_ixi import write_tarballs  # noqa: E402

from brainspresso.bids.layout import Layout  # noqa: E402
from brainspresso.datasets.unc_wisconsin import harvest as unc  # noqa: E402
from brainspresso.sources import fcp  # noqa: E402
from brainspresso.sources.s3 import S3Bucket  # noqa: E402

#: Validator version (JSR `@bids/validator`), pinned here only (the cache
#: key of the workflow hashes this file)
VALIDATOR_VERSION = "3.0.2"
#: Command that runs the validator
VALIDATOR = f"deno run -ERWN jsr:@bids/validator@{VALIDATOR_VERSION}"
#: BIDS schema (JSR `@bids/schema`): schema 2.0.1 is BIDS 1.11.2, the
#: first with `rawbids/`; the validator bundles BIDS 1.11.1
SCHEMA_VERSION = "2.0.1"
SCHEMA = f"https://jsr.io/@bids/schema/{SCHEMA_VERSION}/schema.json"
BIDS_VERSION = "1.11.2"
CONFIG = HERE / "bids_validator_config.json"
KNOWN = HERE / "bids_validator_known.json"


# ----------------------------------------------------------------------
#   Fixtures: (parent folder, layout) -> dataset folder, roasted
# ----------------------------------------------------------------------

def roast_source(name: str, folder: str, make_source, *args):
    """A fixture whose sources `make_source(root)` writes"""
    def build(parent: Path, layout: str) -> Path:
        root = make_source(parent / folder)
        bx(name, "roast", "--out", root, "--layout", layout, *args)
        return root
    return build


def ixi(parent, layout):
    """IXI from its original tarballs (and the bundled IXI.xls)"""
    root = parent / "IXI"
    write_tarballs(root / "sourcedata")
    bx("ixi", "roast", "--out", root, "--layout", layout,
       "--keys", "meta", "T1w", "T2w", "dwi")
    return root


def oasis1(parent, layout):
    """OASIS-I: an image disc and a FreeSurfer disc"""
    root = test_roast_oasis1.make_source(parent / "OASIS-1")
    test_roast_oasis1.freesurfer_disc(root / "sourcedata")
    bx("oasis1", "roast", "--out", root, "--layout", layout)
    return root


def unc_wisconsin(parent, layout):
    """UNC-Wisconsin, harvested from the fake Girder server"""
    with test_girder.serve(test_girder.TREE) as fake, \
            patched(unc, KITWARE=fake.api, SCAN_DATA=fake.root):
        bx("unc-wisconsin", "harvest", parent, "--layout", layout,
           "--keys", "all")
    root = parent / "UNC-Wisconsin"
    bx("unc-wisconsin", "roast", "--out", root)
    return root


def sald(parent, layout):
    """
    SALD (INDI, hosted BIDS), harvested from the fake `fcp-indi` bucket.

    The bucket of `test_hosted` with NIfTI images instead of filler
    bytes (the hosted images are copied as they are); its shipped
    fill-in templates.
    """
    t = test_hosted
    objects = {k: nii_bytes(shape=(2, 2, 2, 3) if "_bold" in k
                            else (2, 2, 2))
               if k.endswith(".nii.gz") else v
               for k, v in t.INDI.items() if k.startswith(t.SALD)}
    url, stop = t.serve(t.bucket_app("fcp-indi", objects))
    try:
        with patched(fcp, get_bucket=lambda: S3Bucket("fcp-indi",
                                                      endpoint=url)):
            bx("sald", "harvest", parent, "--layout", layout)
    finally:
        stop()
    return parent / "SALD"


@contextlib.contextmanager
def patched(module, **values):
    """Set module attributes for the duration of a block"""
    old = {k: getattr(module, k) for k in values}
    for k, v in values.items():
        setattr(module, k, v)
    try:
        yield
    finally:
        for k, v in old.items():
            setattr(module, k, v)


#: Fixtures: name -> build(parent, layout) -> roasted dataset folder
FIXTURES = {
    "ixi": ixi,
    "oasis1": oasis1,
    "oasis2": roast_source("oasis2", "OASIS-2",
                           test_roast_oasis2.make_source),
    "oasis3": roast_source("oasis3", "OASIS-3",
                           test_roast_oasis3.make_source),
    "cobre": roast_source("cobre", "COBRE",
                          test_roast_cobre.make_source),
    "abide1-nitrc": roast_source("abide1", "ABIDE-1",
                                 test_roast_abide1_nitrc.make_source),
    "abide1-xnat": roast_source("abide1", "ABIDE-1",
                                test_roast_abide1_xnat.make_source,
                                "--source", "xnat"),
    "abide2": roast_source("abide2", "ABIDE-2",
                           test_roast_abide2.make_source),
    "bullitt": roast_source("bullitt", "Bullitt",
                            test_roast_bullitt.make_source),
    "unc-wisconsin": unc_wisconsin,
    "sald": sald,
}


# ----------------------------------------------------------------------
#   Validation
# ----------------------------------------------------------------------

def targets(root: Path) -> dict[str, Path]:
    """BIDS datasets of a roasted dataset: name -> folder"""
    layout = Layout.detect(root)
    assert layout is not None, f"no layout detected in {root}"
    out = {"raw": layout.raw}
    if layout.kind == "study":
        out["study"] = layout.root
    for drv in layout.list_derivatives():
        out[f"derivatives/{drv.root.name}"] = drv.root
    return out


def local_schema(schema: str, cache: Path) -> str:
    """A `file://` URL of the schema (downloaded once into `cache`)"""
    if not schema.startswith(("http://", "https://")):
        return Path(schema).resolve().as_uri()
    path = cache / ("bids-schema-" + schema.rstrip("/").split("/")[-2]
                    + ".json")
    if not path.exists():
        cache.mkdir(parents=True, exist_ok=True)
        # (a generic user agent: jsr.io refuses urllib's own)
        request = urllib.request.Request(
            schema, headers={"User-Agent": "brainspresso-ci"})
        with urllib.request.urlopen(request, timeout=60) as response:
            data = response.read()
        json.loads(data)
        path.write_bytes(data)
    return path.as_uri()


def validate(validator: list[str], folder: Path, study: bool) -> dict:
    """Run the validator on a folder; return its JSON report"""
    args = [*validator, str(folder), "--format", "json",
            "--config", str(CONFIG), "--no-color"]
    if study:
        args += ["--datasetTypes", "study"]
    out = subprocess.run(args, capture_output=True, text=True)
    try:
        return json.loads(out.stdout)
    except json.JSONDecodeError:
        raise RuntimeError(
            f"validator failed on {folder} ({out.returncode}):\n"
            f"{out.stdout[-2000:]}\n{out.stderr[-2000:]}") from None


def findings(report: dict, **where) -> list[dict]:
    """Issues of a report, one dict each (without the long messages)"""
    out = []
    for issue in report["issues"]["issues"]:
        message = (issue.get("issueMessage") or "").strip()
        out.append({**where, "severity": issue["severity"],
                    "code": issue["code"],
                    "subCode": issue.get("subCode") or "",
                    "location": issue.get("location") or "",
                    "message": message.splitlines()[0][:200]
                    if message else ""})
    return out


KEY = ("dataset", "layout", "target", "code", "subCode", "location")


def key_of(finding: dict) -> tuple:
    """What identifies a finding in the known list"""
    return tuple(finding.get(k, "") for k in KEY)


def run(names, layouts, validator, work: Path) -> tuple[list, dict]:
    """Roast and validate; return the findings and the versions"""
    fixtures = FIXTURES
    result, versions = [], {}
    for name in names:
        for layout in layouts:
            parent = work / name / layout
            parent.mkdir(parents=True)
            print(f"== {name} ({layout})", flush=True)
            root = fixtures[name](parent, layout)
            for target, folder in targets(root).items():
                report = validate(validator, folder, target == "study")
                summary = report.get("summary", {})
                versions.setdefault("schema", summary.get("schemaVersion"))
                got = findings(report, dataset=name, layout=layout,
                               target=target)
                result += got
                count = Counter(f["severity"] for f in got)
                print(f"   {target}: {count['error']} errors, "
                      f"{count['warning']} warnings, "
                      f"{count['ignore']} ignored", flush=True)
    return result, versions


def compare(found: list[dict], known: list[dict], names, layouts):
    """(new errors, known errors, known findings no longer reported)"""
    known = [k for k in known
             if k["dataset"] in names and k["layout"] in layouts]
    known_keys = {key_of(k) for k in known}
    errors = [f for f in found if f["severity"] == "error"]
    found_keys = {key_of(f) for f in errors}
    new = [f for f in errors if key_of(f) not in known_keys]
    old = [f for f in errors if key_of(f) in known_keys]
    gone = [k for k in known if key_of(k) not in found_keys]
    return new, old, gone


def table(rows: list[dict]) -> list[str]:
    """Markdown table of findings"""
    out = ["| dataset | layout | target | code | location |",
           "|---|---|---|---|---|"]
    for f in rows:
        code = f["code"] + (f":{f['subCode']}" if f["subCode"] else "")
        out.append(f"| {f['dataset']} | {f['layout']} | {f['target']} | "
                   f"`{code}` | `{f['location']}` |")
    return out


def summary(found, new, old, gone, versions) -> str:
    """Markdown summary of the check"""
    warnings = [f for f in found if f["severity"] == "warning"]
    ignored = Counter(f["code"] for f in found if f["severity"] == "ignore")
    lines = [
        "## BIDS validator",
        "",
        f"bids-validator {VALIDATOR_VERSION}, schema "
        f"{versions.get('schema')} (BIDS {BIDS_VERSION}).",
        "",
        f"* new errors (fatal): {len(new)}",
        f"* known errors (`tests/bids_validator_known.json`): {len(old)}",
        f"* known findings no longer reported: {len(gone)}",
        f"* warnings: {len(warnings)}",
        "* ignored (`tests/bids_validator_config.json`): "
        + (", ".join(f"`{c}` ({n})" for c, n in sorted(ignored.items()))
           or "none"),
    ]
    for title, rows in (("New errors", new), ("Known errors", old),
                        ("No longer reported", gone)):
        if rows:
            lines += ["", f"### {title}", "", *table(rows)]
    if warnings:
        counts = Counter((f["dataset"], f["layout"], f["target"], f["code"])
                         for f in warnings)
        lines += ["", "### Warnings (by code)", "",
                  "| dataset | layout | target | code | count |",
                  "|---|---|---|---|---|"]
        lines += [f"| {d} | {lay} | {t} | `{c}` | {n} |"
                  for (d, lay, t, c), n in sorted(counts.items())]
    return "\n".join(lines) + "\n"


def main(argv=None):
    """Command-line entry point"""
    fixtures = list(FIXTURES)
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--datasets", nargs="+", choices=fixtures,
                        default=fixtures)
    parser.add_argument("--layouts", nargs="+", choices=LAYOUTS,
                        default=list(LAYOUTS))
    parser.add_argument("--validator", default=os.environ.get(
        "BIDS_VALIDATOR", VALIDATOR), help="command (default: %(default)s)")
    parser.add_argument("--schema", default=SCHEMA,
                        help="BIDS schema, URL or file (default: "
                        "%(default)s)")
    parser.add_argument("--schema-cache", type=Path, default=os.environ.get(
        "BIDS_SCHEMA_CACHE"), help="folder of the downloaded schema")
    parser.add_argument("--work", type=Path,
                        help="keep the roasted datasets in this folder")
    parser.add_argument("--summary", type=Path,
                        help="append a Markdown summary to this file")
    parser.add_argument("--json", type=Path, help="write every finding")
    parser.add_argument("--write-known", action="store_true",
                        help="record the current errors as known")
    args = parser.parse_args(argv)
    with tempfile.TemporaryDirectory() as tmp:
        schema = local_schema(args.schema, args.schema_cache or Path(tmp))
        validator = [*shlex.split(args.validator), "--schema", schema]
        work = args.work or Path(tmp) / "work"
        found, versions = run(args.datasets, args.layouts, validator, work)

    known = json.loads(KNOWN.read_text())["findings"] \
        if KNOWN.exists() else []
    if args.write_known:
        keep = [k for k in known if k["dataset"] not in args.datasets
                or k["layout"] not in args.layouts]
        errors = [{k: f[k] for k in (*KEY, "message")}
                  for f in found if f["severity"] == "error"]
        known = sorted(keep + errors, key=key_of)
        KNOWN.write_text(json.dumps({
            "validator": VALIDATOR_VERSION,
            "schema": SCHEMA_VERSION,
            "findings": known,
        }, indent=2) + "\n")
    new, old, gone = compare(found, known, args.datasets, args.layouts)
    text = summary(found, new, old, gone, versions)
    print(text)
    if args.summary:
        with open(args.summary, "a") as f:
            f.write(text)
    if args.json:
        args.json.write_text(json.dumps(found, indent=2) + "\n")
    return 1 if new else 0


if __name__ == "__main__":
    sys.exit(main())
