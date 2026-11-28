"""`validate_bids.py` (the BIDS-validator check), with a fake validator."""
import json
import shlex
import sys

import pytest
import validate_bids as vb

#: A fake validator: `--version`, else a report of the issues in
#: $FAKE_ISSUES (JSON), on every dataset whose folder ends with
#: $FAKE_TARGET (default: all)
FAKE = """
import json, os, sys
args = sys.argv[1:]
if "--version" in args:
    print("\\x1b[1mbids-validator\\x1b[22m 9.9.9")
    sys.exit(0)
folder = next(a for a in args if os.path.isdir(a))
folder = folder.replace("\\\\", "/").rstrip("/")
issues = []
if folder.endswith(os.environ.get("FAKE_TARGET", "")):
    issues = json.loads(os.environ.get("FAKE_ISSUES", "[]"))
print(json.dumps({"issues": {"issues": issues},
                  "summary": {"schemaVersion": "0.0.1"}}))
"""


@pytest.fixture
def fake(tmp_path):
    script = tmp_path / "fake_validator.py"
    script.write_text(FAKE)
    schema = tmp_path / "schema.json"
    schema.write_text('{"bids_version": "1.2.3", "schema_version": "0.0.1"}')
    return [sys.executable, str(script)], schema


def issue(code, location, severity="error"):
    return {"code": code, "severity": severity, "location": location}


def test_run_twice_in_the_same_folder(tmp_path, fake):
    """A second run with the same `--work` starts from fresh folders"""
    validator, _ = fake
    work = tmp_path / "work"
    found, versions = vb.run(["cobre"], ["study"], validator, work)
    assert found == [] and versions == {"schema": "0.0.1"}
    stale = work / "cobre" / "study" / "stale.txt"
    stale.write_text("x")
    found, _ = vb.run(["cobre"], ["study", "flat"], validator, work)
    assert found == []
    assert not stale.exists()
    assert (work / "cobre" / "flat" / "COBRE" / "participants.tsv").exists()


def test_targets(tmp_path, fake, monkeypatch):
    """Raw, study and derivative datasets are validated"""
    validator, _ = fake
    monkeypatch.setenv("FAKE_ISSUES", json.dumps(
        [issue("X", "/dataset_description.json", "warning")]))
    found, _ = vb.run(["bullitt"], ["study"], validator, tmp_path)
    assert {f["target"] for f in found} == {
        "raw", "study", "derivatives/bullitt"}


@pytest.mark.parametrize("known", [False, True])
def test_new_errors_fail(tmp_path, fake, monkeypatch, capsys, known):
    validator, schema = fake
    found = issue("SIDECAR_KEY_REQUIRED", "/sub-0040000/func/x.nii.gz")
    monkeypatch.setenv("FAKE_ISSUES", json.dumps([found]))
    monkeypatch.setenv("FAKE_TARGET", "COBRE")      # (the flat raw only)
    entry = {"dataset": "cobre", "layout": "flat", "target": "raw",
             "code": found["code"], "subCode": "",
             "location": found["location"]}
    known_file = tmp_path / "known.json"
    known_file.write_text(json.dumps(
        {"findings": [entry] if known else []}))
    monkeypatch.setattr(vb, "KNOWN", known_file)
    summary = tmp_path / "summary.md"
    status = vb.main(["--datasets", "cobre", "--layouts", "flat",
                      "--validator", shlex.join(validator),
                      "--schema", str(schema), "--work",
                      str(tmp_path / "work"), "--summary", str(summary)])
    assert status == (0 if known else 1)
    text = summary.read_text()
    assert "bids-validator 9.9.9, schema 0.0.1 (BIDS 1.2.3)" in text
    assert f"new errors (fatal): {0 if known else 1}" in text


def test_ignore_rules_are_scoped():
    rules = [{"code": "REPETITION_TIME_MISMATCH", "datasets": ["abide2"],
              "location": "*_bold.nii.gz"}]
    bold = "/sub-1/ses-1/func/sub-1_ses-1_task-rest_bold.nii.gz"

    def finding(dataset, location=bold):
        return {"dataset": dataset, "severity": "error",
                "code": "REPETITION_TIME_MISMATCH", "location": location}
    found = [finding("abide2"), finding("cobre"),
             finding("abide2", "/sub-1/anat/sub-1_T1w.nii.gz")]
    assert vb.ignore(found, rules) == [1]
    assert [f["severity"] for f in found] == ["ignore", "error", "error"]


def test_schema_cache_named_by_url(tmp_path, monkeypatch):
    """Two URLs never share a cached schema"""
    served = {}

    class Response:
        def __init__(self, url):
            self.data = served[url]

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def read(self):
            return self.data

    monkeypatch.setattr(vb.urllib.request, "urlopen",
                        lambda request, timeout: Response(request.full_url))
    a, b = "https://a.org/v/schema.json", "https://b.org/v/schema.json"
    served.update({a: b'{"bids_version": "1"}', b: b'{"bids_version": "2"}'})
    assert vb.local_schema(a, tmp_path) != vb.local_schema(b, tmp_path)
    assert json.loads(vb.local_schema(b, tmp_path).read_text()) == {
        "bids_version": "2"}


def test_validator_timeout(monkeypatch):
    monkeypatch.setattr(vb, "TIMEOUT", 0.5)
    with pytest.raises(RuntimeError, match="timed out"):
        vb.call([sys.executable, "-c", "import time; time.sleep(5)"])
