"""
Roaster outputs against the golden baseline (`scripts/golden.py`).

A reduced matrix (`golden.CI_MATRIX`) of the golden harness: each
roasted fixture must match `tests/golden/<dataset>.json`. When a change
of output is intended, regenerate the baseline with
`python scripts/golden.py record` and paste `python scripts/golden.py
diff` in the pull request.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
import golden  # noqa: E402


@pytest.mark.parametrize("name", list(golden.CI_MATRIX))
def test_golden(name, tmp_path):
    lines = golden.check({name: golden.CI_MATRIX[name]}, tmp_path)
    assert not lines, (
        "roasted outputs differ from the golden baseline "
        f"(regenerate with `{golden.RECORD}` if intended):\n"
        + "\n".join(lines))
