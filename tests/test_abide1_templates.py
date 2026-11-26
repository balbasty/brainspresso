"""ABIDE-I: every site spelling of every source resolves to a template."""
import json
import logging
from pathlib import PurePosixPath

import pytest

from brainspresso.datasets.ABIDE.I import sources
from brainspresso.datasets.ABIDE.I.bidsifier import BidsifierBase
from brainspresso.datasets.ABIDE.I.bidsifier import TemplateError
from brainspresso.datasets.ABIDE.I.bidsifier_nitrc import Bidsifier
from roast_utils import init_layout, make_tar, nii_bytes, raw_file

TPL = BidsifierBase.TPLDIR
NAMES = ("T1w.json", "bold.json")


def _stem(url):
    return PurePosixPath(url).name.split(".")[0]


# NITRC tarballs and S3 `RawData/` folders: `<Site>[_<sample>][_<part>]`
NITRC = sorted({_stem(u) for v in sources.URLS.values() for u in v["raw"]})
# phenotypic site IDs (`phenotypic_<SITE_ID>.csv`, SITE_ID column)
PHENO = sorted({_stem(u)[len("phenotypic_"):]
                for v in sources.URLS.values() for u in v["meta"]})
# XNAT: subject labels `<site>_<id>` (`rsplit('_', 1)` in the roast),
# scan-parameter folders
XNAT = sorted({
    "Caltech_0051456".rsplit("_", 1)[0],      # test_sources fixture
    "MaxMun_a_0051330".rsplit("_", 1)[0],
    "CMU_50642".rsplit("_", 1)[0],
    *sources.XNAT_SITES,
})
SITES = sorted({*BidsifierBase.SITES, *NITRC, *PHENO, *XNAT})


def test_site_spellings():
    # sanity: the source spellings that motivated the mapping are covered
    assert {"CMU_a", "MaxMun_d", "Leuven_2", "UM_1", "NYU_e"} <= set(NITRC)
    assert {"MAX_MUN", "LEUVEN_1", "UCLA_1", "UM_1", "CMU"} <= set(PHENO)
    assert {"Leuven-1", "UCLA1", "UM2", "CMU", "MaxMun"} <= set(XNAT)


@pytest.mark.parametrize("name", NAMES)
@pytest.mark.parametrize("site", SITES)
def test_template_exists(site, name):
    assert BidsifierBase.template(site, name).is_file()


@pytest.mark.parametrize("site", ["UCLA", "UCLA_1", "UCLA2", "UCLA_2"])
def test_template_hires(site):
    assert BidsifierBase.template(site, "acq-hires_T1w.json").is_file()


@pytest.mark.parametrize("site, name, expected", [
    # sample given, in any spelling
    ("MaxMun_b", "bold.json", "MaxMun_b/bold.json"),
    ("Leuven-2", "T1w.json", "Leuven_2/T1w.json"),
    ("LEUVEN_2", "bold.json", "Leuven_2/bold.json"),
    ("UCLA2", "bold.json", "UCLA_2/bold.json"),
    ("UM_2", "T1w.json", "UM_2/T1w.json"),
    ("CMU_b", "T1w.json", "CMU_b/T1w.json"),
    ("CMU_b", "bold.json", "CMU_b/bold.json"),
    # no anatomical printout for CMU_a: site sidecar
    ("CMU_a", "T1w.json", "CMU/T1w.json"),
    # sample unknown: default sample of the site
    ("CMU", "bold.json", "CMU_a/bold.json"),
    ("CMU", "T1w.json", "CMU/T1w.json"),
    ("MaxMun", "T1w.json", "MaxMun_a/T1w.json"),
    ("MaxMun", "bold.json", "MaxMun_a/bold.json"),
    ("MAX_MUN", "bold.json", "MaxMun_a/bold.json"),
    ("Leuven", "bold.json", "Leuven_1/bold.json"),
    ("UCLA", "T1w.json", "UCLA_1/T1w.json"),
    ("UM", "bold.json", "UM_1/bold.json"),
    # sites without samples: suffix (tarball part) ignored
    ("NYU_c", "bold.json", "NYU/bold.json"),
    ("CALTECH", "T1w.json", "Caltech/T1w.json"),
    ("USM", "T1w.json", "USM/T1w.json"),
])
def test_template_mapping(site, name, expected):
    assert BidsifierBase.template(site, name) == TPL / expected


def test_template_errors():
    with pytest.raises(TemplateError, match="unknown site 'Nowhere'"):
        BidsifierBase.template("Nowhere", "T1w.json")
    with pytest.raises(TemplateError, match="unknown sample 'e'.*MaxMun"):
        BidsifierBase.template("MaxMun_e", "T1w.json")
    with pytest.raises(TemplateError, match="'dwi.json' for site 'CMU_a'"):
        BidsifierBase.template("CMU_a", "dwi.json")


def test_samples_match_templates():
    """Every per-sample template folder is declared, and vice versa."""
    folders = {p.name for p in TPL.iterdir() if p.is_dir()
               and p.name != "phenotype"}
    declared = {f"{site}_{s}" for site, samples in
                BidsifierBase.SAMPLES.items() for s in samples}
    assert {f for f in folders if "_" in f} == declared
    assert {f for f in folders if "_" not in f} <= set(BidsifierBase.SITES)
    for site in BidsifierBase.SITES:
        for name in NAMES:
            assert BidsifierBase.template(site, name).is_file()


def test_roast_unsampled_sites(tmp_path, caplog):
    """Bare `CMU` bold and `MaxMun` without sample; unknown site logged."""
    root = tmp_path / "ABIDE-1"
    init_layout(root, "study")
    src = root / "sourcedata"
    src.mkdir(parents=True)
    make_tar(src / "CMU.tgz", {
        "CMU/0050642/session_1/rest_1/rest.nii.gz": nii_bytes((2, 2, 2, 3)),
    })
    make_tar(src / "MaxMun.tgz", {
        "MaxMun/0051330/session_1/anat_1/mprage.nii.gz": nii_bytes(),
    })
    make_tar(src / "Nowhere.tgz", {
        "Nowhere/0099999/session_1/anat_1/mprage.nii.gz": nii_bytes(),
    })
    with caplog.at_level(logging.ERROR):
        Bidsifier(root, keys={"T1w", "rest"}).run()

    def sidecar(path):
        return json.loads(raw_file(root, path).read_text())

    def tpl(path):
        return json.loads((TPL / path).read_text())

    assert sidecar("sub-50642/func/sub-50642_task-rest_bold.json") \
        == tpl("CMU_a/bold.json")
    assert sidecar("sub-51330/anat/sub-51330_T1w.json") \
        == tpl("MaxMun_a/T1w.json")
    # unknown site: image converted, sidecar error logged
    assert raw_file(root, "sub-99999/anat/sub-99999_T1w.nii.gz").exists()
    assert not raw_file(root, "sub-99999/anat/sub-99999_T1w.json").exists()
    assert "unknown site 'Nowhere'" in caplog.text
