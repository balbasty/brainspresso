"""ABIDE-I: every site spelling of every source resolves to a template."""
import gzip
import io
import json
import logging
from pathlib import PurePosixPath

import nibabel as nib
import numpy as np
import pytest
from roast_utils import init_layout, make_tar, nii_bytes, raw_file

from brainspresso.datasets.indi.abide.i import sites, sources
from brainspresso.datasets.indi.abide.i.roaster import header_tr, roast
from brainspresso.datasets.indi.abide.i.sites import (
    TemplateError,
    site_template,
)

TPL = sites.TEMPLATES
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
SITES = sorted({*sites.SITES, *NITRC, *PHENO, *XNAT})


def test_site_spellings():
    # sanity: the source spellings that motivated the mapping are covered
    assert {"CMU_a", "MaxMun_d", "Leuven_2", "UM_1", "NYU_e"} <= set(NITRC)
    assert {"MAX_MUN", "LEUVEN_1", "UCLA_1", "UM_1", "CMU"} <= set(PHENO)
    assert {"Leuven-1", "UCLA1", "UM2", "CMU", "MaxMun"} <= set(XNAT)


@pytest.mark.parametrize("name", NAMES)
@pytest.mark.parametrize("site", SITES)
def test_template_exists(site, name):
    assert site_template(site, name).is_file()


@pytest.mark.parametrize("site", ["UCLA", "UCLA_1", "UCLA2", "UCLA_2"])
def test_template_hires(site):
    assert site_template(site, "acq-hires_T2w.json").is_file()


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
    # sample unknown, samples differ: what the samples share
    ("CMU", "bold.json", "CMU/bold.json"),
    ("CMU_50642".rsplit("_", 1)[0], "bold.json", "CMU/bold.json"),
    ("CMU", "T1w.json", "CMU/T1w.json"),
    ("MaxMun", "bold.json", "MaxMun/bold.json"),
    ("MAX_MUN", "bold.json", "MaxMun/bold.json"),
    # sample unknown, samples share one protocol: first sample
    ("MaxMun", "T1w.json", "MaxMun_a/T1w.json"),
    ("Leuven", "bold.json", "Leuven_1/bold.json"),
    ("UCLA", "T1w.json", "UCLA_1/T1w.json"),
    ("UM", "bold.json", "UM_1/bold.json"),
    # sites without samples: suffix (tarball part) ignored
    ("NYU_c", "bold.json", "NYU/bold.json"),
    ("CALTECH", "T1w.json", "Caltech/T1w.json"),
    ("USM", "T1w.json", "USM/T1w.json"),
])
def test_template_mapping(site, name, expected):
    assert site_template(site, name) == TPL / expected


def test_template_errors():
    with pytest.raises(TemplateError, match="unknown site 'Nowhere'"):
        site_template("Nowhere", "T1w.json")
    with pytest.raises(TemplateError, match="unknown sample 'e'.*MaxMun"):
        site_template("MaxMun_e", "T1w.json")
    with pytest.raises(TemplateError, match="'dwi.json' for site 'CMU_a'"):
        site_template("CMU_a", "dwi.json")
    # sites without samples: only a tarball part is accepted as suffix
    with pytest.raises(TemplateError, match="unknown suffix 'backup'.*SDSU"):
        site_template("SDSU_backup", "T1w.json")
    with pytest.raises(TemplateError, match="unknown suffix 'f'.*NYU"):
        site_template("NYU_f", "T1w.json")
    with pytest.raises(TemplateError, match="unknown suffix 'a'.*Caltech"):
        site_template("Caltech_a", "T1w.json")


def test_parts_match_sources():
    """Tarball parts of sites without samples are those of the sources."""
    expected = {site: tuple(p.lstrip("_") for p in parts)
                for site, parts in sources.PARTS.items()
                if site not in sites.SAMPLES}
    assert sites.PARTS == expected


def _load(path):
    return json.loads(path.read_text())


@pytest.mark.parametrize("name", NAMES + ("acq-hires_T2w.json",))
@pytest.mark.parametrize("site", sorted(sites.SAMPLES))
def test_unknown_sample_never_borrows(site, name):
    """With no sample in the data, a site whose samples differ uses a
    template holding only what they share, never one sample's values."""
    samples = [TPL / f"{site}_{s}" / name
               for s in sites.SAMPLES[site]]
    samples = [_load(p) for p in samples if p.is_file()]
    if not samples:
        return
    common = TPL / site / name
    got = _load(site_template(site, name))
    if all(s == samples[0] for s in samples):
        assert got in samples
    else:
        assert common.is_file()
        assert got == {k: v for k, v in samples[0].items()
                       if all(s.get(k, object()) == v for s in samples)}


def test_unknown_sample_warns(caplog):
    sites._warned.clear()
    with caplog.at_level(logging.WARNING):
        site_template("CMU", "bold.json")
        site_template("CMU", "bold.json")
        site_template("CMU_b", "bold.json")
        site_template("Leuven", "bold.json")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "'CMU' names no sample" in warnings[0].getMessage()


def test_samples_match_templates():
    """Every per-sample template folder is declared, and vice versa."""
    folders = {p.name for p in TPL.iterdir() if p.is_dir()
               and p.name != "phenotype"}
    declared = {f"{site}_{s}" for site, samples in
                sites.SAMPLES.items() for s in samples}
    assert {f for f in folders if "_" in f} == declared
    assert {f for f in folders if "_" not in f} <= set(sites.SITES)
    for site in sites.SITES:
        for name in NAMES:
            assert site_template(site, name).is_file()


def bold_bytes(tr=None, unit="sec"):
    """Gzipped 4D NIfTI-1 with TR `tr` (pixdim[4]) in `unit`."""
    img = nib.Nifti1Image(np.zeros((2, 2, 2, 3), dtype=np.int16), np.eye(4))
    if tr is not None:
        img.header.set_zooms((1., 1., 1., tr))
        img.header.set_xyzt_units("mm", unit)
    return gzip.compress(img.to_bytes())


@pytest.mark.parametrize("tr, unit, expected", [
    (1.5, "sec", 1.5), (2000., "msec", 2.0), (None, "sec", None),
    (1.5, "unknown", None), (0., "sec", None), (300., "sec", None),
])
def test_header_tr(tr, unit, expected):
    f = io.BytesIO(bold_bytes(tr, unit))
    assert header_tr(f, "rest.nii.gz") == expected


def test_header_tr_3d():
    f = io.BytesIO(nii_bytes())
    assert header_tr(f, "mprage.nii.gz") is None


def test_roast_unsampled_sites(tmp_path, caplog):
    """Bare `CMU` bold and `MaxMun` without sample; unknown site logged."""
    root = tmp_path / "ABIDE-1"
    init_layout(root, "study")
    src = root / "sourcedata"
    src.mkdir(parents=True)
    make_tar(src / "CMU.tgz", {
        # TR from the header (not in the common CMU/bold.json)
        "CMU/0050642/session_1/rest_1/rest.nii.gz": bold_bytes(1.5),
        # no TR in the header: sidecar error
        "CMU/0050643/session_1/rest_1/rest.nii.gz": bold_bytes(),
    })
    make_tar(src / "MaxMun.tgz", {
        "MaxMun/0051330/session_1/anat_1/mprage.nii.gz": nii_bytes(),
    })
    make_tar(src / "Nowhere.tgz", {
        "Nowhere/0099999/session_1/anat_1/mprage.nii.gz": nii_bytes(),
    })
    with caplog.at_level(logging.ERROR):
        report = roast(root, keys={"T1w", "rest"}, display=False)

    def sidecar(path):
        return json.loads(raw_file(root, path).read_text())

    def tpl(path):
        return json.loads((TPL / path).read_text())

    assert "RepetitionTime" not in tpl("CMU/bold.json")    # 2 s / 1.5 s
    bold = sidecar("sub-50642/func/sub-50642_task-rest_bold.json")
    assert bold == {**tpl("CMU/bold.json"), "RepetitionTime": 1.5}
    assert raw_file(root, "sub-50643/func/sub-50643_task-rest_bold.nii.gz"
                    ).exists()
    assert not raw_file(root, "sub-50643/func/sub-50643_task-rest_bold.json"
                        ).exists()
    assert "has no RepetitionTime (REQUIRED)" in caplog.text
    assert sidecar("sub-51330/anat/sub-51330_T1w.json") \
        == tpl("MaxMun_a/T1w.json")
    # unknown site: image converted, sidecar error logged and counted
    assert raw_file(root, "sub-99999/anat/sub-99999_T1w.nii.gz").exists()
    assert not raw_file(root, "sub-99999/anat/sub-99999_T1w.json").exists()
    assert "unknown site 'Nowhere'" in caplog.text
    assert sorted(tag for tag, *_ in report.errors) == ["T1w", "rest"]
