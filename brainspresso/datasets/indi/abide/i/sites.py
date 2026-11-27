"""
Sites and samples of ABIDE-I, and their sidecar templates.

```python
parse_site("MaxMun_a")              # ("MaxMun", "a")
parse_site("UCLA1")                 # ("UCLA", "1")
site_template("CMU_a", "T1w.json")  # templates/CMU/T1w.json
```

Every source spells sites its own way: S3/NITRC folders and tarballs
(`MaxMun_a`, `UCLA_1`, `NYU_a`), XNAT subject labels and scan-parameter
folders (`CMU`, `Leuven-1`, `UCLA1`) and phenotypic site IDs (`MAX_MUN`,
`LEUVEN_1`). Sites with per-sample sidecars have one template folder per
sample (`templates/<Site>_<sample>/`), the others one per site
(`templates/<Site>/`).
"""
from __future__ import annotations

import os
from logging import getLogger
from pathlib import Path
from typing import Callable

lg = getLogger(__name__)

__all__ = [
    "PARTS",
    "SAMPLES",
    "SITES",
    "TEMPLATES",
    "TemplateError",
    "parse_site",
    "site_template",
]

#: Folder of the templates shipped with ABIDE-I
TEMPLATES = Path(__file__).parent / "templates"

#: Sites (names of `--sites`)
SITES: tuple[str, ...] = (
    'Caltech', 'CMU', 'KKI', 'Leuven', 'MaxMun', 'NYU', 'OHSU', 'Olin',
    'Pitt', 'SBL', 'SDSU', 'Stanford', 'Trinity', 'UCLA', 'UM', 'USM',
    'Yale',
)

#: Sites whose sidecars are per sample (`templates/<Site>_<sample>/`)
SAMPLES: dict[str, tuple[str, ...]] = {
    'CMU': ('a', 'b'),
    'Leuven': ('1', '2'),
    'MaxMun': ('a', 'b', 'c', 'd'),
    'UCLA': ('1', '2'),
    'UM': ('1', '2'),
}

#: Parts of the NITRC tarballs of sites without samples (`NYU_a.tgz`;
#: the CMU and MaxMun parts of `sources.PARTS` are samples here)
PARTS: dict[str, tuple[str, ...]] = {
    'NYU': ('a', 'b', 'c', 'd', 'e'),
}


class TemplateError(LookupError):
    """No sidecar template for a (site, template) pair."""


def parse_site(site: str) -> tuple[str, str | None]:
    """
    Split a site name found in the data into (Site, sample).

    Parameters
    ----------
    site : str
        A site name in the spelling of any source

    Returns
    -------
    site : str
        Name of the site (as in `SITES`)
    sample : str | None
        None when the data does not give it, or for a site without
        per-sample sidecars (whose only accepted suffix is a tarball
        part, e.g. `NYU_a`)

    Raises
    ------
    TemplateError
        For an unknown site, sample or suffix
    """
    flat = ''.join(c for c in site if c.isalnum()).lower()
    for name in sorted(SITES, key=len, reverse=True):
        if flat.startswith(name.lower()):
            break
    else:
        raise TemplateError(f'ABIDE-I: unknown site {site!r}')
    suffix = flat[len(name):] or None
    if name not in SAMPLES:
        if suffix is not None and suffix not in PARTS.get(name, ()):
            raise TemplateError(
                f'ABIDE-I: unknown suffix {suffix!r} of site {name} '
                f'(from {site!r}); known parts: {PARTS.get(name, ())}')
        return name, None
    if suffix is not None and suffix not in SAMPLES[name]:
        raise TemplateError(
            f'ABIDE-I: unknown sample {suffix!r} of site {name} '
            f'(from {site!r}); known samples: {SAMPLES[name]}')
    return name, suffix


_warned: set = set()


def _packaged(name: str | os.PathLike) -> Path | None:
    """Find a template shipped with ABIDE-I (None if there is none)."""
    path = TEMPLATES / name
    return path if path.is_file() else None


def site_template(
    site: str,
    name: str,
    resource: Callable[[str], Path | None] | None = None,
) -> Path:
    """
    Find the sidecar template of a scan.

    * Site without samples: `<Site>/<name>`.
    * Known sample: `<Site>_<sample>/<name>`, else `<Site>/<name>`
      (e.g. `CMU/T1w.json`, used by CMU_a, which has no anatomical
      printout).
    * Unknown sample (e.g. XNAT `CMU_50642`, `MaxMun`): `<Site>/<name>`
      if it exists, a template holding only what the samples share
      (e.g. `CMU/bold.json`; a warning is logged); else the first
      sample's, which is only used where all samples share one protocol
      (see `tests/test_abide1_templates.py`).

    Parameters
    ----------
    site : str
        The site name found in the data (see `parse_site`)
    name : str
        Name of the template (`T1w.json`, `bold.json`...)
    resource : callable(str) -> Path | None, optional
        Finds a resource (`RoastContext.resource`: `sourcedata/` first,
        then the templates of the dataset); default: the templates
        shipped with ABIDE-I

    Returns
    -------
    Path

    Raises
    ------
    TemplateError
        Naming the site and template, if none exists
    """
    resource = resource or _packaged
    site_name, sample = parse_site(site)
    common = f'{site_name}/{name}'
    if site_name in SAMPLES and sample is None:
        candidates = [common, f'{site_name}_{SAMPLES[site_name][0]}/{name}']
    elif site_name in SAMPLES:
        candidates = [f'{site_name}_{sample}/{name}', common]
    else:
        candidates = [common]
    for candidate in candidates:
        path = resource(candidate)
        if path is None:
            continue
        if (sample is None and site_name in SAMPLES
                and candidate == common and (site, name) not in _warned):
            _warned.add((site, name))
            lg.warning(f'ABIDE-I: site {site!r} names no sample of '
                       f'{site_name}: using the site template {candidate}')
        return Path(path)
    raise TemplateError(
        f'ABIDE-I: no template {name!r} for site {site!r} '
        f'(tried {", ".join(candidates)})')
