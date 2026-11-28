"""
Sites and samples of ABIDE-II, and their sidecar templates.

```python
parse_site("ABIDEII-KKI_1_29273_29322")     # ("KKI", "1")
parse_site("ABIDEII-U_MIA_1")               # ("U_MIA", "1")
parse_site("ABIDE-I_PITT")                  # ("UPSM", "1")
site_template("NYU", "2", "dwi.json")       # templates/NYU/2/dwi.json
```

Sources spell sites their own way: NITRC tarballs and the folders inside
them (`ABIDEII-<SITE>_<sample>[_<part>]`), the `SITE_ID` of the
phenotypic files (`ABIDEII-NYU_2`, also seen as `ABIDE-II_NYU_2`, and
`ABIDE-I_UCLA_1`, `ABIDE-I_PITT` for the ABIDE-I subjects of the
longitudinal samples) and the bucket (`ETHZ` for `ETH`). Templates are
per site and sample: `templates/<SITE>/<sample>/<name>`.
"""
from __future__ import annotations

import os
import re
from logging import getLogger
from pathlib import Path
from typing import Callable

from brainspresso.roast.sidecars import TemplateError

lg = getLogger(__name__)

__all__ = [
    "ALIASES",
    "NO_TEMPLATES",
    "SAMPLES",
    "SITES",
    "SITE_LABELS",
    "TEMPLATES",
    "parse_site",
    "sample_label",
    "site_label",
    "site_template",
]

#: Folder of the templates shipped with ABIDE-II
TEMPLATES = Path(__file__).parent / "templates"

#: Sites (names of `--sites`)
SITES: tuple[str, ...] = (
    'BNI', 'EMC', 'ETH', 'GU', 'IU', 'IP', 'KUL', 'KKI', 'NYU', 'ONRC',
    'OHSU', 'TCD', 'SDSU', 'SU', 'UCD', 'UCLA', 'U_MIA', 'USM', 'UPSM'
)

#: Other spellings of sites (upper case, without `_`; `OILH`: Olin,
#: Institute of Living at Hartford Hospital, the ONRC printouts)
ALIASES: dict[str, str] = {'ETHZ': 'ETH', 'PITT': 'UPSM', 'MIA': 'U_MIA',
                           'OILH': 'ONRC'}

#: Sites without sidecar templates (no protocol printout): their images
#: are written without sidecars, with a warning
NO_TEMPLATES: tuple[str, ...] = ('SU', 'U_MIA')

#: Sites with several samples (their `sample` in `participants.tsv` is
#: `<SITE><sample>`; that of the others is the site). The `SITE_ID`s of
#: ABIDE-I (`ABIDE-I_UCLA_1`, `ABIDE-I_PITT`) are the baselines of the
#: longitudinal samples: their sample is `UCLALong`, `UPSMLong`
SAMPLES: dict[str, tuple[str, ...]] = {
    'NYU': ('1', '2'),
    'ONRC': ('1', '2'),
    'SU': ('1', '2'),
    'UCLA': ('1', 'Long'),
    'UPSM': ('Long',),
}

#: Site names of `participants.tsv` that are not those of `SITES`
SITE_LABELS: dict[str, str] = {'U_MIA': 'UMia'}

_PREFIX_II = re.compile(r"^ABIDE[-_]?(?:II|2)[-_]", re.IGNORECASE)
_PREFIX_I = re.compile(r"^ABIDE[-_]?(?:I|1)[-_]", re.IGNORECASE)
_SAMPLE = re.compile(r"\d+|long", re.IGNORECASE)
_NAMES = {**{s.replace("_", ""): s for s in SITES}, **ALIASES}


def parse_site(name: str) -> tuple[str, str | None]:
    """
    Split a site name found in the data into (SITE, sample).

    Parameters
    ----------
    name : str
        `ABIDEII-<SITE>_<sample>[_<part>]` (tarballs and their folders),
        a `SITE_ID` (`ABIDEII-NYU_2`, `ABIDE-II_NYU_2`, `ABIDE-I_PITT`)
        or a bare `<SITE>[_<sample>]`; any case

    Returns
    -------
    site : str
        Name of the site (as in `SITES`)
    sample : str | None
        `"1"`, `"2"`, `"Long"`...; `"1"` for an ABIDE-I name without
        one (`ABIDE-I_PITT`), else None if the name gives none

    Raises
    ------
    TemplateError
        For an unknown site
    """
    text = str(name).strip()
    abide1 = False
    if _PREFIX_II.match(text):
        text = _PREFIX_II.sub("", text)
    elif _PREFIX_I.match(text):
        text, abide1 = _PREFIX_I.sub("", text), True
    tokens = [t for t in re.split(r"[_\s]+", text) if t]
    for n in (2, 1):
        key = "".join(tokens[:n]).upper()
        if len(tokens) >= n and key in _NAMES:
            site, rest = _NAMES[key], tokens[n:]
            break
    else:
        raise TemplateError(f'ABIDE-II: unknown site {name!r}')
    sample = rest[0] if rest and _SAMPLE.fullmatch(rest[0]) else None
    if sample is not None:
        sample = "Long" if sample.lower() == "long" else str(int(sample))
    elif abide1:
        sample = "1"
    return site, sample


_unknown: set = set()


def _parse_site_id(site_id: str) -> tuple[str, str | None, bool] | None:
    """(SITE, sample, ABIDE-I) of a `SITE_ID` (None, warned once: none)."""
    try:
        site, sample = parse_site(site_id)
    except TemplateError:
        if site_id not in _unknown:
            _unknown.add(site_id)
            lg.warning(f"ABIDE-II: unknown SITE_ID {site_id!r}: kept as "
                       f"it is in participants.tsv")
        return None
    return site, sample, bool(_PREFIX_I.match(str(site_id).strip()))


def site_label(site_id: str) -> str:
    """
    Site of a `SITE_ID` (`participants.tsv`).

    Parameters
    ----------
    site_id : str
        `ABIDEII-NYU_2`, `ABIDE-I_PITT`...

    Returns
    -------
    str
        `NYU`, `UPSM`, `UMia` (`SITE_LABELS`); the value itself if it
        names no site (warned once)
    """
    parsed = _parse_site_id(site_id)
    if parsed is None:
        return site_id
    return SITE_LABELS.get(parsed[0], parsed[0])


def sample_label(site_id: str) -> str:
    """
    Sample of a `SITE_ID` (`participants.tsv`).

    Parameters
    ----------
    site_id : str
        `ABIDEII-NYU_2`, `ABIDE-I_PITT`...

    Returns
    -------
    str
        `<SITE><sample>` for the sites with several samples (`NYU2`,
        `UCLA1`, `UCLALong`); `<SITE>Long` for a `SITE_ID` of ABIDE-I
        (`ABIDE-I_UCLA_1`, `ABIDE-I_PITT`: the baseline of a
        longitudinal sample); else the site (`BNI`, `UMia`); the value
        itself if it names no site (warned once)
    """
    parsed = _parse_site_id(site_id)
    if parsed is None:
        return site_id
    site, sample, abide1 = parsed
    if abide1:
        return f"{site}Long"
    if site in SAMPLES and sample is not None:
        return f"{site}{sample}"
    return SITE_LABELS.get(site, site)


def _packaged(name: str | os.PathLike) -> Path | None:
    """Find a template shipped with ABIDE-II (None if there is none)."""
    path = TEMPLATES / name
    return path if path.is_file() else None


def site_template(
    site: str,
    sample: str | None,
    name: str,
    resource: Callable[[str], Path | None] | None = None,
) -> Path:
    """
    Find the sidecar template of a scan: `<SITE>/<sample>/<name>`.

    Parameters
    ----------
    site : str
        Site (as in `SITES`)
    sample : str | None
        Sample (`"1"` if None: every site has a first sample but UPSM,
        KUL, ONRC and SU)
    name : str
        Name of the template (`T1w.json`, `bold.json`, `dwi.json`)
    resource : callable(str) -> Path | None, optional
        Finds a resource (`RoastContext.resource`: `sourcedata/` first,
        then the templates of the dataset); default: the templates
        shipped with ABIDE-II

    Returns
    -------
    Path

    Raises
    ------
    TemplateError
        Naming the site, sample and template, if none exists
    """
    resource = resource or _packaged
    candidate = f"{site}/{sample or '1'}/{name}"
    path = resource(candidate)
    if path is None:
        raise TemplateError(f'ABIDE-II: no template {name!r} for site '
                            f'{site} sample {sample or "1"} (tried '
                            f'{candidate})')
    return Path(path)
