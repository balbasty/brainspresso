
from brainspresso.sources.base import StaticURLs


URLBASE = 'https://download.nrg.wustl.edu/data'
OASISBASE = 'https://sites.wustl.edu/oasisbrains/files/2024/03/'
URLS = {
    'raw': [
        f'{URLBASE}/OAS2_RAW_PART1.tar.gz',
        f'{URLBASE}/OAS2_RAW_PART2.tar.gz',
    ],
    'meta': [
        f'{OASISBASE}/oasis_longitudinal_demographics-8d83e569fa2e2d30.xlsx',
    ],
}



class WUSTL(StaticURLs):
    """OASIS-II parts (download.nrg.wustl.edu) and demographics
    (sites.wustl.edu), no login. Per-run option: `parts`."""

    name = "wustl"
    parts: frozenset = frozenset({1, 2})
    urls = {
        "raw": [StaticURLs.entry(url, part=part)
                for part, url in enumerate(URLS["raw"], 1)],
        "meta": [StaticURLs.entry(URLS["meta"][0],
                                  "oasis_longitudinal_demographics.xlsx")],
    }

    def keep(self, entry, selection, wants) -> bool:
        part = entry.meta.get("part")
        return part is None or part in self.parts


