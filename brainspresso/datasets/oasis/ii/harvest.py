"""Sources of OASIS-2: the WUSTL parts and demographics."""
from brainspresso.datasets.oasis.ii.roaster import roast_oasis2
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
    """
    OASIS-II parts and demographics, no login.

    Parts from download.nrg.wustl.edu, demographics from
    sites.wustl.edu. Per-run option: `parts`.
    """

    name = "wustl"
    roaster = staticmethod(roast_oasis2)
    parts: frozenset = frozenset({1, 2})
    urls = {
        "raw": [StaticURLs.entry(url, part=part)
                for part, url in enumerate(URLS["raw"], 1)],
        "meta": [StaticURLs.entry(URLS["meta"][0],
                                  "oasis_longitudinal_demographics.xlsx")],
    }

    def keep(self, entry, selection, wants) -> bool:
        """
        Keep the selected parts (`parts`) and the demographics.

        Parameters
        ----------
        entry : RemoteEntry
            A file of the source
        selection : Selection
            Subjects, sessions... (unused: parts hold many subjects)
        wants : Wants
            Keys (unused: entries are listed per key)

        Returns
        -------
        bool
            Whether to harvest the entry
        """
        part = entry.meta.get("part")
        return part is None or part in self.parts
