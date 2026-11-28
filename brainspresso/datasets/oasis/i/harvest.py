
"""Sources of OASIS-1: the WUSTL discs and spreadsheets."""
from brainspresso.datasets.oasis.i.roaster import roast_oasis1
from brainspresso.sources.base import StaticURLs

URLDATA = 'https://download.nrg.wustl.edu/data'
URLMETA = 'https://sites.wustl.edu/oasisbrains/files'
UID_META = '5708aa0a98d82080'
UID_RELI = '063c8642b909ee76'
UID_FACT = 'bcc7a002dfb104f4'
URLS = {
    'meta':
        f'{URLMETA}/2024/04/oasis_cross-sectional-{UID_META}.xlsx',
    'reliability':
        f'{URLMETA}/2024/04/oasis_cross-sectional-reliability-{UID_RELI}.xlsx',
    'facts':
        f'{URLMETA}/2024/03/oasis_cross-sectional_facts-{UID_FACT}.pdf',
    'raw': [
        URLDATA + '/oasis_cross-sectional_disc{:d}.tar.gz'.format(d)
        for d in range(1, 13)
    ],
    'fs': [
        URLDATA + '/oasis_cs_freesurfer_disc{:d}.tar.gz'.format(d)
        for d in range(1, 12)  # !!! no freesurfer disk 12
    ],
}


# local names of the spreadsheets (without their upload id)
NAMES = {
    'meta': 'oasis_cross-sectional.xlsx',
    'reliability': 'oasis_cross-sectional-reliability.xlsx',
    'facts': 'oasis_cross-sectional_facts.pdf',
}


class WUSTL(StaticURLs):
    """
    OASIS-I discs and spreadsheets, no login.

    Discs from download.nrg.wustl.edu, spreadsheets from
    sites.wustl.edu. Per-run option: `discs`.
    """

    name = "wustl"
    roaster = staticmethod(roast_oasis1)
    discs: frozenset = frozenset(range(1, 13))
    urls = {
        key: (
            [StaticURLs.entry(url, disc=disc)
             for disc, url in enumerate(value, 1)]
            if isinstance(value, list) else
            [StaticURLs.entry(value, NAMES[key])]
        )
        for key, value in URLS.items()
    }

    def keep(self, entry, selection, wants) -> bool:
        """
        Keep the selected discs (`discs`) and every spreadsheet.

        Parameters
        ----------
        entry : RemoteEntry
            A file of the source
        selection : Selection
            Subjects, sessions... (unused: discs hold many subjects)
        wants : Wants
            Keys (unused: entries are listed per key)

        Returns
        -------
        bool
            Whether to harvest the entry
        """
        disc = entry.meta.get("disc")
        return disc is None or disc in self.discs
