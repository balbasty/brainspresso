"""COBRE harvest: the files of the NITRC project (login required)."""
from brainspresso.datasets.indi.cobre.roaster import roast_cobre
from brainspresso.sources.nitrc import NITRC

URLBASE = 'https://fcon_1000.projects.nitrc.org/indi/retro/COBRE'
URLS = {
    'meta': [
        f'{URLBASE}/COBRE_parameters_rest.csv',
        f'{URLBASE}/COBRE_parameters_mprage.csv',
        f'{URLBASE}/COBRE_phenotypic_data.csv',
    ],
    'raw': [
        f'{URLBASE}/COBRE_scan_data.tgz'
    ],
}


def sources() -> dict:
    """
    Return the sources of COBRE.

    Returns
    -------
    dict[str, Source]
        NITRC files (login), four at a time, roasted by `roast_cobre`
    """
    return {"nitrc": NITRC(URLS, jobs=4, roaster=roast_cobre)}
