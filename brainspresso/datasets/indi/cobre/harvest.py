
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
    """Sources of COBRE: NITRC files (login), four at a time"""
    return {"nitrc": NITRC(URLS, jobs=4)}


