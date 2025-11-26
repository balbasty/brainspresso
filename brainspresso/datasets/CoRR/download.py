from pathlib import Path
from enum import Enum
from typing import Iterable
from urllib.parse import urlparse
from humanize import naturalsize
from warnings import filterwarnings
from urllib3.exceptions import InsecureRequestWarning

from brainspresso.utils.ui import human2bytes
from brainspresso.utils.path import get_tree_path
from brainspresso.utils.log import setup_filelog
from brainspresso.sources.nitrc import nitrc_authentifier_async
from brainspresso.download import DownloadManager
from brainspresso.download import Downloader
from brainspresso.download import IfExists
from brainspresso.download import CHUNK_SIZE
from brainspresso.datasets.CoRR.command import corr


SITES = (
    'BMB', 'BNU', 'DC', 'HNU', 'IACAS', 'IBA', 'IPCAS', 'JNHU', 'LMU', 'MPG',
    'MRN', 'NKI', 'NYU', 'SWU', 'UM', 'UPSM', 'Utah', 'UWM', 'XHCUMS',
)
KEYS = ("raw", "meta")

SiteChoice = Enum("SiteChoice", [(site, site) for site in SITES], type=str)
KeyChoice = Enum("KeyChoice", [("raw", "raw"), ("meta", "meta")], type=str)

URLBASE = 'https://fcon_1000.projects.nitrc.org/indi/CoRR/Data'
SITES = {
    'BMB': {1: ['0003001_0003042', '0003044_0003064']},
    'BNU': {1: ['0025864_0025883', '0025884_0025903', '0025904_0025920'],
            2: ['0025921_0025945', '0025946_0025970', '0025971_0025981'],
            3: ['0027055_0027069', '0027070_0027084', '0027085_0027099',
                '0027100_0027102']},
    'DC': {1: ['0027306_0027331', '0027332_0027367', '0027368_0027403',
               '0027404_0027429']},
    'HNU': {1: ['0025427_0025429', '0025430_0025432', '0025433_0025435',
                '0025436_0025438', '0025439_0025441', '0025442_0025444',
                '0025445_0025447', '0025448_0025449', '0025450',
                '0025451_0025453', '0025454_0025456']},
    'IACAS': {1: ['0025457_0025482', '0025483_0025484']},
    'IBA': {'TRT': ['0027223_0027233', '0027234_0027247', '0027248_0027258']},
    'IPCAS': {1: ['0025485_0025497', '0025498_0025510', '0025511_0025514'],
              2: ['0025515_0025535', '0025536_0025549'],
              3: ['0025550_0025583', '0025584_0025585'],
              4: ['0026190_0026209'],
              5: ['0027284_0027301', '0027302_0027305'],
              6: ['0026044_0026045'],
              7: ['0026046_0026074', '0026075_0026103', '0026104_0026119'],
              8: ['0025586_0025598']},
    'JHNU': {1: ['0025599_0025628']},
    'LMU': {1: ['0025335_0025339', '0025340_0025344', '0025345_0025349',
                '0025350_0025354', '0025355_0025359', '0025360_0025361'],
            2: ['0025362_0025389', '0025390_0025401'],
            3: ['0025402_0025426']},
    'MPG': {1: ['0027430', '0027431', '0027432', '0027433', '0027434',
                '0027435', '0027436', '0027437', '0027438', '0027439',
                '0027440', '0027441', '0027442', '0027443', '0027444',
                '0027445', '0027446', '0027447', '0027448', '0027449',
                '0027450', '0027451']},
    'MRN': {1: ['0027010_0027048', '0027049_0027419']},
    'NKI': {'TRT': ['0021001', '0021002', '0021006', '0021018', '0021024',
                    '1427581', '1793622', '1961098', '2475376', '2799329',
                    '2842950', '3201815', '3313349', '3315657', '3795193',
                    '3808535', '3893245', '4176156', '4288245', '6471972',
                    '7055197', '8574662', '8735778', '9630905'],
            2: ['imaging_data_A00018030_A00027159',
                'imaging_data_A00027167_A00027439',
                'imaging_data_A00031881_A00033714',
                'imaging_data_A00034350_A00035292',
                'imaging_data_A00035377_A00035561',
                'imaging_data_A00035940_A00035945',
                'imaging_data_A00040556_A00040798',
                'imaging_data_A00040800_A00040815',
                'imaging_data_A00043240_A00043494',
                'imaging_data_A00043740_A00043788',
                'imaging_data_A00050848_A00051691',
                'imaging_data_A00051727_A00052069',
                'imaging_data_A00052165_A00052183',
                'imaging_data_A00052461_A00053203',
                'imaging_data_A00053320_A00053390',
                'imaging_data_A00053490_A00053744',
                'imaging_data_A00053873_A00054206',
                'imaging_data_A00054578_A00054581',
                'imaging_data_A00055076_A00055122',
                'imaging_data_A00055267_A00055462',
                'imaging_data_A00055612_A00055693',
                'imaging_data_A00055727_A00055728',
                'imaging_data_A00055866_A00055867',
                'imaging_data_A00055903_A00055907',
                'imaging_data_A00055908_A00055920',
                'imaging_data_A00055991_A00056022',
                'imaging_data_A00056138_A00056198',
                'imaging_data_A00056295_A00056307',
                'imaging_data_A00056420_A00056470',
                'imaging_data_A00056605_A00056679',
                'imaging_data_A00056746_A00056920',
                'imaging_data_A00057405_A00057480',
                'imaging_data_A00057725_A00058053',
                'imaging_data_A00058060_A00058061',
                'imaging_data_A00058215_A00058229',
                'imaging_data_A00058516_A00059325',
                'imaging_data_A00059865_A00060280',
                'imaging_data_A00060384_A00060429',
                'imaging_data_A00060603']},
    'NYU': {1: ['0027103_0027119', '0027120_0027127'],
            2: ['0025000_0025006', '0025007_0025012', '0025013_0025018',
                '0025019_0025024', '0025025_0025030', '0025031_0025037',
                '0025038_0025044', '0025045_0025050', '0025051_0025056',
                '0025057_0025063', '0025064_0025075', '0025076_0025087',
                '0025088_0025099', '0025100_0025111', '0025112_0025123',
                '0025124_0025135', '0025136_0025147', '0025148_0025159',
                '0025160_0025171', '0025172_0025183', '0025184_0025186']},
    'SWU': {1: ['0027203_0027212', '0027213_0027222'],
            2: ['0027176_0027202'],
            3: ['0027152_0027175'],
            4: ['0025629_0025638', '0025639_0025649', '0025650_0025660',
                '0025661_0025671', '0025672_0025681', '0025682_0025691',
                '0025692_0025702', '0025703_0025713', '0025714_0025723',
                '0025724_0025733', '0025734_0025744', '0025745_0025756',
                '0025757_0025766', '0025767_0025777', '0025778_0025787',
                '0025788_0025799', '0025800_0025809', '0025810_0025819',
                '0025820_0025830', '0025831_0025841', '0025842_0025852',
                '0025853_0025862', '0025863_0025863']},
    'UM': {1: ['0026007_0026135', '0026136_0026163', '0026164_0026189']},
    'UPSM': {1: ['0025234_0025263', '0025264_0025291', '0025292_0025328',
                 '0025329_0025333']},
    'Utah': {1: ['0026018_0026029', '0026030_0026041', '0026042_0026043'],
             2: ['0026017_part1', '0026017_part2']},
    'UWM': {1: ['0027259_0027273', '0027274_0027283']},
    'XHCUMS': {1: ['0025982', '0025983_0025986', '0025987_0025991',
                   '0025992_0025996', '0025997_0026000', '0026001_0026004',
                   '0026005_0026006']},
}


def fix_sample_data(site, samp):
    if site in ('IACAS', 'JHNU', 'MRN', 'UM', 'UWM', 'XHCUMS'):
        return ''
    return '_' + str(samp)


def fix_sample_meta(site, samp):
    if site in ('IACAS', 'UM', 'UWM', 'XHCUMS'):
        return ''
    if site in ('IBA',):
        return str(samp)
    if site == 'NKI' and samp == 'TRT':
        samp = 1
    return '_' + str(samp)


URLS = {
    SITE: {
        'raw': [
            f'{URLBASE}/{SITE}{fix_sample_data(SITE, SAMP)}/'
            f'{SITE}{fix_sample_data(SITE, SAMP)}'
            f'_{PART}.tar.gz'
            for SAMP, PARTS in SAMPLES.items()
            for PART in PARTS
        ],
        'meta': [
            f'{URLBASE}/PhenotypicData/{SITE}{fix_sample_meta(SITE, SAMP)}'
            f'_phenotypic_data.csv'
            for SAMP in SAMPLES
        ],
    }
    for SITE, SAMPLES in SITES.items()
}
URLS['DC']['meta'] = []
URLS['NKI']['meta'] = [
    f'{URLBASE}/PhenotypicData/NKI_1_phenotypic_data.csv',
    f'{URLBASE}/NKI_2/nki_2_corr_phenodata.csv'
]
URLS['NKI']['raw'] = [
    url.replace('NKI_2_', 'nki_2_') for url in URLS['NKI']['raw']
]


@corr.command(name="harvest")
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = KEYS,
    sites: Iterable[SiteChoice] = SITES,
    if_exists: IfExists.Choice = "skip",
    user: str | None = None,
    password: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    jobs: int | None = 1,
    log: str | None = None,
    level: str = "info",
):
    """
    Download source data for the CoRR dataset.

    **Possible keys:**
    * **raw**          All the raw imaging data
    * **meta**         Metadata

    Parameters
    ----------
    path
        Path to root of all datasets. A `CoRR` folder will be created.
    keys
        Data categories to download
    sites
        Sites to download
    parts
        Parts to download
    if_exists
        Behaviour if a file already exists
    user
        NITRC username
    password
        NITRC password
    packet
        Packet size to download, in bytes
    jobs
        Number of parallel downloaders
    log
        Path to log file

    """
    setup_filelog(log, level=level)
    path = Path(get_tree_path(path))
    keys = set(keys or KeyChoice.__args__)
    sites = set(sites or SiteChoice.__args__)
    src = path / 'CoRR' / 'sourcedata'
    auth = nitrc_authentifier_async(user, password)

    def downloaders():
        for site in sites:
            for key in keys:
                for url in URLS[site][key]:
                    yield Downloader(
                        url, src / Path(urlparse(url).path).name,
                        chunk_size=human2bytes(packet),
                        auth=auth,
                        get_opt=dict(verify_ssl=False),
                        ifnodigest="continue",
                    )

    filterwarnings('ignore', category=InsecureRequestWarning)
    DownloadManager(
        downloaders(),
        ifexists=if_exists,
        jobs=jobs,
    ).run("async")
