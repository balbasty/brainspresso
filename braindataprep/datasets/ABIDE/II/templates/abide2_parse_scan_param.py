import json
from pathlib import Path
from warnings import filterwarnings
from urllib3.exceptions import InsecureRequestWarning

from braindataprep.download import Downloader, DownloadManager
from braindataprep.utils.prot.fcon import parse_fcon_summary
from braindataprep.utils.prot.philips_txt import parse_philipps_txt
from braindataprep.utils.prot.ge_dicom_printout import parse_ge_dicom


DOWNLOAD = False
PARSE = True

# Download all PDFs

SITES = [
    'BNI', 'EMC', 'ETH', 'GU', 'IU', 'IP', 'KUL', 'KKI', 'NYU', 'ONRC',
    'OHSU', 'TCD', 'SDSU', 'SU', 'UCD', 'UCLA', 'U_MIA', 'USM', 'UPSM'
]
SITES = {SITE: [1] for SITE in SITES}
SITES.update({'KUL': [3], 'NYU': [1, 2], 'UCLA': [1, 'Long'],
              'UPSM': ['Long'], 'ONRC': [2], 'SU': [2]})

URLBASE = 'https://fcon_1000.projects.nitrc.org/indi/abide/scan_params'
URLS = {
    SITE: {
        SAMP: [
            f'{URLBASE}/ABIDEII-{SITE}{"_" + str(SAMP)}_scantable.pdf'
        ]
        for SAMP in SAMPLES
    }
    for SITE, SAMPLES in SITES.items()
}

# special cases
del URLS['KKI']
del URLS['ONRC']
del URLS['SU']
del URLS['U_MIA']

URLS['BNI'][1] += [
    f'{URLBASE}/ABIDEII-BNI_1/anat.txt',
    f'{URLBASE}/ABIDEII-BNI_1/rest.txt',
    f'{URLBASE}/ABIDEII-BNI_1/dti.txt',
    f'{URLBASE}/ABIDEII-BNI_1/3DFLAIR.txt',
]
URLS['EMC'][1] += [
    f'{URLBASE}/ABIDEII-EMC_1/anat.pdf',
    f'{URLBASE}/ABIDEII-EMC_1/rest.pdf',
]
URLS['ETH'][1] += [
    f'{URLBASE}/ABIDEII-ETH_1/anat.txt',
    f'{URLBASE}/ABIDEII-ETH_1/rest.txt',
]


def downloaders():
    for sites in URLS.values():
        for urls in sites.values():
            for url in urls:
                parts = url.split('/')
                if not parts[-1].endswith('scantable.pdf'):
                    name = parts[-2] + '_' + parts[-1]
                else:
                    name = parts[-1]
                yield Downloader(
                    url, Path(__file__).parent / 'PDFs' / name,
                    get_opt=dict(verify=False),
                    ifexists='skip',
                )


if DOWNLOAD:
    filterwarnings('ignore', category=InsecureRequestWarning)
    DownloadManager(downloaders()).run()


# Parse PDFs


def parse_pdf(path):
    name = list(path.name.split('_'))
    name[0] = name[0].split('-')[-1]
    odir = Path(__file__).parent / name[0] / name[1]
    odir.mkdir(parents=True, exist_ok=True)

    sidecars = parse_fcon_summary(path)

    for key, sidecar in sidecars.items():
        if key.lower().startswith('anat'):
            opath = odir / 'T1w.json'
        elif key.lower().startswith('rest'):
            if sidecar['PulseSequenceType'] == 'EPI':
                sidecar['PulseSequenceType'] = 'Gradient Echo EPI'
            opath = odir / 'bold.json'
        elif 'dti' in key.lower():
            if sidecar['PulseSequenceType'] == 'EPI':
                sidecar['PulseSequenceType'] = 'Spin Echo EPI'
            opath = odir / 'dwi.json'
        else:
            opath = odir / (key.replace(' ', '_') + '.json')

        with open(opath, 'w') as f:
            json.dump(sidecar, f, indent=4)


def parse_txt(path):
    name = list(path.name.split('_'))
    name[0] = name[0].split('-')[-1]
    odir = Path(__file__).parent / name[0] / name[1]
    odir.mkdir(parents=True, exist_ok=True)

    mod = {
        'anat': 'T1w', 'rest': 'bold', 'dti': 'dwi', '3DFLAIR': 'FLAIR'
    }[name[-1].split('.')[0]]
    mod += '.json'

    if (odir / mod).exists():
        with (odir / mod).open('r') as f:
            sidecar = json.load(f)
        strength = sidecar['MagneticFieldStrength']
    else:
        with (odir / 'T1w.json').open('r') as f:
            sidecar = json.load(f)
        strength = sidecar['MagneticFieldStrength']
        sidecar = {}

    sidecar.update(parse_philipps_txt(path, strength))

    with (odir / mod).open('w') as f:
        json.dump(sidecar, f, indent=4)


def parse_dicom_pdf(path):
    name = list(path.name.split('_'))
    name[0] = name[0].split('-')[-1]
    odir = Path(__file__).parent / name[0] / name[1]
    odir.mkdir(parents=True, exist_ok=True)

    mod = {
        'anat': 'T1w', 'rest': 'bold', 'dti': 'dwi', '3DFLAIR': 'FLAIR'
    }[name[-1].split('.')[0]]
    mod += '.json'

    if (odir / mod).exists():
        with (odir / mod).open('r') as f:
            sidecar = json.load(f)
    else:
        sidecar = {}

    sidecar.update(parse_ge_dicom(path))

    with (odir / mod).open('w') as f:
        json.dump(sidecar, f, indent=4)


if PARSE:
    for path in (Path(__file__).parent / 'PDFs').glob('*scantable.pdf'):
        parse_pdf(path)
    for path in (Path(__file__).parent / 'PDFs').glob('*.txt'):
        parse_txt(path)
    for path in (Path(__file__).parent / 'PDFs').glob('*.pdf'):
        if path.name.endswith('scantable.pdf'):
            continue
        parse_dicom_pdf(path)
