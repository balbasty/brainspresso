import pymupdf
import json
from pathlib import Path
from warnings import filterwarnings
from urllib3.exceptions import InsecureRequestWarning

from brainspresso.download import Downloader, DownloadManager


DOWNLOAD = False
PARSE = True

# Download all PDFs

SITES = {
    'BMB': [1], 'BNU': [1, 2, 3], 'DC': [1], 'HNU': [1], 'IACAS': [1],
    'IBA': ['TRT'], 'IPCAS': [1, 2, 3, 4, 5, 6, 7, 8], 'JHNU': [1],
    'LMU': [1, 2, 3], 'MPG': [1], 'MRN': [1], 'NKI': ['TRT', 2],
    'NYU': [1, 2], 'SWU': [1, 2, 3, 4], 'UM': [1], 'UPSM': [1],
    'Utah': [1, 2], 'UWM': [''], 'XHCUMS': [''],
}
URLBASE = 'https://fcon_1000.projects.nitrc.org/indi/CoRR/html/_static/scan_parameters'
URLS = {
    SITE: {
        SAMP:
            f'{URLBASE}/{SITE}{"_" + str(SAMP) if SAMP and SAMP != "TRT" else SAMP}_scantable.pdf'
        for SAMP in SAMPLES
    }
    for SITE, SAMPLES in SITES.items()
}
# Fix special cases
URLS['BNU'][2] = {
    'test': f'{URLBASE}/BNU_2_Test_scantable.pdf',
    'retest': f'{URLBASE}/BNU_2_Retest_scantable.pdf',
}
del URLS['NKI']['TRT']
# {
#     'rest_645': f'{URLBASE}/nki/nki_rest_645.pdf',
#     'rest_1400': f'{URLBASE}/nki/nki_rest_1400.pdf',
#     'rest_2500': f'{URLBASE}/nki/nki_rest_2500.pdf',
#     'dti': f'{URLBASE}/nki/nki_DTI.pdf',
# }
URLS['XHCUMS'][''] = f'{URLBASE}/beijing_li/beijing_li_all.pdf'
del URLS['UWM']


def downloaders():
    for sites in URLS.values():
        for urls in sites.values():
            if isinstance(urls, dict):
                urls = urls.values()
            else:
                urls = [urls]
            for url in urls:
                yield Downloader(
                    url, Path(__file__).parent / 'PDFs' / url.split('/')[-1],
                    get_opt=dict(verify=False),

                )


if DOWNLOAD:
    filterwarnings('ignore', category=InsecureRequestWarning)
    DownloadManager(downloaders()).run()


# Parse PDFs


def error():
    raise RuntimeError


keymap_common = {
    'Manufacturer': 'Manufacturer',
    'ManufacturerModelName': 'Model',
    'MagneticFieldStrength': {
        'args': ['Field Strength'],
        'formula': lambda x: int(x[:-1])
    },
    'PulseSequenceType': {
        'args': ['Sequence'],
        'formula': lambda x: x.split()[-1],
    },
    'EchoTime': {
        'args': ['Echo Time (TE) [ms]'],
        'formula': lambda x: float(x) * 1e-3
    },
    'FlipAngle': {
        'args': ['Flip Angle [Deg]'],
        'formula': float
    },
    'ParallelReductionFactorInPlane': {
        'args': ['Parallel Acquisition'],
        'formula': lambda x: float(x.split('x')[-1].split('/')[0].strip())
    },
    'ParallelReductionFactorOutOfPlane': {
        'args': ['Parallel Acquisition'],
        'formula': lambda x: float(x.split('x')[-1].split('/')[1].strip())
    },
    'ParallelAcquisitionTechnique': {
        'args': ['Parallel Acquisition'],
        'formula': lambda x: x.split('x')[0].strip()
    },
    'PartialFourier': {
        'args': ['Partial Fourier'],
        'formula': lambda x: float(x.split('/')[0]) / float(x.split('/')[1])
    },
    'PhaseEncodingDirection': {
        'args': ['Slice Phase Encoding Direction'],
        'formula': lambda x: x.split()[0][0] + x.split()[-1][0]
        # needs to be fixed based on nifti affine later
    },
    'SliceEncodingDirection': {
        'args': ['Slice Orientation'],
        'formula': lambda x: x[0],
        # needs to be fixed based on nifti affine later
    },
    'SliceTiming': 'Slice Acquisition Order',
    # ^ needs to be fixed later
}


keymap_anat = {
    **keymap_common,
    'RepetitionTimePreparation': {
        'args': ['Repetition Time (TR) [ms]'],
        'formula': lambda x: float(x) * 1e-3
    },
    'InversionTime': {
        'args': ['Inversion Time (TI) [ms]'],
        'formula': lambda x: float(x) * 1e-3
    },
    'DwellTime': {
        'args': [
            'Bandwidth per Voxel (Readout) [Hz]',
            'Acquisition Matrix',
        ],
        'formula': (
            lambda bw, mat: 1 / (max(map(int, mat.split('x'))) * float(bw))
        )
    },
    'MRAcquisitionType': {
        'args': ['Sequence'],
        'formula': lambda x:
            x.split()[0] if x.split()[0] in ('2D', '3D') else '3D',
    },
}

keymap_rest = {
    **keymap_common,
    'RepetitionTimeExcitation': {
        'args': ['Repetition Time (TR) [ms]'],
        'formula': lambda x: float(x) * 1e-3
    },
    'MRAcquisitionType': {
        'args': [],
        'formula': '2D',
    },
    'EffectiveEchoSpacing': {
        'args': [
            'Bandwidth per Voxel (Readout) [Hz]',
            'Acquisition Matrix',
        ],
        'formula': (
            lambda bw, mat: 1 / (min(map(int, mat.split('x'))) * float(bw))
        )
    },
}

keymap_dti = {
    **keymap_common,
    'MRAcquisitionType': {
        'args': [],
        'formula': '2D',
    },
    'EffectiveEchoSpacing': {
        'args': [
            'Bandwidth per Voxel (Readout) [Hz]',
            'Acquisition Matrix',
        ],
        'formula': (
            lambda bw, mat: 1 / (min(map(int, mat.split('x'))) * float(bw))
        )
    },
}


def do_parse(input, mapper, modality):
    output = {}
    for key, keymap in mapper.items():
        try:
            if isinstance(keymap, str):
                value = input[keymap]
            else:
                args = [input[arg] for arg in keymap['args']]
                value = keymap['formula'](*args)
            if value not in ('-', '--'):
                output[key] = value
        except Exception:  # as e:
            # print(modality, key, type(e), e)
            pass
    return output


def parse_pdf(path):
    print(path.name)
    name = path.name.split('_')
    if name[0] == 'IBATRT':
        opath = Path(__file__).parent / 'IBA' / 'TRT'
    else:
        opath = Path(__file__).parent / name[0] / name[1]
    opath.mkdir(parents=True, exist_ok=True)

    pdf = pymupdf.open(str(path))
    page = pdf[0]
    content = page.get_text()
    has_dti = 'DTI' in content
    content = list(map(lambda x: x.strip(), content.split('\n')))
    if 'NKI' in path.name:
        keys = content[0::6]
        anat = content[1::6]
        rest = {
            '2500': content[2::6],
            '1400': content[3::6],
            '645': content[4::6],
        }
        dti = content[5::6]
    elif has_dti:
        keys = content[0::4]
        anat = content[1::4]
        rest = content[2::4]
        dti = content[3::4]
    else:
        keys = content[0::3]
        anat = content[1::3]
        rest = content[2::3]
    # print(keys)

    anat = {key: value for key, value in zip(keys, anat)}
    T1w = do_parse(anat, keymap_anat, 'T1w')
    with open(opath / 'T1w.json', 'w') as f:
        json.dump(T1w, f, indent=4)

    if 'NKI' in path.name:
        for te, values in rest.items():
            rest = {key: value for key, value in zip(keys, values)}
            bold = do_parse(rest, keymap_rest, 'bold' + te)
            with open(opath / f'bold_TE={te}.json', 'w') as f:
                json.dump(bold, f, indent=4)
    else:
        rest = {key: value for key, value in zip(keys, rest)}
        bold = do_parse(rest, keymap_rest, 'bold')
        with open(opath / 'bold.json', 'w') as f:
            json.dump(bold, f, indent=4)

    if not has_dti:
        return

    dti = {key: value for key, value in zip(keys, dti)}
    dwi = do_parse(dti, keymap_dti, 'dwi')
    with open(opath / 'dwi.json', 'w') as f:
        json.dump(dwi, f, indent=4)


if PARSE:
    for path in (Path(__file__).parent / 'PDFs').glob('*.pdf'):
        if 'nki' in path.name or 'beijing_li' in path.name:
            continue
        parse_pdf(path)
