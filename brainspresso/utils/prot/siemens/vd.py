import pymupdf
import re
from os import PathLike
from typing import Iterator
from .utils import peekable


def _error(*a, **k):
    raise RuntimeError(*a, **k)


def _find_alignment(page: pymupdf.Page) -> float:
    """
    Find the left-most position of content within the page
    """
    traces = page.get_texttrace()
    # Skip page number and date
    traces = traces[1:-1]
    # Skip title elements:
    # [0] Protocol path
    # [1..5] General stuff (PAT, voxel size, etc)
    traces = traces[6:]
    # Find column alignment
    colx = float('inf')
    for trace in traces[:-1]:
        box = trace['bbox']
        text = page.get_textbox(box).strip()
        # skip non header/key components
        if not text:
            continue
        if '\n' in text:
            continue
        if text.startswith('\\\\'):
            continue
        # update left alignment
        colx = min(colx, box[0])
    return colx


def _parse_model(page: pymupdf.Page) -> tuple[str, str]:
    """
    Parse scanner model and software version
    """
    first_element = page.get_texttrace()[0]
    text = page.get_textbox(first_element['bbox'])
    pattern = r'SIEMENS MAGNETOM (?P<model>\w+) (?P<version>.+)'
    match = re.fullmatch(pattern, text)
    if match:
        return match.group('model'), match.group('version')
    else:
        return None, None


def _parse_title(blocks: peekable) -> dict:
    """
    Parse the title box of a protocol
    """
    *path, text = blocks.next()[0]
    path = ''.join([''.join(subpath) for subpath in path])
    text = ' '.join(text)
    title = dict(path=path)
    text = text.strip()
    pattern = (
        r'TA:\s*(?P<TA>\S+)\s+'
        r'PAT:\s*(?P<PAT>\S+)\s+'
        r'Voxel size:\s*'
        r'(?P<vx>[\d\.]+)\s*×\s*(?P<vy>[\d\.]+)\s*×\s*(?P<vz>[\d\.]+)\s*mm\s*'
        r'Rel. SNR:\s*(?P<SNR>\S+)\s+'
        r':\s*(?P<SIEMENS>\S+)'
    )
    match = re.fullmatch(pattern, text)
    if match:
        PAT = match.group('PAT')
        title.update({
            'TA': match.group('TA'),
            'PAT': int(PAT) if PAT != 'Off' else 'Off',
            'Voxel size': [float(match.group('vx')),
                           float(match.group('vy')),
                           float(match.group('vz'))],
            'Rel. SNR': float(match.group('SNR')),
            'SIEMENS': match.group('SIEMENS'),
        })
    return title


def _iter_blocks(doc: pymupdf.Document) -> Iterator[tuple[list, dict]]:
    """
    Iterator over all blocks in the document.
    Returns the corresponding text and block object.
    The returnted text is a list of list
    - outer loop: rows
    - inner loop: cells
    """
    for page in doc:
        blocks = page.get_textpage().extractDICT(sort=True)['blocks']
        # Skip page number (first elem) and date (last elem)
        for block in blocks:
            lines = [[]]
            x = None
            for line in block['lines']:
                if x is None:
                    x = line['bbox'][1]
                text = '.'.join([span['text'] for span in line['spans']])
                if abs(line['bbox'][1] - x) < 1:
                    lines[-1].append(text)
                else:
                    lines.append([text])
                x = line['bbox'][1]
            print(lines)
            yield lines, block


def _parse_printout_content(path: str | PathLike):
    """
    Parse the content in a protocol printout

    Returns
    -------
    model_name : str
        Name of the scanner
    software_version : str
        Version of the software
    protocols : list[(dict, dict)]
        A list of protocol, where each protocol contains
        a "title" dictionary, with keys "path" and a few others,
        and a "key-value" dictionary that contains all parameters
        in the protocol.
    """
    doc = pymupdf.open(str(path))

    prots: list = []                    # All protocols in the doc
    title: dict | None = None           # Current protocol title object
    prot: dict | None = None            # Current protocol content
    header: str | None = None           # Current header

    colx = _find_alignment(doc[0])

    model_name, software_version = _parse_model(doc[0])

    iter_blocks = peekable(_iter_blocks(doc))
    while True:
        try:
            lines, block = iter_blocks.peek()
        except StopIteration:
            if prot is not None:
                prots.append(prot)
            break

        first_line = lines[0]
        first_span = first_line[0]

        if first_span.startswith('\\\\'):
            if first_span.strip() == '\\\\USER':
                # we're in the table of contents somehow...
                break

            # Start of a new protocol (paths start with \\)
            if prot is not None:
                prots.append(prot)
            title = _parse_title(iter_blocks)
            prot = dict(Header=title)
            continue

        iter_blocks.next()

        if not ''.join(first_line).strip():
            continue
        if first_span.startswith('SIEMENS MAGNETOM'):
            # Separation between protocols
            continue
        if first_span.startswith('Page'):
            # Page number (header)
            continue
        if re.fullmatch(r'\d\d/\d\d/\d\d\d\d', first_span):
            # Date (footer)
            continue
        if first_span == 'Table of contents':
            # Table of contents is always at the end, we can stop here
            if prot is not None:
                prots.append([title, prot])
            break

        # Compute indentation size
        box = block['bbox']
        indent = abs(colx - box[0])

        for line in lines:
            if len(line) == 1 and indent < 10:
                header = line[0]
                prot.setdefault(header, {})
            elif len(line) > 1:
                prot[header][line[0]] = ''.join(line[1:]).strip()
            else:
                prot[header][line[0]] = None

    return model_name, software_version, prots


keymap = {
    'SequenceName': 'Header/SIEMENS',
    'ScanningSequence': {
        'args': ['Header/SIEMENS'],
        'formula': lambda x: {
            'fl': ['GR'],                # FLASH
            'tfl': ['GR'],               # TurboFLASH
            'epfid': ['GR', 'EP'],       # Echo-planar FID ?
            'spcir': ['IR'],             # Spoiled Inversion Recovery ?
            'epse': ['SE', 'EP'],        # Echo-planar Spin Echo
        }[x]
    },
    'SequenceVariant': {
        'args': [
            'Header/SIEMENS',
            'Routine/Phase oversampling',
        ],
        'formula': lambda x, po: (
            {
                'fl': [],                    # FLASH
                'tfl': ['SP'],               # TurboFLASH
                'epfid': ['GR', 'EP'],       # Echo-planar FID ?
                'spcir': ['IR'],             # Spoiled Inversion Recovery ?
                'epse': ['SE', 'EP'],        # Echo-planar Spin Echo
            }[x]
            + (['PO'] if int(po.split()[0]) > 0 else [])
        ) or ['NONE']
    },
    'PulseSequenceType': [
        {
            'args': ['Header/SIEMENS', 'Routine/Multi-band accel. factor'],
            'formula': lambda x, mb: ('Multiband ' if int(mb) > 1 else '') + {
                'fl': 'Gradient Echo',          # FLASH
                'tfl': 'Spiled Gradient Echo',  # TurboFLASH
                'epfid': 'Gradient Echo EPI',   # Echo-planar FID ?
                'spcir': 'FLAIR',               # Spoiled Inversion Recovery ?
                'epse': 'Spin Echo EPI',        # Echo-planar Spin Echo
            }[x]
        },
        {
            'args': ['Header/SIEMENS'],
            'formula': lambda x: {
                'fl': 'Gradient Echo',          # FLASH
                'tfl': 'Spiled Gradient Echo',  # TurboFLASH
                'epfid': 'Gradient Echo EPI',   # Echo-planar FID ?
                'spcir': 'FLAIR',               # Spoiled Inversion Recovery ?
                'epse': 'Spin Echo EPI',        # Echo-planar Spin Echo
            }[x]
        },
    ],
    'PhaseEncodingDirection': {
        'args': [
            'Routine/Orientation',
            'System/Sagittal',
            'System/Coronal',
            'System/Transversal',
        ],
        'formula': lambda x, s, c, t: {
            'Sagittal': s[0] + s[-1],
            'Transversal': {'FH': 'IS', 'HF': 'SI'}[t[0] + t[-1]],
            'Coronal': c[0] + c[-1],
        }[x]
    },
    'SliceEncodingDirection': [
        {
            'args': ['Routine/Phase enc. dir.'],
            'formula': lambda x: x[0] + x[-1],
        },
        {
            'args': ['Geometry/Phase enc. dir.'],
            'formula': lambda x: x[0] + x[-1],
        },
    ],
    'SliceThickness': {
        'args': ['Routine/Slice thickness'],
        'formula': lambda x: float(x.split()[0]),
    },
    'NumberOfAverages': {
        'args': ['Routine/Averages'],
        'formula': int,
    },
    'EchoTime': {
        'args': ['Routine/TE'],
        'formula': lambda x: float(x.split()[0]) * 1e-3,
    },
    'RepetitionTime': {
        'args': ['Routine/TR'],  # FIXME: what does TR mean in siemens?
        'formula': lambda x: float(x.split()[0]) * 1e-3,
    },
    'ReceiveCoilActiveElements': 'Routine/Coil elements',
    'InversionTime': {
        'args': ['Contrast/TI'],
        'formula': lambda x: float(x.split()[0]) * 1e-3,
    },
    'FlipAngle': {
        'args': ['Contrast/Flip angle'],
        'formula': lambda x: float(x.split()[0]),
    },
    'FatSaturation': {
        'args': ['Contrast/Fat suppr.'],
        'formula': lambda x: False if x == "None" else True,
    },
    'CoilCombinationMethod': 'System/Coil Combine Mode',
    'ParallelAcquisitionTechnique': {
        'args': ['Resolution/PAT mode'],
        'formula': lambda x: _error() if x == 'Off' else x
    },
    'ParallelReductionFactorInPlane': {
        'args': ['Resolution/Accel. factor PE'],
        'formula': lambda x: int(x)
    },
    'ParallelReductionFactorOurOfPlane': {
        'args': ['Resolution/Accel. factor 3D'],
        'formula': lambda x: int(x)
    },
    'AcqusitionMatrixSE': [
        {
            'args': ['Geometry/Slabs', 'Geometry/Slices per slab'],
            'formula': lambda slabs, slices: int(slabs) * int(slices)
        },
        {
            'args': ['Slices'],
            'formula': int,
        },
    ],
    'SliceOrder': 'Geometry/Series',
    'ImagingFrequency': {
        'args': ['System/Frequency 1H'],
        'formula': lambda x: float(x.split()[0])
    },
    'MRAcquisitionType': 'Sequence/Dimension',
    'PixelBandwidth': {
        'args': ['Bandwidth'],
        'formula': lambda x: float(x.split()[0])
    },
    'MultibandAccelerationFactor': {
        'args': ['Routine/Multi-band accel. factor'],
        'formula': int
    }
}


def _get(mapping, key):
    key = list(key.spit('/'))
    while key:
        mapping = mapping[key.pop(0)]
    return mapping


def _siemens_to_bids(prot):
    bids = {}
    for key, keymaps in _siemens_to_bids.items():
        if not isinstance(keymaps, list):
            keymaps = [keymaps]
        for keymap in keymaps:
            try:
                if isinstance(keymap, dict):
                    args = []
                    for item in keymap['args']:
                        args.append(_get(prot, item))
                    value = keymap['formula'](*args)
                else:
                    value = _get(prot, keymap)
                bids[key] = value
            except Exception:
                continue
    return bids


def parse_printout(path: str | PathLike):
    model, software, prots = _parse_printout_content(path)
    base = {
        'Manufacturer': 'Siemens',
        'ManufacturersModelName': model,
        'SoftwareVersions': software,
    }
    sidecars = {}
    for prot in prots:
        # Convert to BIDS
        sidecar = {**base, **_siemens_to_bids(prot)}
        # Fix RepetitionTime based on sequence type
        if 'EPI' in sidecar.get('PulseSequenceType', ''):
            if 'RepetitionTime' in sidecar:
                sidecar['RepetitionTimeExcitation'] = 'RepetitionTime'
                del sidecar['RepetitionTime']
        else:
            if 'RepetitionTime' in sidecar:
                sidecar['RepetitionTimePreparation'] = 'RepetitionTime'
                del sidecar['RepetitionTime']
        # Save BIDS sidecar
        sidecars[prot['Header']['path']] = sidecar
