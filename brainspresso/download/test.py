"""
Manual check: download the IXI metadata file into a temporary folder.

    python -m brainspresso.download.test
"""

import tempfile
from .manager import DownloadManager
from .downloader import Downloader

URLBASE = 'http://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI'
URLS = {
    'T1w': [f'{URLBASE}/IXI-T1.tar'],
    'T2w': [f'{URLBASE}/IXI-T2.tar'],
    'PDw': [f'{URLBASE}/IXI-PD.tar'],
    'MRA': [f'{URLBASE}/IXI-MRA.tar'],
    'DTI': [
        f'{URLBASE}/IXI-DTI.tar',
        f'{URLBASE}/bvecs.txt',
        f'{URLBASE}/bvals.txt',
    ],
    'meta': [f'{URLBASE}/IXI.xls'],
}

if __name__ == "__main__":
    odir = tempfile.gettempdir()
    print(odir)

    manager = DownloadManager(
        # Downloader(URLS['T1w'][0], odir, ifnochecksum='continue'),
        # Downloader(URLS['T2w'][0], odir, ifnochecksum='continue'),
        Downloader(URLS['meta'][0], odir, ifnodigest='continue'),
        on_error='raise',
        ifexists='refresh',
    )
    manager.run()
