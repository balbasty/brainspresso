import logging
from importlib import import_module

logging.basicConfig(format='%(levelname)s | %(message)s', level=15)


# discover usable datasets
_datasets = [
    'ABIDE.I', 'ABIDE.II',
    'ADHD200',
    # 'ADNI.I',
    'COBRE',
    'CoRR',
    'GSP',
    'IXI',
    'OASIS.I', 'OASIS.II', 'OASIS.III',
    'UNCWisconsin',
    'Bullitt',
    "OpenNeuro",
]
for _dataset in _datasets:
    try:
        import_module('.datasets.' + _dataset, package='brainspresso')
    except Exception as _e:
        # Do not crash the whole CLI, but say why the command is missing
        logging.getLogger(__name__).warning(
            f'Dataset {_dataset} is unavailable '
            f'[{type(_e).__name__}: {_e}]'
        )
