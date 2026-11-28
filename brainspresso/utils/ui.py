def human2bytes(x: str | int) -> int:
    """
    Convert human byte size (3MB, 2GB, etc) into a number of bytes
    """
    if isinstance(x, int):
        return x
    if not isinstance(x, str):
        raise TypeError('Expected an int or a stirng')
    x = x.strip()
    unit = ''
    while x[-1] in 'ptgmkbPTGMKB':
        unit = x[-1] + unit.upper()
        x = x[:-1]
    if unit and unit[-1] == 'B':
        unit = unit[:-1]
    unit = 1024**({'': 0, 'K': 1, 'M': 2, 'G': 3, 'T': 4, 'P': 5}[unit])
    x = int(float(x.strip())*unit)
    return x

