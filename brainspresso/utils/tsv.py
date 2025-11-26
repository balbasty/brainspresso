class TableMapper:
    """
    Utility to recode tables.

    We follow BIDS conventions for missing values: all missing values
    are coded "n/a"; therefore empty strings, float('nan'), and all
    flavours of n/a strings ("NA", "N/A", "NAN", "NaN", "nan") are
    converted to "n/a" by default.
    """

    class Converter:

        NANs = {
            '', 'n/a', 'N/A', 'nan', 'NaN', 'NAN', 'na', 'NA', float('nan')
        }

        def __new__(cls, converter, *args, **kwargs):
            if isinstance(converter, cls):
                return converter
            return super().__new__(cls)

        def __init__(self, converter={}, fallback=None):
            self.converter = converter
            self.fallback = fallback

        def __call__(self, elem):
            if elem in self.NANs:
                return 'n/a'
            if callable(self.converter):
                return self.converter(elem)
            elif isinstance(self.converter, dict):
                if elem in self.converter:
                    return self.converter[elem]
                elif self.fallback is None:
                    return elem
                else:
                    return self.fallback
            elif self.converter:
                raise TypeError(
                    f"Don't know what to do with {type(self.converter)}"
                )
            else:
                return elem

    header: dict[str, str] = {}
    row: dict[str, Converter] = {}

    @classmethod
    def remap_row(cls, header, row, fallback=None):
        old_header = list(header)
        old_row = list(row)
        new_row = []
        for new_head, old_head in cls.header.items():
            old_elem = old_row[old_header.index(old_head)]
            convert = cls.Converter(
                cls.row.get(new_head, None), fallback=fallback
            )
            new_row.append(convert(old_elem))
        return new_row

    @classmethod
    def remap(cls, rows, header=True):
        rows = iter(rows)
        old_header = next(rows)
        if header:
            yield list(cls.header.keys())
        for row in rows:
            yield cls.remap_row(old_header, row)
