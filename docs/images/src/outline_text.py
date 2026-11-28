"""
Convert the wordmark text to SVG outlines (wordmark.txt), so that the
wordmark renders the same everywhere, without the font.

Font: Inter ExtraBold (rsms.me/inter, SIL Open Font License 1.1).
Needs fontTools; only run it to change the text or the font.

Usage: python outline_text.py [FONT.otf]
"""
import sys
from pathlib import Path

from fontTools.pens.boundsPen import BoundsPen
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont

HERE = Path(__file__).parent
FONT = sys.argv[1] if len(sys.argv) > 1 else \
    '/usr/share/fonts/opentype/inter/Inter-ExtraBold.otf'
TEXTS = ('brainspresso',)
TRACKING = -0.02                 # letter spacing, in em


def outline(font, text):
    """SVG path of `text` (y down, baseline at 0) and its advance."""
    cmap, glyphs = font.getBestCmap(), font.getGlyphSet()
    hmtx = font['hmtx']
    pen = SVGPathPen(glyphs)
    bounds = BoundsPen(glyphs)
    x, track = 0, TRACKING * font['head'].unitsPerEm
    for ch in text:
        name = cmap[ord(ch)]
        for p in (pen, bounds):
            glyphs[name].draw(TransformPen(p, (1, 0, 0, -1, x, 0)))
        x += hmtx[name][0] + track
    return pen.getCommands(), x - track, bounds.bounds


font = TTFont(FONT)
lines = []
for text in TEXTS:
    d, advance, (x0, y0, x1, y1) = outline(font, text)
    lines.append(f'{text}\t{font["head"].unitsPerEm}\t{advance}\t'
                 f'{x0} {y0} {x1} {y1}\t{d}')
(HERE / 'wordmark.txt').write_text('\n'.join(lines) + '\n')
