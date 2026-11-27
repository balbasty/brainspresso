"""
Generate the brainspresso logos, wordmark and site icon.

The BIDS mark (half brain and file tree), file and folder glyphs are
adapted from the BIDS logo
(github.com/bids-standard/bids-specification, CC BY 4.0).

Usage: python make_logos.py [OUTDIR]   (default: docs/images)
"""
import re
import sys
from pathlib import Path

HERE = Path(__file__).parent
OUT = sys.argv[1] if len(sys.argv) > 1 else str(HERE.parent)
# BIDS mark without its lettering: half brain + file tree, 181.3 x 213.6
MARK = (HERE / 'bids_mark.txt').read_text().strip()
MARK_W, MARK_H = 181.32, 213.6
FILE = ('<polygon points="25.85 9.74 25.85 9.77 16.69 9.77 16.69 0 25.85 9.74"/>'
        '<polygon points="13.56 12.9 25.85 12.9 25.85 30.63 0 30.63 0 0 13.56 0 13.56 12.9"/>')
FILE_W, FILE_H = 25.85, 30.63
FOLDER = ('<path transform="translate(-105.6 -26.18)" d="M142.7,29.61H127.82l-.61-1.85A2.3,2.3,0,0,0,125,26.18H113.83a2.3,2.3,0,0,0-2.19,1.6l-.59,1.83h-1.83a3.65,3.65,0,0,0-3.65,3.64V55.31A3.65,3.65,0,0,0,109.22,59H142.7a3.63,3.63,0,0,0,3.63-3.64V33.25A3.63,3.63,0,0,0,142.7,29.61Zm-34.56,5.83,2.23-3H141.5l1.65,3Z"/>')
FOLDER_W, FOLDER_H = 40.73, 32.82

INK, CREMA, CREAM = '#2b1b13', '#c98b4e', '#f6ecdd'
COFFEE = '#6b3e26'
FOAM, LATTE = '#fbf5ec', '#b7783f'   # milk foam / latte surface
ATTR = ('<!-- BIDS mark, file and folder glyphs adapted from the BIDS logo '
        '(github.com/bids-standard/bids-specification, CC BY 4.0). -->')

def mark(cx, cy, h, cls):
    """The BIDS mark, centred on (cx, cy), h high."""
    s = h / MARK_H
    x, y = cx - MARK_W * s / 2, cy - h / 2
    return (f'<g class="{cls}" transform="translate({x:.2f} {y:.2f}) '
            f'scale({s:.4f})">{MARK}</g>')

def foam_mark(cx, cy, h, cls):
    """
    The BIDS mark as latte art: the gyri of the brain are filled with
    foam, so that its outline and sulci are drawn by the coffee.
    """
    lines = MARK.splitlines()
    d = re.search(r'd="([^"]+)"', lines[0]).group(1)
    gyri = ''.join(re.findall(r'M[^M]+', d)[1:])   # the holes of the outline
    branches = ''.join(f'<rect x="88" y="{y}" width="59.27" height="12.21"/>'
                       for y in (85.94, 126, 168.39))
    s = h / MARK_H
    x, y = cx - MARK_W * s / 2, cy - h / 2
    return (f'<g class="{cls}" transform="translate({x:.2f} {y:.2f}) '
            f'scale({s:.4f})"><path fill-rule="evenodd" d="{gyri}"/>'
            f'{branches}{"".join(lines[1:])}</g>')


def glyph(kind, cx, cy, w, cls, rot=0):
    body, gw, gh = (FILE, FILE_W, FILE_H) if kind == 'file' else (FOLDER, FOLDER_W, FOLDER_H)
    s = w / gw
    return (f'<g class="{cls}" transform="translate({cx:.1f} {cy:.1f}) rotate({rot}) '
            f'scale({s:.4f}) translate({-gw/2:.2f} {-gh/2:.2f})">{body}</g>')

DARK = f'''@media (prefers-color-scheme: dark) {{
      .ink{{fill:{CREAM}}} .ink-s{{stroke:{CREAM}}} .cream{{fill:{INK}}}
    }}'''

def style(extra='', themed=True):
    return f'''<style>
    .ink{{fill:{INK}}} .ink-s{{fill:none;stroke:{INK}}}
    .cream{{fill:{CREAM}}} .crema{{fill:{CREMA}}} .fixed-ink{{fill:{INK}}}
    .coffee{{fill:{COFFEE}}} .foam{{fill:{FOAM}}}
    {DARK if themed else ''}{extra}
  </style>'''

# ------------------------------------------- brainspresso (espresso, alternate)
def brainspresso(themed=True):
    cx = 230                    # cup axis (handle adds weight on the right)
    top, bot = 176, 410         # rim line / cup bottom
    hw = 136                    # half width
    body = (f'M{cx-hw} {top} H{cx+hw} V{top+64} '
            f'C{cx+hw} {bot-40} {cx+92} {bot} {cx} {bot} '
            f'C{cx-92} {bot} {cx-hw} {bot-40} {cx-hw} {top+64} Z')
    steam = ''.join(
        f'<path class="ink-s" stroke-width="16" stroke-linecap="round" d="M{x} {y0} '
        f'c-20 -22 20 -36 0 -60 c-14 -17 -4 -32 4 -40"/>'
        for x, y0 in ((cx-48, 150), (cx, 140), (cx+48, 150)))
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" width="512" height="512" role="img" aria-labelledby="title">
  <title id="title">brainspresso</title>
  {ATTR}
  {style(themed=themed)}
  <defs>
    <mask id="saucer-gap" maskUnits="userSpaceOnUse" x="0" y="0" width="512" height="512">
      <rect width="512" height="512" fill="#fff"/>
      <path d="{body}" fill="#000" stroke="#000" stroke-width="22" stroke-linejoin="round"/>
      <circle cx="{cx+hw+12}" cy="{top+92}" r="50" fill="none" stroke="#000" stroke-width="44"/>
    </mask>
  </defs>
  <g>
    {steam}
    <!-- saucer -->
    <ellipse class="ink" cx="{cx+8}" cy="{bot+18}" rx="178" ry="24" mask="url(#saucer-gap)"/>
    <!-- handle -->
    <circle class="ink-s" cx="{cx+hw+12}" cy="{top+92}" r="50" stroke-width="24"/>
    <!-- cup -->
    <path class="ink" d="{body}"/>
    <ellipse class="ink" cx="{cx}" cy="{top}" rx="{hw}" ry="24"/>
    <ellipse class="crema" cx="{cx}" cy="{top+1}" rx="{hw-18}" ry="13"/>
    <!-- BIDS mark -->
    {mark(cx - 4, top + 126, 124, "cream")}
  </g>
</svg>
'''

# ------------------------------------------------- brainspresso (latte): the logo
def brainspresso_latte(themed=True, wobble=16, soft=1.0):
    """
    A cappuccino seen from above, the BIDS mark poured in the foam.

    `wobble` (displacement of the foam edges) and `soft` (blur of the
    edges) make the art look poured rather than printed.
    """
    cx, cy = 240, 206           # centre of the rim
    rx, ry = 172, 96            # rim (a circle seen at an angle)
    tilt = ry / rx
    bot = cy + 196              # bottom of the bowl
    body = (f'M{cx-rx} {cy} C{cx-rx} {cy+120} {cx-104} {bot} {cx} {bot} '
            f'C{cx+104} {bot} {cx+rx} {cy+120} {cx+rx} {cy} Z')
    hx, hy = cx + rx + 18, cy + 62
    art = (f'<g filter="url(#poured)" transform="translate({cx} {cy+8}) '
           f'scale(1 {tilt:.3f}) translate({-cx} {-(cy+8)})">'
           f'{foam_mark(cx - 6, cy + 8, 192, "foam")}</g>')
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" width="512" height="512" role="img" aria-labelledby="title">
  <title id="title">brainspresso</title>
  {ATTR}
  {style(themed=themed)}
  <defs>
    <mask id="saucer-gap" maskUnits="userSpaceOnUse" x="0" y="0" width="512" height="512">
      <rect width="512" height="512" fill="#fff"/>
      <path d="{body}" fill="#000" stroke="#000" stroke-width="22" stroke-linejoin="round"/>
      <ellipse cx="{cx}" cy="{cy}" rx="{rx}" ry="{ry}" fill="#000" stroke="#000" stroke-width="22"/>
      <ellipse cx="{hx}" cy="{hy}" rx="40" ry="30" fill="none" stroke="#000" stroke-width="42"/>
    </mask>
    <!-- poured foam: wobbly edges, soft rim, milk spreading in the crema -->
    <filter id="poured" x="-15%" y="-15%" width="130%" height="130%">
      <feTurbulence type="fractalNoise" baseFrequency="0.016 0.024" numOctaves="2" seed="11" result="noise"/>
      <feDisplacementMap in="SourceGraphic" in2="noise" scale="{wobble}" xChannelSelector="R" yChannelSelector="G" result="wavy"/>
      <feGaussianBlur in="wavy" stdDeviation="{soft}" result="foam"/>
      <feGaussianBlur in="wavy" stdDeviation="{soft*4}" result="spread"/>
      <feComponentTransfer in="spread" result="halo"><feFuncA type="linear" slope="0.3"/></feComponentTransfer>
      <feMerge><feMergeNode in="halo"/><feMergeNode in="foam"/></feMerge>
    </filter>
    <radialGradient id="latte" cx="50%" cy="46%" r="55%">
      <stop offset="0" stop-color="#d39a5e"/>
      <stop offset=".7" stop-color="#bf7f45"/>
      <stop offset="1" stop-color="#9d5f2f"/>
    </radialGradient>
  </defs>
  <g>
    <!-- saucer -->
    <ellipse class="ink" cx="{cx+6}" cy="{bot-34}" rx="226" ry="76" mask="url(#saucer-gap)"/>
    <!-- handle -->
    <ellipse class="ink-s" cx="{hx}" cy="{hy}" rx="40" ry="30" stroke-width="22"/>
    <!-- cup: bowl, rim, latte surface -->
    <path class="ink" d="{body}"/>
    <ellipse class="ink" cx="{cx}" cy="{cy}" rx="{rx}" ry="{ry}"/>
    <ellipse class="coffee" cx="{cx}" cy="{cy+3}" rx="{rx-16}" ry="{ry-12}"/>
    <ellipse fill="url(#latte)" cx="{cx}" cy="{cy+4}" rx="{rx-26}" ry="{ry-20}"/>
    <!-- latte art: the BIDS mark in milk foam -->
    {art}
  </g>
</svg>
'''


# ------------------------------------------------------------------ site icons
# Glyph-style icons (one colour, solid shapes, a few knock-outs, as in
# Font Awesome) that read at 16-32 px. Shapes are drawn white in a mask
# and knock-outs black, so that gaps show whatever is behind the icon.
def _glyph_svg(title, shapes, themed):
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" width="512" height="512" role="img" aria-labelledby="title">
  <title id="title">{title}</title>
  {style(themed=themed)}
  <defs>
    <mask id="glyph" maskUnits="userSpaceOnUse" x="0" y="0" width="512" height="512">
      {shapes}
    </mask>
  </defs>
  <rect class="ink" width="512" height="512" mask="url(#glyph)"/>
</svg>
'''


def brainspresso_icon(themed=True):
    """A cappuccino cup seen from above, on its saucer."""
    cx, cy = 236, 178           # centre of the rim
    rx, ry = 196, 98
    bot = cy + 214
    bowl = (f'M{cx-rx} {cy} C{cx-rx} {cy+138} {cx-118} {bot} {cx} {bot} '
            f'C{cx+118} {bot} {cx+rx} {cy+138} {cx+rx} {cy} Z')
    hx, hy = cx + rx + 6, cy + 74
    W, K = 'fill="#fff"', 'fill="#000"'
    shapes = f'''<ellipse cx="{cx+10}" cy="{bot-38}" rx="236" ry="80" {W}/>
      <path d="{bowl}" {K} stroke="#000" stroke-width="40" stroke-linejoin="round"/>
      <ellipse cx="{hx}" cy="{hy}" rx="50" ry="40" fill="none" stroke="#000" stroke-width="72"/>
      <ellipse cx="{hx}" cy="{hy}" rx="50" ry="40" fill="none" stroke="#fff" stroke-width="34"/>
      <path d="{bowl}" {W}/>
      <ellipse cx="{cx}" cy="{cy}" rx="{rx}" ry="{ry}" {W}/>
      <ellipse cx="{cx}" cy="{cy+4}" rx="{rx-34}" ry="{ry-30}" {K}/>
      <ellipse cx="{cx}" cy="{cy+8}" rx="{rx-66}" ry="{ry-52}" {W}/>'''
    return _glyph_svg('brainspresso', shapes, themed)


# ------------------------------------------------------------------- wordmark
def _wordmark_text(text):
    """Outlines of `text` in Inter ExtraBold (see outline_text.py)."""
    for line in (HERE / 'wordmark.txt').read_text().splitlines():
        name, upm, advance, bounds, d = line.split('\t')
        if name == text:
            return int(upm), float(advance), [float(v) for v in bounds.split()], d
    raise KeyError(text)


def brainspresso_wordmark(themed=True):
    """The latte logo, and "brainspresso" in the ink of the logo."""
    # the latte logo, cropped to its drawing (saucer, rim and handle)
    lx, ly, lw, lh = 14, 102, 474, 348
    H = 200                                  # height of the logo
    logo_w = lw * H / lh
    logo = brainspresso_latte(themed=False)
    logo = logo.replace('viewBox="0 0 512 512" width="512" height="512"',
                        f'viewBox="{lx} {ly} {lw} {lh}" x="0" y="0" '
                        f'width="{logo_w:.1f}" height="{H}"', 1)
    for ref in ('saucer-gap', 'poured', 'latte'):
        logo = logo.replace(f'id="{ref}"', f'id="wm-{ref}"')
        logo = logo.replace(f'url(#{ref})', f'url(#wm-{ref})')
    logo = logo.replace(' role="img" aria-labelledby="title"', '')
    logo = re.sub(r'<title id="title">[^<]*</title>', '', logo)
    if themed:                               # the cup follows the palette
        logo = re.sub(r'<style>.*?</style>', '', logo, flags=re.S)
    # the name: x-height centred on the coffee, ascenders near the rim
    upm, advance, (x0, y0, x1, y1), d = _wordmark_text('brainspresso')
    size = 116                               # font size
    k = size / upm
    gap = 26
    base = H * 0.66                          # baseline
    tx = logo_w + gap - x0 * k
    width = tx + x1 * k + 8
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width:.0f} {H}" width="{width:.0f}" height="{H}" role="img" aria-labelledby="title">
  <title id="title">brainspresso</title>
  {ATTR}
  <!-- Lettering: Inter ExtraBold (rsms.me/inter, SIL OFL 1.1), as outlines. -->
  {style(themed=themed)}
  {logo}
  <path class="ink" transform="translate({tx:.2f} {base:.2f}) scale({k:.5f})" d="{d}"/>
</svg>
'''


if __name__ == '__main__':
  open(f'{OUT}/brainspresso-logo.svg', 'w').write(brainspresso_latte())
  open(f'{OUT}/brainspresso-logo-espresso.svg', 'w').write(brainspresso())
  open(f'{OUT}/brainspresso-icon.svg', 'w').write(brainspresso_icon())
  open(f'{OUT}/brainspresso-wordmark.svg', 'w').write(brainspresso_wordmark())
