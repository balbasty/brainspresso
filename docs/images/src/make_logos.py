"""
Generate the brainspresso and brainfilter logos.

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
PAPER, PAPER_DARK = '#f1e2cb', '#cfb28c'   # unbleached filter paper
GLASS, GLASS_DARK = '#e6eceb', '#2a3438'   # coffee server
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

def drop(cx, cy, h, cls):
    # teardrop: circle of radius r with a pointed top, height h
    r = h * 0.36
    return (f'<path class="{cls}" d="M{cx} {cy - h/2} '
            f'C{cx + r*0.35} {cy - h/2 + h*0.28} {cx + r} {cy + h/2 - 1.55*r} {cx + r} {cy + h/2 - r} '
            f'A{r} {r} 0 0 1 {cx - r} {cy + h/2 - r} '
            f'C{cx - r} {cy + h/2 - 1.55*r} {cx - r*0.35} {cy - h/2 + h*0.28} {cx} {cy - h/2}Z"/>')

DARK = f'''@media (prefers-color-scheme: dark) {{
      .ink{{fill:{CREAM}}} .ink-s{{stroke:{CREAM}}} .cream{{fill:{INK}}} .paper{{fill:{PAPER_DARK}}}
      .glass{{fill:{GLASS_DARK}}}
    }}'''

def style(extra='', themed=True):
    return f'''<style>
    .ink{{fill:{INK}}} .ink-s{{fill:none;stroke:{INK}}}
    .cream{{fill:{CREAM}}} .paper{{fill:{PAPER}}} .crema{{fill:{CREMA}}} .fixed-ink{{fill:{INK}}}
    .glass{{fill:{GLASS}}} .coffee{{fill:{COFFEE}}} .foam{{fill:{FOAM}}}
    {DARK if themed else ''}{extra}
  </style>'''

# ---------------------------------------------------------------- brainspresso
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

# ---------------------------------------------------------------- brainfilter
def brainfilter(themed=True):
    cx = 248
    # dripper
    rim_y, base_y = 70, 236
    rt, bt = 148, 40             # half widths of the cone at rim / base
    cone = (f'M{cx-rt} {rim_y} L{cx+rt} {rim_y} L{cx+bt} {base_y} '
            f'L{cx-bt} {base_y} Z')
    pw, n = rt - 12, 12          # pinked top edge of the paper filter
    pts = [f'{cx - pw + 2 * pw * i / n:.1f} {30 if i % 2 == 0 else 43}'
           for i in range(n + 1)]
    paper = f'M{cx-pw+5} {rim_y} L' + ' L'.join(pts) + f' L{cx+pw-5} {rim_y} Z'

    def wall(y):                 # x of the right wall of the cone at height y
        return cx + rt - (rt - bt) * (y - rim_y) / (base_y - rim_y)
    y1, y2 = rim_y + 28, rim_y + 104
    handle = (f'M{wall(y1)-6:.1f} {y1} C{wall(y1)+58:.1f} {y1-6} '
              f'{wall(y1)+62:.1f} {y2-8} {wall(y2)-4:.1f} {y2}')
    # server (glass coffee pot) the dripper sits on
    mt, bb = 258, 484            # mouth / bottom
    m, b = 80, 124               # half widths of the mouth / the belly
    server = (f'M{cx-m} {mt} C{cx-b-4} {mt+46} {cx-b-12} {mt+116} '
              f'{cx-b} {bb-40} C{cx-b+4} {bb-10} {cx-b+24} {bb} {cx-b+56} {bb} '
              f'H{cx+b-56} C{cx+b-24} {bb} {cx+b-4} {bb-10} {cx+b} {bb-40} '
              f'C{cx+b+12} {mt+116} {cx+b+4} {mt+46} {cx+m} {mt} Z')
    lip = (f'M{cx-m-26} {mt+2} L{cx-m-4} {mt-8} H{cx+m+6} '
           f'a9 9 0 0 1 0 18 H{cx-m-4} Z')
    level = bb - 62              # coffee level
    s_handle = (f'M{cx+m+30} {mt+34} C{cx+b+58} {mt+30} {cx+b+62} {mt+150} '
                f'{cx+b+2} {mt+156}')
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" width="512" height="512" role="img" aria-labelledby="title">
  <title id="title">brainfilter</title>
  {ATTR}
  {style(themed=themed)}
  <defs>
    <clipPath id="server-inside"><path d="{server}"/></clipPath>
  </defs>
  <g stroke-linejoin="round">
    <!-- server: handle, glass, coffee -->
    <path class="ink-s" stroke-width="22" stroke-linecap="round" d="{s_handle}"/>
    <path class="glass" d="{server}"/>
    <g clip-path="url(#server-inside)">
      <rect class="coffee" x="{cx-b-20}" y="{level}" width="{2*b+40}" height="{bb-level+10}"/>
      <rect class="crema" x="{cx-b-20}" y="{level}" width="{2*b+40}" height="9"/>
    </g>
    <path class="ink-s" stroke-width="14" d="{server}"/>
    <path d="M{cx-b+26} {mt+64} C{cx-b+16} {mt+96} {cx-b+16} {mt+130} {cx-b+26} {mt+156}"
          fill="none" stroke="#fff" stroke-width="9" stroke-linecap="round" opacity=".75"/>
    <!-- what drips through, inside the glass: coffee, files, folders -->
    {drop(cx, mt+26, 22, "crema")}
    {glyph("file", cx-20, mt+64, 24, "ink", -10)}
    {drop(cx+22, mt+92, 18, "crema")}
    {glyph("folder", cx-2, mt+128, 34, "ink", 7)}
    <path class="ink" d="{lip}"/>
    <!-- dripper: handle, paper filter, cone, rim, base -->
    <path class="ink-s" stroke-width="20" stroke-linecap="round" d="{handle}"/>
    <path class="paper" d="{cone}"/>
    <path class="paper" d="{paper}"/>
    <path class="ink-s" stroke-width="9" d="{paper}"/>
    <path class="ink-s" stroke-width="14" d="{cone}"/>
    <rect class="ink" x="{cx-rt-12}" y="{rim_y-10}" width="{2*rt+24}" height="20" rx="10"/>
    <rect class="ink" x="{cx-bt-34}" y="{base_y-4}" width="{2*bt+68}" height="18" rx="9"/>
    <!-- BIDS mark, printed on the filter -->
    {mark(cx, rim_y + 76, 96, "fixed-ink")}
  </g>
</svg>
'''


# ---------------------------------------------------------- brainspresso (latte)
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


if __name__ == '__main__':
  open(f'{OUT}/brainspresso-logo.svg', 'w').write(brainspresso())
  open(f'{OUT}/brainfilter-logo.svg', 'w').write(brainfilter())
  open(f'{OUT}/brainspresso-logo-latte.svg', 'w').write(brainspresso_latte())
