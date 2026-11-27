"""
Generate the brainspresso and brainfilter logos.

The brain, file and folder glyphs are adapted from the BIDS logo
(github.com/bids-standard/bids-specification, CC BY 4.0).

Usage: python make_logos.py [OUTDIR]   (default: docs/images)
"""
import sys
from pathlib import Path

HERE = Path(__file__).parent
OUT = sys.argv[1] if len(sys.argv) > 1 else str(HERE.parent)
BRAIN = (HERE / 'bids_brain.txt').read_text().strip()          # 0..102.7 x 0..213.6
BRAIN_W, BRAIN_H = 102.7, 213.6
FILE = ('<polygon points="25.85 9.74 25.85 9.77 16.69 9.77 16.69 0 25.85 9.74"/>'
        '<polygon points="13.56 12.9 25.85 12.9 25.85 30.63 0 30.63 0 0 13.56 0 13.56 12.9"/>')
FILE_W, FILE_H = 25.85, 30.63
FOLDER = ('<path transform="translate(-105.6 -26.18)" d="M142.7,29.61H127.82l-.61-1.85A2.3,2.3,0,0,0,125,26.18H113.83a2.3,2.3,0,0,0-2.19,1.6l-.59,1.83h-1.83a3.65,3.65,0,0,0-3.65,3.64V55.31A3.65,3.65,0,0,0,109.22,59H142.7a3.63,3.63,0,0,0,3.63-3.64V33.25A3.63,3.63,0,0,0,142.7,29.61Zm-34.56,5.83,2.23-3H141.5l1.65,3Z"/>')
FOLDER_W, FOLDER_H = 40.73, 32.82

INK, CREMA, CREAM = '#2b1b13', '#c98b4e', '#f6ecdd'
PAPER, PAPER_DARK = '#f1e2cb', '#cfb28c'   # unbleached filter paper
ATTR = ('<!-- Brain, file and folder glyphs adapted from the BIDS logo '
        '(github.com/bids-standard/bids-specification, CC BY 4.0). -->')

def brain(cx, top, h, cls):
    s = h / BRAIN_H
    x = cx - BRAIN_W * s / 2
    return (f'<path class="{cls}" transform="translate({x:.2f} {top:.2f}) scale({s:.4f})" '
            f'd="{BRAIN}"/>')

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
    }}'''

def style(extra='', themed=True):
    return f'''<style>
    .ink{{fill:{INK}}} .ink-s{{fill:none;stroke:{INK}}}
    .cream{{fill:{CREAM}}} .paper{{fill:{PAPER}}} .crema{{fill:{CREMA}}} .fixed-ink{{fill:{INK}}}
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
    <!-- BIDS half brain -->
    {brain(cx, top+32, 182, "cream")}
  </g>
</svg>
'''

# ---------------------------------------------------------------- brainfilter
def brainfilter(themed=True):
    cx = 248
    rim_y, base_y = 84, 292
    rt, bt = 170, 44             # half widths of the cone at rim / base
    cone = f'M{cx-rt} {rim_y} L{cx+rt} {rim_y} L{cx+bt} {base_y} L{cx-bt} {base_y} Z'
    # pinked top edge of the paper filter, above the rim
    pw, n = rt - 14, 14
    pts = []
    for i in range(n + 1):
        x = cx - pw + 2 * pw * i / n
        y = 40 if i % 2 == 0 else 54
        pts.append(f'{x:.1f} {y}')
    paper = (f'M{cx-pw+6} {rim_y} L' + ' L'.join(pts) + f' L{cx+pw-6} {rim_y} Z')
    # handle: loop on the right side of the cone
    def wall(y):                 # x of the right wall of the cone at height y
        return cx + rt - (rt - bt) * (y - rim_y) / (base_y - rim_y)
    y1, y2 = rim_y + 34, rim_y + 126
    handle = (f'M{wall(y1)-6:.1f} {y1} C{wall(y1)+64:.1f} {y1-6} {wall(y1)+70:.1f} {y2-8} '
              f'{wall(y2)-4:.1f} {y2}')
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" width="512" height="512" role="img" aria-labelledby="title">
  <title id="title">brainfilter</title>
  {ATTR}
  {style(themed=themed)}
  <g stroke-linejoin="round">
    <!-- handle -->
    <path class="ink-s" stroke-width="22" stroke-linecap="round" d="{handle}"/>
    <!-- paper filter (seen through the glass dripper) -->
    <path class="paper" d="{cone}"/>
    <path class="paper" d="{paper}"/>
    <path class="ink-s" stroke-width="10" d="{paper}"/>
    <!-- dripper: cone outline, rim and base -->
    <path class="ink-s" stroke-width="16" d="{cone}"/>
    <rect class="ink" x="{cx-rt-14}" y="{rim_y-11}" width="{2*rt+28}" height="22" rx="11"/>
    <rect class="ink" x="{cx-bt-40}" y="{base_y-4}" width="{2*bt+80}" height="20" rx="10"/>
    <!-- BIDS half brain, printed on the filter -->
    {brain(cx, rim_y+24, 170, "fixed-ink")}
    <!-- what drips through: coffee, files and folders -->
    {drop(cx, 334, 34, "crema")}
    {glyph("file", cx-24, 386, 34, "ink", -10)}
    {drop(cx+26, 414, 26, "crema")}
    {glyph("folder", cx-2, 462, 50, "ink", 7)}
  </g>
</svg>
'''

if __name__ == '__main__':
  open(f'{OUT}/brainspresso-logo.svg', 'w').write(brainspresso())
  open(f'{OUT}/brainfilter-logo.svg', 'w').write(brainfilter())
