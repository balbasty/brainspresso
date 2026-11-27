"""
Generate the brainspresso workflow diagram (harvest, roast, grind, percolate).

Usage: python make_diagram.py [OUTDIR]   (default: docs/images)
"""
import math
import sys
from pathlib import Path

from make_logos import CREAM, CREMA, INK, brainspresso_latte, glyph

OUT = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).parent.parent)
SANS = "Inter, 'Helvetica Neue', Helvetica, Arial, sans-serif"
MONO = "ui-monospace, 'SF Mono', Menlo, Consolas, 'Liberation Mono', monospace"
BG, CARD, LINE, MUTED, ROAST = '#fbf6ee', '#ffffff', '#e7d8c3', '#8a6a52', '#6b3e26'
CHERRY, LEAF = '#b8432f', '#6f7f3c'
W, H = 1200, 800

def text(x, y, s, size=15, weight=400, fill=INK, anchor='start', font=SANS, style='', ls=0):
    s = s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    extra = f' font-style="{style}"' if style else ''
    extra += f' letter-spacing="{ls}"' if ls else ''
    return (f'<text x="{x}" y="{y}" font-family="{font}" font-size="{size}" '
            f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}"{extra}>{s}</text>')

def pill(x, y, s, size=15):
    w = len(s) * size * 0.6 + 28
    return (f'<rect x="{x}" y="{y}" width="{w:.0f}" height="34" rx="8" fill="{INK}"/>'
            + text(x + 14, y + 22.5, s, size, 500, CREAM, font=MONO))

def out_chip(x, y, s):
    w = len(s) * 14 * 0.6 + 52
    return (f'<rect x="{x}" y="{y}" width="{w:.0f}" height="32" rx="16" fill="#f3e6d3"/>'
            + glyph('folder', x + 19, y + 16, 18, 'g-ink')
            + text(x + 36, y + 21, s, 14, 600, ROAST, font=MONO))

def arrow(x1, y1, x2, y2, dashed=False, color=INK, width=3):
    dash = ' stroke-dasharray="6 6"' if dashed else ''
    return (f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{width}"'
            f' stroke-linecap="round"{dash} marker-end="url(#head)"/>')

# ---------------------------------------------------------------- icons
def icon_harvest(cx, cy):
    """A coffee sprig: opposite leaves, cherries clustered on the stem."""
    green, vein = '#3f6b2e', '#7da35a'
    ripe = {'r': '#b8322a', 'd': '#8f2420', 'o': '#d7762e', 'y': '#d9a93a',
            'g': '#8fa548'}

    def leaf(x, y, rot, size=1.0):
        L, w = 46 * size, 17 * size
        return (f'<g transform="translate({x} {y}) rotate({rot})">'
                f'<path d="M0 0 C{L*.25} {-w} {L*.75} {-w} {L} 0 '
                f'C{L*.75} {w} {L*.25} {w} 0 0Z" fill="{green}"/>'
                f'<path d="M3 0 L{L-6} 0" stroke="{vein}" stroke-width="2" '
                f'stroke-linecap="round"/></g>')

    def cherry(nx, ny, dx, dy, colour):
        rot = math.degrees(math.atan2(dx, -dy))
        return (f'<g transform="translate({nx+dx} {ny+dy}) rotate({rot:.0f})">'
                f'<ellipse rx="9" ry="10.5" fill="{ripe[colour]}"/>'
                f'<circle cy="-8" r="2.2" fill="#3a1d14" opacity=".6"/>'
                f'<ellipse cx="-3.2" cy="-1" rx="2.2" ry="3.4" fill="#fff" '
                f'opacity=".45"/></g>')

    def cluster(nx, ny, spec):
        offsets = [(-11, -9), (0, -14), (11, -9), (-11, 9), (0, 14), (11, 9)]
        return ''.join(cherry(nx, ny, dx, dy, c)
                       for (dx, dy), c in zip(offsets, spec) if c != '-')

    return (f'<g transform="translate({cx} {cy})">'
            f'<path d="M-92 10 C-40 2 30 -2 92 -14" fill="none" '
            f'stroke="{ROAST}" stroke-width="6" stroke-linecap="round"/>'
            + leaf(-66, 6, -128) + leaf(-66, 6, 128)
            + leaf(66, -8, -58, .9) + leaf(66, -8, 52, .9)
            + leaf(90, -13, -10, .55)
            + cluster(-30, 4, 'rdrgro')
            + cluster(24, -4, 'ryrdog')
            + '</g>')


def icon_roast(cx, cy):
    def bean(x, y, rot):
        return (f'<g transform="translate({x} {y}) rotate({rot})">'
                f'<ellipse rx="19" ry="27" fill="{ROAST}"/>'
                f'<path d="M0 -24 C-9 -10 9 10 0 24" stroke="{CREAM}" stroke-width="3.5" fill="none" stroke-linecap="round"/></g>')
    return bean(cx - 34, cy + 6, -28) + bean(cx + 4, cy - 8, 12) + bean(cx + 38, cy + 12, 40)

def icon_grind(cx, cy):
    return (f'<g transform="translate({cx} {cy})">'
            f'<path d="M-30 -14 L30 -14 L22 -34 L-22 -34 Z" fill="{INK}"/>'            # hopper
            f'<rect x="-34" y="-12" width="68" height="54" rx="6" fill="{ROAST}"/>'      # body
            f'<rect x="-14" y="10" width="28" height="18" rx="3" fill="{INK}"/>'         # drawer
            f'<line x1="0" y1="-34" x2="0" y2="-46" stroke="{INK}" stroke-width="5"/>'
            f'<line x1="0" y1="-46" x2="46" y2="-46" stroke="{INK}" stroke-width="5" stroke-linecap="round"/>'
            f'<circle cx="46" cy="-46" r="8" fill="{CREMA}"/>'
            + ''.join(f'<circle cx="{x}" cy="{y}" r="2.6" fill="{ROAST}"/>'
                      for x, y in ((-46, 40), (-52, 30), (48, 36), (54, 26), (44, 18)))
            + '</g>')

def icon_moka(cx, cy):
    return (f'<g transform="translate({cx} {cy})">'
            f'<path d="M-24 4 L24 4 L32 44 L-32 44 Z" fill="{INK}"/>'                     # boiler
            f'<rect x="-22" y="-2" width="44" height="8" fill="{ROAST}"/>'                 # waist
            f'<path d="M-22 -2 L22 -2 L30 -44 L-30 -44 Z" fill="{INK}"/>'                  # upper chamber
            f'<path d="M-32 -44 L32 -44 L28 -52 L-28 -52 Z" fill="{ROAST}"/>'              # lid
            f'<circle cx="0" cy="-56" r="5" fill="{CREMA}"/>'
            f'<path d="M-30 -42 L-44 -50 L-28 -30 Z" fill="{INK}"/>'                       # spout
            f'<path d="M28 -36 C48 -34 50 -6 26 -8" fill="none" stroke="{ROAST}" stroke-width="8" stroke-linecap="round"/>'
            + ''.join(f'<path d="M{x} -66 c-6 -7 6 -11 0 -18" stroke="{MUTED}" stroke-width="3" fill="none" stroke-linecap="round"/>'
                      for x in (-10, 10))
            + '</g>')

# ---------------------------------------------------------------- cards
def card(x, y, w, h, fill=CARD):
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="18" fill="{fill}" stroke="{LINE}" stroke-width="1.5"/>'

def stage(x, n, name, metaphor, cmd, desc, out, icon):
    y, w = 120, 340
    cx = x + w / 2
    g = [card(x, y, w, 400), icon(cx, y + 74)]
    g.append(f'<circle cx="{x+34}" cy="{y+34}" r="15" fill="{CREMA}"/>' + text(x + 34, y + 39.5, str(n), 15, 700, CARD, 'middle'))
    g.append(text(x + 28, y + 168, name, 24, 800, INK, ls=2.5))
    g.append(text(x + 28, y + 192, metaphor, 15, 400, MUTED, style='italic'))
    g.append(pill(x + 28, y + 208, cmd))
    for i, line in enumerate(desc):
        g.append(text(x + 28, y + 272 + 22 * i, line, 15, 400, INK))
    g.append(text(x + 28, y + 366, 'writes', 12, 600, MUTED, ls=1))
    g.append(out_chip(x + 80, y + 345, out))
    return ''.join(g)

parts = []
parts.append(f'<rect width="{W}" height="{H}" rx="24" fill="{BG}"/>')
# header with the (unthemed) brainspresso mark
mark = brainspresso_latte(themed=False)
mark = mark.replace('width="512" height="512"', 'x="30" y="14" width="96" height="96"', 1)
for ref in ('saucer-gap', 'poured', 'latte'):    # ids of the nested logo
    mark = mark.replace(f'id="{ref}"', f'id="dg-{ref}"')
    mark = mark.replace(f'url(#{ref})', f'url(#dg-{ref})')
mark = mark.replace('id="title"', 'id="mark-title"').replace('aria-labelledby="title"', 'aria-hidden="true"')
parts.append(mark)
parts.append(text(132, 62, 'brainspresso', 28, 800))
parts.append(text(132, 86, 'From a public dataset to preprocessed derivatives, one command per step.', 16, 400, MUTED))

stages = [
    (40, 1, 'HARVEST', 'pick the cherries', 'bx <ds> harvest',
     ["Download the dataset's source data", 'from its hosts: S3, NITRC, XNAT,', 'Girder, OpenNeuro, …'],
     'sourcedata/', icon_harvest),
    (430, 2, 'ROAST', 'roast the beans', 'bx <ds> roast',
     ['Convert the source data into a raw', 'BIDS dataset: images, JSON sidecars,', 'participants.tsv, phenotype/.'],
     'rawbids/', icon_roast),
    (820, 3, 'GRIND', 'grind for the brew', 'bx <ds> grind',
     ['Preprocess the raw BIDS dataset with', 'standard pipelines, in containers,', 'per subject or session.'],
     'derivatives/<pipeline>/', icon_grind),
]
for s in stages:
    parts.append(stage(*s))
for x in (380, 770):
    parts.append(arrow(x + 8, 320, x + 40, 320))

# ---------------------------------------------------------------- on disk
x, y = 40, 560
parts.append(card(x, y, 340, 200))
parts.append(text(x + 28, y + 38, 'ON DISK', 14, 800, INK, ls=2))
parts.append(text(x + 108, y + 38, 'study layout', 14, 400, MUTED, style='italic'))
tree = [('<root>/<DS>/', ''), ('├─ sourcedata/', '1'), ('├─ rawbids/', '2'),
        ('├─ derivatives/<pipeline>/', '3'), ('└─ .brainspresso/', 'plans')]
for i, (p, tag) in enumerate(tree):
    yy = y + 70 + 22 * i
    parts.append(text(x + 28, yy, p, 14, 500, INK, font=MONO))
    if tag:
        parts.append(text(x + 312, yy, tag, 13, 700, CREMA, 'end'))
parts.append(text(x + 28, y + 182, '--layout flat: the raw dataset is <DS>/ itself', 12.5, 400, MUTED))

# ---------------------------------------------------------------- percolate
x, y, w = 430, 560, 730
parts.append(card(x, y, w, 200, '#f6ead8'))
parts.append(icon_moka(x + 66, y + 112))
tx = x + 132
parts.append(text(tx, y + 44, 'PERCOLATE', 24, 800, INK, ls=2.5))
parts.append(text(tx, y + 68, 'brew a saved plan, here or on the cluster', 15, 400, MUTED, style='italic'))
parts.append(pill(tx, y + 82, 'bx percolate PLAN'))
parts.append(text(tx, y + 150, 'Runs the tasks of a roast or grind plan', 15, 400))
parts.append(text(tx, y + 172, 'that are not done yet. --status follows them.', 15, 400))
# fan-out: plan -> tasks -> backends
fx = x + 500
parts.append(glyph('file', fx, y + 95, 30, 'g-ink'))
parts.append(text(fx, y + 136, 'plan', 12, 600, MUTED, 'middle'))
for i, lab in enumerate(('sub-01', 'sub-02', 'sub-…')):
    ty = y + 52 + 42 * i
    parts.append(f'<path d="M{fx+20} {y+95} C{fx+40} {y+95} {fx+34} {ty+14} {fx+52} {ty+14}" fill="none" stroke="{MUTED}" stroke-width="2"/>')
    parts.append(f'<rect x="{fx+54}" y="{ty}" width="76" height="28" rx="6" fill="{CARD}" stroke="{LINE}" stroke-width="1.5"/>')
    parts.append(text(fx + 92, ty + 19, lab, 13, 600, INK, 'middle', MONO))
    parts.append(f'<line x1="{fx+130}" y1="{ty+14}" x2="{fx+150}" y2="{y+95}" stroke="{MUTED}" stroke-width="2"/>')
parts.append(f'<rect x="{fx+150}" y="{y+45}" width="74" height="100" rx="10" fill="{INK}"/>')
for i, lab in enumerate(('local', 'slurm', 'sge', 'pbs')):
    parts.append(text(fx + 187, y + 68 + 21 * i, lab, 13, 600, CREAM, 'middle', MONO))

# dashed hand-off from roast and grind
for sx in (600, 990):
    parts.append(arrow(sx, 526, sx, 552, dashed=True, color=MUTED, width=2.5))
    parts.append(text(sx + 12, 544, '--plan PLAN', 12.5, 600, MUTED, font=MONO))

svg = f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-labelledby="dg-title dg-desc">
  <title id="dg-title">brainspresso workflow</title>
  <desc id="dg-desc">Harvest downloads source data into sourcedata/, roast converts it to a raw BIDS dataset in rawbids/, grind preprocesses it into derivatives/. Roast and grind can save a plan with --plan; bx percolate PLAN runs its tasks locally or as Slurm, SGE or PBS jobs.</desc>
  <!-- File and folder glyphs adapted from the BIDS logo (bids-standard/bids-specification, CC BY 4.0). -->
  <style>.g-ink{{fill:{ROAST}}}</style>
  <defs>
    <marker id="head" viewBox="0 0 10 10" refX="7" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
      <path d="M1 1 L8 5 L1 9" fill="none" stroke="context-stroke" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
    </marker>
  </defs>
  {"".join(parts)}
</svg>
'''
open(f'{OUT}/brainspresso-workflow.svg', 'w').write(svg)
