# Artwork

| File | What |
|------|------|
| `brainspresso-logo.svg` | The logo: a cappuccino with the BIDS mark poured in the foam |
| `brainspresso-wordmark.svg` | The logo and the name (the heading of the README) |
| `brainspresso-wordmark-light.svg`, `brainspresso-wordmark-dark.svg` | The wordmark in the light and dark palettes only (the title of the docs site, which picks the one of its palette) |
| `brainspresso-logo-espresso.svg` | An alternate logo: an espresso cup with the BIDS mark |
| `brainspresso-icon.svg` | The site icon (one colour, legible at 16-32 px), also the favicon of the docs site |
| `brainspresso-favicon-32.png`, `brainspresso-icon-180.png` | PNG fallbacks of the site icon (favicon, Apple touch icon) |
| `brainspresso-workflow.svg` | The diagram of harvest, roast, grind and percolate |
| `overrides/.icons/brainspresso/icon.svg` | The site icon as a theme icon: the logo in the header of the docs site (`brainspresso/icon` in `zensical.toml`) |

The SVGs follow `prefers-color-scheme` (light-on-dark in dark mode), except the
diagram, which has a background of its own, and the light and dark wordmarks.
The docs site has its own palette toggle, so it shows one of these two instead
of following the browser. The theme icon has no colour of its own: the theme
draws it in the colour of the header text.

## Regenerating

* `python docs/images/src/make_logos.py`: the logos, wordmarks, icon and theme
  icon;
* `python docs/images/src/make_diagram.py`: the diagram;
* `node docs/images/src/render_pngs.js`: the PNGs (needs Node and Playwright);
* `python docs/images/src/outline_text.py`: the outlines of the lettering
  (`wordmark.txt`; needs fontTools and Inter), only to change the name or
  the font.

## Credits

* The BIDS mark (half brain and file tree), file and folder glyphs are
  adapted from the [BIDS logo](https://github.com/bids-standard/bids-specification/tree/master/BIDS_logo)
  ([CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)).
* The lettering is [Inter](https://rsms.me/inter/) ExtraBold
  ([SIL OFL 1.1](https://openfontlicense.org)), as outlines.
