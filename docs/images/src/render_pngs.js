// Render PNG fallbacks of the site icons (the SVGs are the sources).
//   node docs/images/src/render_pngs.js        (needs Node and Playwright)
// * <name>-favicon-32.png : 32 px, transparent, light palette
// * <name>-icon-180.png   : 180 px Apple touch icon, cream background
const fs = require('fs');
const os = require('os');
const path = require('path');
const { chromium } = require('playwright');

const DIR = path.resolve(__dirname, '..');
const CREAM = '#f6ecdd';

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ colorScheme: 'light' });
  for (const name of ['brainspresso']) {
    const svg = 'file://' + path.join(DIR, `${name}-icon.svg`);
    for (const [size, bg, pad, file] of [
      [32, 'transparent', 0, `${name}-favicon-32.png`],
      [180, CREAM, 22, `${name}-icon-180.png`],
    ]) {
      await page.setViewportSize({ width: size, height: size });
      // a file:// page, so that it may load the file:// SVG
      const html = path.join(os.tmpdir(), `render-${name}-${size}.html`);
      fs.writeFileSync(html,
        `<body style="margin:0;background:${bg}">` +
        `<img src="${svg}" style="display:block;margin:${pad}px;` +
        `width:${size - 2 * pad}px;height:${size - 2 * pad}px"></body>`);
      await page.goto('file://' + html);
      await page.waitForFunction(() => document.images[0].complete &&
                                       document.images[0].naturalWidth > 0);
      fs.unlinkSync(html);
      await page.screenshot({ path: path.join(DIR, file),
                              omitBackground: bg === 'transparent' });
    }
  }
  await browser.close();
})();
