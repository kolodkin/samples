---
title: "Anyone Can Build a Website Now: One No-Build SPA Skeleton, Three Sites, a Tight Deploy Loop"
published: false
description: "Three very different single-page apps share one no-bundler skeleton and a push-to-deploy loop. The build was never the slow part."
tags: webdev, javascript, github, showdev
series: "Anyone Can Build a Website Now"
---

Three live sites that look nothing alike:

- **[What They Mean](https://kolodkin.github.io/what-they-mean/)** — tech concepts explained to non-developers, one idea per screen
- **[PCL Viewer](https://kolodkin.github.io/samples/pcl-viewer/)** — a LiDAR point-cloud viewer that streams a KITTI drive and decodes it in the browser
- **[Archer](https://kolodkin.github.io/samples/archer/)** — a first-person wave-defense archery game in three.js

Underneath they are the same thing: a folder of plain HTML, CSS and ES modules, a Python `http.server` for local dev, Playwright tests, and a GitHub Actions workflow that deploys to Pages on every push to `main`. No bundler, no Node toolchain, no build step.

Idea to deployed page took under half an hour each time. Making the UI feel right took weeks. The skeleton is what made those weeks bearable.

## The skeleton

```
web/
  index.html      import map + one <script type="module">
  main.js         the app
  styles.css
  favicon.svg
serve.py          static server with ES-module MIME types, no caching
run.sh            ./run.sh -> http://127.0.0.1:8000
tests/            Playwright e2e
conftest.py       starts the server on a free port for the tests
.github/workflows/pages.yml
```

**No bundler.** The template ships with zero dependencies: one HTML file, one module script. When an app needs a library, bare module specifiers resolve through an import map in `index.html`, and that is the whole build system:

```html
<script type="importmap">
{
  "imports": {
    "three":        "https://unpkg.com/three@0.160.0/build/three.module.js",
    "preact":       "https://unpkg.com/preact@10.19.3/dist/preact.module.js",
    "preact/hooks": "https://unpkg.com/preact@10.19.3/hooks/dist/hooks.module.js",
    "htm":          "https://unpkg.com/htm@3.1.1/dist/htm.module.js"
  }
}
</script>
<script type="module" src="./main.js"></script>
```

Preact plus [htm](https://github.com/developit/htm) gives components without JSX, so nothing needs transpiling. The import map is the practice worth keeping; where its entries point is up to the project. A CDN works as above. The three sites point at copies downloaded into `web/vendor/` instead, which only takes the CDN out of the loop as something that can be down.

**The filesystem is the router.** What They Mean has six concept pages, each its own folder with its own `index.html`, `app.js` and `styles.css`, reached by a relative link from the menu. Styles cannot leak between demos, and adding one is `cp -r web/db web/<name>` plus a card on the menu. No JS router, no server rewrites, and relative links mean the project-path Pages URL just works.

**One flag makes the tests fast.** Every app sets `window.__APP = { ready: true }` once rendered, and the tests wait on it instead of sleeping:

```python
def test_page_loads(server_url, page, shot):
    page.goto(server_url + "/")
    page.wait_for_function("() => window.__APP && window.__APP.ready === true")
    expect(page.locator("h1")).to_have_text("Hello, world!")
    shot("home")
```

`shot("home")` is the other half. Tests screenshot at visually meaningful moments, numbered in order, so `test-results/shots/` reads as a walkthrough of the app after every run. Archer goes further: its suite plays the game through `window.__ARCHER` hooks with a seeded RNG, so runs are reproducible and the screenshots show real arrows in flight.

**Deploy: test, build, deploy.** Pull requests run the first two jobs as a check. Pushes to `main` run all three.

```yaml
jobs:
  test:
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
      - run: uv sync
      - run: uv run playwright install --with-deps chromium
      - run: uv run pytest

  build:
    needs: test
    steps:
      - uses: actions/checkout@v4
      - run: mkdir -p _site && cp -r web/. _site/
      - uses: actions/upload-pages-artifact@v3
        with: { path: _site }

  deploy:
    if: github.event_name != 'pull_request'
    needs: build
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - id: deployment
        uses: actions/deploy-pages@v4
```

The "build" is a `cp`.

One-time setup: **Settings → Pages → Source: GitHub Actions**. The site appears at `https://<owner>.github.io/<repo>/`. Pages serves one artifact per repo, so the samples repo stages both apps into it (`_site/pcl-viewer`, `_site/archer`) with a root redirect.

## The loop

```bash
./run.sh              # edit, refresh, repeat
uv run pytest         # tests + fresh screenshots
git push              # CI tests and deploys
```

Nothing to install beyond `uv` and a browser. No `node_modules`, no watcher. Refresh the tab and the edit is there, because `serve.py` sends `Cache-Control: no-store`.

## Concept to preliminary site: fast

From empty folder to a tested, documented, deployable first version:

- **PCL Viewer** — 16 minutes
- **Archer** — 24 minutes
- **What They Mean** — one day, first demo and test suite included

These apps are not trivial. The viewer decodes Draco-compressed LiDAR frames through a bounded worker queue. The game runs animated glTF characters with arrow physics, cover, and a monster radar. The speed comes from the skeleton removing every question that is not about the app. Where do files go? `web/`. How do I run it? `./run.sh`. How do I ship it? Push.

## Fine-tuning the UI: slow

The first version of every site was live within a day. Then the UI ate weeks: 24 pull requests on PCL Viewer in 8 days, 48 on Archer in a month, 25 and counting on What They Mean.

**PCL Viewer — Where should the camera start?** Bird's-eye, then low and forward-facing, then closer to the sensor, then aimed down the road, then an elevated chase-cam. Six tries for a question a user never consciously asks.

**Archer — How should the aim cue look?** A bullseye, then a dashed trajectory lane, then a marker at the impact point, then a soft warmth on the hit zone, then a brighter one. The final answer is a small point light on the patch of ground or enemy the arrow would hit, nothing drawn over the scene.

**What They Mean — How tall should a panel be?** The database demo's context pane went from half the screen to "size to content" to "cap at 50vh" in one day. The Play demo button moved three times before it settled next to the back link.

**Archer — When should things be visible?** The perch underfoot went invisible, visible, slightly translucent, then shrank and grew back until it read as a platform. On a phone the bow was clipped off the right edge, because the field of view is vertical and a portrait frustum is narrow.

These are just a few examples. Most of the pull requests were this kind of small correction, and none of them could have been planned up front. You look at the page, something is off, you change it, you look again.

## Why the tight build makes the slow part survivable

Every one of those commits was cheap, and that is the point of the build process being this small:

- **The screenshots are the review.** After `uv run pytest`, I look at a folder of PNGs, not a running app I have to click through. A camera-default change is two files side by side.
- **Every push is a deploy.** Ten minutes after a change I could send a link and ask "does the aim cue read better now?"
- **Nothing between the edit and the browser.** No stale build, no watcher to restart, no source-map mismatch. The file I edited is the file the browser ran.
- **The tests already drive the app.** When a UI element moved, the e2e that clicked it failed and the screenshot showed where it went. Archer's tests guard that the title screen quotes the real stage count.

Oscillating between "size to content" and "cap at 50vh" is not a broken process. It is what building a UI looks like. A tight loop makes each swing cost minutes instead of an afternoon.

## Steal it

The skeleton lives at [kolodkin/spa-template](https://github.com/kolodkin/spa-template): a "Hello, world" page, the server, three e2e tests with screenshots, and the test-build-deploy workflow. It is a GitHub template repository: click **Use this template**, flip Pages to "GitHub Actions" in the new repo, push, and you have a live site.

Then replace `web/` with whatever you want to make. The build will not slow you down. The UI will, and that is fine. That part is the actual work.

- [What They Mean](https://github.com/kolodkin/what-they-mean) — Preact + htm, six standalone demo folders
- [PCL Viewer](https://github.com/kolodkin/samples/tree/main/pcl-viewer) — three.js, Draco, hyparquet, streaming from a Hugging Face dataset
- [Archer](https://github.com/kolodkin/samples/tree/main/archer) — three.js, glTF characters, seeded RNG, e2e that plays the game
