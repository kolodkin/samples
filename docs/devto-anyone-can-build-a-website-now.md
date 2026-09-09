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
  vendor/         pinned ES-module builds (gitignored, fetched by vendor.sh)
serve.py          static server with ES-module MIME types, no caching
run.sh            ./run.sh -> http://127.0.0.1:8000
tests/            Playwright e2e
conftest.py       starts the server on a free port for the tests
.github/workflows/pages.yml
```

**No bundler.** Bare module specifiers resolve through an import map in `index.html`. This is the whole build system:

```html
<script type="importmap">
{
  "imports": {
    "three":        "./vendor/three.module.js",
    "preact":       "./vendor/preact.module.js",
    "preact/hooks": "./vendor/preact-hooks.module.js",
    "htm":          "./vendor/htm.module.js"
  }
}
</script>
<script type="module" src="./main.js"></script>
```

`vendor.sh` is a few `curl` lines that download pinned builds into `web/vendor/`:

```bash
THREE=0.160.0; PREACT=10.19.3; HTM=3.1.1
fetch "https://unpkg.com/three@$THREE/build/three.module.js"    "$DEST/three.module.js"
fetch "https://unpkg.com/preact@$PREACT/dist/preact.module.js"  "$DEST/preact.module.js"
fetch "https://unpkg.com/htm@$HTM/dist/htm.module.js"           "$DEST/htm.module.js"
```

Preact plus [htm](https://github.com/developit/htm) gives components without JSX, so nothing needs transpiling. The browser fetches libraries from my own server, never a CDN: tests run offline and the deployed site has no third-party runtime dependency.

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

**Deploy: test, build, deploy, tag.** Pull requests run the first two jobs as a check. Pushes to `main` run all three.

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
      - run: |
          tag="deploy-$(date -u +%Y-%m-%d-%H%M%S)"
          git tag -a "$tag" -m "Pages deployment" "$GITHUB_SHA" && git push origin "$tag"
```

The "build" is a `cp`. The tag makes `git tag -l 'deploy-*'` the deploy history, which answers "what was live on Tuesday?"

One-time setup: **Settings → Pages → Source: GitHub Actions**. The site appears at `https://<owner>.github.io/<repo>/`. Pages serves one artifact per repo, so the samples repo stages both apps into it (`_site/pcl-viewer`, `_site/archer`) with a root redirect.

## The loop

```bash
./run.sh              # edit, refresh, repeat
uv run pytest         # tests + fresh screenshots
git push              # CI tests, deploys, tags
```

Nothing to install beyond `uv` and a browser. No `node_modules`, no watcher. Refresh the tab and the edit is there, because `serve.py` sends `Cache-Control: no-store`.

## Concept to preliminary site: fast

The commit timestamps tell it better than I can.

**PCL Viewer**, June 19: scaffold and static server at 04:04. Three.js viewer, page, styles and a sample PCD at 04:11. Preact control panel and stats overlay at 04:15. Playwright e2e at 04:17. Runner script and README at 04:20. Sixteen minutes from empty folder to a tested, documented, deployable point-cloud viewer.

**What They Mean**, June 23: initial commit, the first concept demo ("What is an API?") and the Playwright suite, all in one day.

**Archer**, July 7: scaffold with a boot test at 09:59. Seeded stage builders at 10:00. First-person player with a bow at 10:01. Ballistic arrows at 10:02. Goblins and ogres at 10:04. Skeleton archers with cover-seeking AI at 10:06. Exploding, freezing and burning arrows at 10:08. Waves, stage progression, a Preact HUD with title, pause and game-over screens, then README and SPEC at 10:23. Twenty-four minutes.

These apps are not trivial. The viewer decodes Draco-compressed LiDAR frames through a bounded worker queue. The game runs animated glTF characters with arrow physics, cover, and a monster radar. The speed comes from the skeleton removing every question that is not about the app. Where do files go? `web/`. How do I run it? `./run.sh`. How do I ship it? Push.

## Fine-tuning the UI: slow

The first version of every site was live within a day. Then the UI ate weeks: over a hundred commits on PCL Viewer between June 19 and 26, roughly 150 on Archer from July 7 to August 7, 95 and counting on What They Mean.

The log shows what "fine-tuning" actually means. Rarely one big decision. The same small thing, over and over, until it stops bothering you.

**Where should the camera start?** PCL Viewer, one afternoon: bird's-eye view. "Low forward-facing onboard camera instead of bird's-eye." "Move onboard camera in toward the sensor origin." "Set onboard camera direction to (0, -0.5, 0.5)." "Aim onboard camera forward and down the road." Days later: "elevated chase-cam default view." Six commits for a question a user never consciously thinks about.

**How should the aim cue look?** Archer, two days: dashed trajectory lane instead of a bullseye. Tighten the dash pattern. Mark the impact point with a gentle bullseye. Soften it to a hit-zone warmth. Make the warmth actually visible. Tint it yellow-white on ground hits. Each step was a playtest reaction. The final version is a small point light warming the patch of ground or enemy the arrow would hit, nothing drawn over the scene at all.

**How tall should a panel be?** What They Mean's database demo: "size context pane to content, not half the screen." Same day: "cap the context pane at 50vh." The Play demo button moved to the top explanation, then "just below the All concepts link," then into a right-aligned header column with the back link. The menu got a "Learn more" expander and lost it the same day.

**When should things be visible?** The archer's perch went from invisible to visible to "slightly translucent"; its radius shrank from 2.2 to 1.4, then grew back to 1.6 because it vanished underfoot. The title screen copy was reworked, bolded, cut down to only the controls line matching your input device, then enlarged on desktop. On a phone the bow was clipped off the right edge, because a 70° field of view is vertical and a portrait frustum is narrow.

None of this was planned. You cannot plan it. You look at the page, something is off, you change it, you look again.

## Why the tight build makes the slow part survivable

Every one of those commits was cheap, and that is the point of the build process being this small:

- **The screenshots are the review.** After `uv run pytest`, I look at a folder of PNGs, not a running app I have to click through. A camera-default change is two files side by side.
- **Every push is a deploy.** Ten minutes after a change I could send a link and ask "does the aim cue read better now?", and the `deploy-*` tag says exactly what they saw.
- **Nothing between the edit and the browser.** No stale build, no watcher to restart, no source-map mismatch. The file I edited is the file the browser ran.
- **The tests already drive the app.** When a UI element moved, the e2e that clicked it failed and the screenshot showed where it went. Archer's tests guard that the title screen quotes the real stage count.

Oscillating between "size to content" and "cap at 50vh" is not a broken process. It is what building a UI looks like. A tight loop makes each swing cost minutes instead of an afternoon.

## Steal it

The skeleton lives at [kolodkin/spa-template](https://github.com/kolodkin/spa-template): a "Hello, world" page, the server, three e2e tests with screenshots, and the test-build-deploy-tag workflow. Clone it, flip Pages to "GitHub Actions," push, and you have a live site with a deploy history.

Then replace `web/` with whatever you want to make. The build will not slow you down. The UI will, and that is fine. That part is the actual work.

- [What They Mean](https://github.com/kolodkin/what-they-mean) — Preact + htm, six standalone demo folders
- [PCL Viewer](https://github.com/kolodkin/samples/tree/main/pcl-viewer) — three.js, Draco, hyparquet, streaming from a Hugging Face dataset
- [Archer](https://github.com/kolodkin/samples/tree/main/archer) — three.js, glTF characters, seeded RNG, e2e that plays the game
