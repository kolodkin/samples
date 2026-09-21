---
title: "Agentic Flow for a Simple SPA Anyone Can Build"
published: false
description: "Three single-page apps, 97 agent pull requests, one no-build structure. Getting the first version up was minutes. Steering the UI took weeks."
tags: webdev, ai, javascript, showdev
series: "Anyone Can Build a Website Now"
---

I've got three sites live that look nothing like each other, and I didn't hand-write any of them. An agent did, across 97 pull requests that I reviewed and merged.

- **[What They Mean](https://kolodkin.github.io/what-they-mean/)** — tech concepts explained to non-developers, one idea per screen
- **[PCL Viewer](https://kolodkin.github.io/samples/pcl-viewer/)** — a LiDAR point-cloud viewer that streams a KITTI drive and decodes it in the browser
- **[Archer](https://kolodkin.github.io/samples/archer/)** — a first-person wave-defense archery game in three.js

Under the hood they're the same thing: a folder of plain HTML, CSS and ES modules, a Python `http.server` for local dev, Playwright tests, and a GitHub Actions workflow that deploys to Pages on every push to `main`. No bundler, no Node toolchain, no build step.

Getting your own is a two-minute job. [kolodkin/spa-template](https://github.com/kolodkin/spa-template) is a GitHub template, so hit **Use this template** — or say to your agent:

> create an SPA GitHub Pages repo based on https://github.com/kolodkin/spa-template

Getting each one to a first deployed page took minutes. Steering the UI to something I actually liked took weeks. This post is about the flow that made the second part survivable.

## The structure

```
web/
  index.html      import map + one <script type="module">
  main.js         exports init(); replace with your app
  styles.css
  favicon.svg
serve.py          static server with ES-module MIME types, no caching
run.sh            ./run.sh -> http://127.0.0.1:8000
tests/            Playwright e2e
conftest.py       starts the server on a free port for the tests
.github/workflows/pages.yml
```

**No bundler.** Libraries are resolved by an import map in `index.html`, and that's the whole build system. Here's what PCL Viewer adds for three.js, for example:

```html
<script type="importmap">
{
  "imports": {
    "three":         "https://unpkg.com/three@0.160.0/build/three.module.js",
    "three/addons/": "https://unpkg.com/three@0.160.0/examples/jsm/"
  }
}
</script>
```

All three sites also use Preact with [htm](https://github.com/developit/htm), which gives you components without JSX, so there's nothing to transpile. That's my choice for these projects, not something the template cares about. Put whatever you like in the map.

**The filesystem is the router.** What They Mean's six concept pages are six folders, each with its own `index.html`, `app.js` and `styles.css`, linked from the menu by a relative path. No JS router, no server rewrites, and no way for one demo's styles to leak into another. Adding a page is `cp -r web/db web/<name>` plus a card on the menu.

**One flag keeps the tests fast.** Every app sets `window.__APP = { ready: true }` once it has rendered, and the tests wait on that instead of sleeping:

```python
def test_page_loads(server_url, page, shot):
    page.goto(server_url + "/")
    page.wait_for_function("() => window.__APP && window.__APP.ready === true")
    expect(page.locator("h1")).to_have_text("Hello, world!")
    shot("home")
```

That `shot("home")` is the other half of the trick. Tests take a screenshot whenever they reach a state worth seeing, numbered in order, so after every run `test-results/shots/` reads like a walkthrough of the app. Archer takes this further: its suite actually plays the game through `window.__ARCHER` hooks with a seeded RNG, so every run is reproducible and the screenshots show real arrows in flight.

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

Yes, the "build" is a `cp`.

The one-time setup is **Settings → Pages → Source: GitHub Actions**, and the site shows up at `https://<owner>.github.io/<repo>/`. Pages only serves one artifact per repo, so the samples repo stages both apps into it (`_site/pcl-viewer` and `_site/archer`) with a root redirect.

## The loop

```bash
./run.sh              # edit, refresh, repeat
uv run pytest         # tests + fresh screenshots
git push              # CI tests and deploys
```

There's nothing to install beyond `uv` and a browser. No `node_modules`, no watcher. You refresh the tab and your edit is there, because `serve.py` sends `Cache-Control: no-store`.

With an agent driving, one round is: I describe the change, it works on a branch, runs the suite, and opens a pull request. I look at the screenshots that run produced, and either merge or say what's still wrong. That's it. Every one of the 97 merged pull requests across these three repos came in on an agent branch, and I never once had to read a diff to find out what the page now looked like.

The screenshots are why this works. Reviewing an agent's UI change by reading its code is slow and unreliable, and spinning the app up myself defeats the point of delegating. A folder of numbered PNGs from the run I'm reviewing answers the only question I have, which is whether it looks right yet.

## Concept to preliminary site: fast

From an empty folder to a tested, documented, deployable first version:

- **PCL Viewer** — 16 minutes
- **Archer** — 24 minutes
- **What They Mean** — one day, first demo and test suite included

These aren't toy apps. The viewer decodes Draco-compressed LiDAR frames through a bounded worker queue. The game runs animated glTF characters with arrow physics, cover, and a monster radar. What makes them fast to start is that the structure has already answered every question that isn't about the app itself. Where do files go? `web/`. How do I run it? `./run.sh`. How do I ship it? Push.

## Steering the UI: slow

Every one of these sites was live within a day. Then the UI ate weeks: 24 pull requests on PCL Viewer in 8 days, 48 on Archer in a month, 25 and counting on What They Mean.

An agent builds exactly what you specify. Nobody knows quite what they want until they see it, so most of those pull requests carry one round of review feedback.

**PCL Viewer — Where should the camera start?** Bird's-eye at first. Then low and forward-facing. Then closer to the sensor. Then aimed down the road. Then an elevated chase-cam. Six tries to settle a question no user ever consciously asks.

**Archer — How should the aim cue look?** It started as a bullseye, became a dashed trajectory lane, then got a marker at the impact point, then a soft warmth on the hit zone, then a brighter one. Where it ended up is a small point light on the exact patch of ground or enemy the arrow would hit, with nothing drawn over the scene at all.

**What They Mean — How tall should a panel be?** The database demo's context pane went from half the screen to "size to content" to "cap at 50vh", all in one day. The Play demo button moved three times before it settled next to the back link.

Those are just a few examples. Most of the 97 were this kind of small correction, and I couldn't have specified any of them up front. You look at the page, something's off, you say so, you look again.

## Why the tight build makes the slow part survivable

Going back and forth on a panel height a few times in one day might look like indecision. It's just what it takes to get a UI right. The agent makes each try cost minutes instead of an afternoon, so you can afford to keep going until it feels right — which is the whole trick, because taste is the one part you can't delegate.

Feel free to reach out with any questions, in the comments or directly. Happy to go deeper on any part of this.
