Preview Clip Spec
---

How the archer promo clip (`archer-gameplay.gif` / `.mp4`) is produced: a
scripted Playwright session drives the real game through its `window.__ARCHER`
test hooks and the real input path, Chromium records the page, and ffmpeg
speeds up and trims the result.

Nothing is faked — every arrow is loosed by a `mousedown` on the canvas under
pointer lock, so the bow draw, the dashed trajectory lane and the shot all
agree with where the camera points.

## Shot list

Five stages, ~3 s each after the speed-up, three shots per stage, one signature
effect per arena. Stage order matches the game's own progression.

| # | Stage   | Ammo pinned | Beat                                        |
|---|---------|-------------|---------------------------------------------|
| 1 | meadow  | normal      | opening pan onto a goblin war band, headshots |
| 2 | forest  | lightning   | bolt chains between skeleton archers + goblins |
| 3 | desert  | burning     | ogre set alight, fire spreads to neighbours  |
| 4 | iceberg | freezing    | ogre frozen mid-stride, ice tint + snowburst |
| 5 | volcano | exploding   | ground bursts thin a charging pack           |

Deliberate constraints, learned from earlier cuts:

- **One explosion in the whole clip** (the volcano finale). Explosions on every
  stage read as noise.
- **No sky shots.** The camera stays level; a lob that tracks up into empty sky
  wastes the viewer's attention.
- **No pop-in.** Enemies are spawned in the *same tick* as the stage cut, so
  each segment's first rendered frame already has its monsters on the field.
- **No target dummies.** Every enemy is live and advancing. The `inert` flag of
  `spawnEnemy` guarantees hits but makes the game look dead.

## Setup

```bash
cd archer && bash vendor.sh          # ESM deps, incl. GLTFLoader for the models
```

Serve `web/` with `serve.py` on a spare port, then drive it with Playwright
Chromium. In a container without a bundled browser download, launch with
`executable_path="/opt/pw-browsers/chromium"`; record via
`new_context(record_video_dir=..., record_video_size={"width":1280,"height":720})`.

Boot URL — deterministic, menu-skipping, wave spawning off so every beat is
staged by hand:

```
/?autostart=1&seed=42&stage=meadow&waves=0
```

Then `page.mouse.click(640, 360)` to acquire pointer lock (this also clears the
click-to-aim hint), and two `Equal` presses to raise the bow to 80 % power.

## Aiming

The hard part. Four things must be right or the arrows miss.

**1. Arrow speed is not proportional to power.** From `config.js`:

```
speed = bow.minSpeed + (bow.maxSpeed - bow.minSpeed) * power   # 20 + 75 * power
```

At 60 % power a naive `108 * power` happens to agree, which hides the bug until
the power changes. Flight time is `horizontal_distance / speed`.

**2. Ballistic hold-over.** Gravity is `-15`. Aim at

```
pitch = atan2(dy + 0.5 * 15 * t², horiz),  yaw = atan2(-dx, -dz)
```

from the player camera at `(0, 3.2, 34)`.

**3. Lead the advance, never the weave.** Sample the target twice ~130 ms apart
to get its velocity, then extrapolate **only `z`** over the flight time. Goblins
zigzag; a momentary lateral velocity says nothing about where they will be a
third of a second later, and projecting it throws the shot wide. Their weave
averages out around their current `x`. Iterate the extrapolation ~3× — flight
time depends on the lead distance, which depends on flight time.

**4. Close the loop on the engine, not on your own bookkeeping.** `state.yaw` /
`state.pitch` expose the real camera; dispatched `movementX` deltas are
coarsened in transit, so dead reckoning drifts. Read the angles back and fold in
the residual. Before releasing, `state.enemies[i].highlighted` tells you the
trajectory hint is on that enemy — i.e. the engine agrees the shot connects.

Measured hit rate went from 2/5 to 4/5 once (1) and (3) were fixed.

Aim heights: goblin `1.25` (head — 34 × 2 one-shots its 40 hp), skeleton `1.35`,
ogre `1.9` (body centre). Volcano bursts aim at the pack's leading edge at
`y ≈ 0.4`; splash forgives the weave.

## Camera motion

Headless Chromium renders slowly — measure it, don't assume. An empty stage
runs ~15 fps; a full field of animated monsters has been as low as **4.7 fps**
(~214 ms per frame) since the pathfinding and flank-obstacle work landed.
Measure before a session:

```python
fps = page.evaluate("""() => new Promise(res => { let n = 0; const t0 = performance.now();
  const tick = () => { n++; if (performance.now() - t0 < 3000) requestAnimationFrame(tick);
                       else res(n / 3); };
  requestAnimationFrame(tick); })""")
```

A mouse-move step only becomes visible camera motion if it lands in its own
rendered frame; steps closer together pile into one frame and read as a
**teleport** — the source of every "glitch" in review. `FRAME = 68` is the
step spacing, but what actually governs smoothness is how many rendered frames
the whole turn spans and how far the camera moves in each.

**The rule that survives fps changes:**

```
turn_wall_duration × speed_factor ≈ 300 ms on screen
```

300 ms is the figure that was signed off. Turn duration is `steps × FRAME`, so
when the capture gets slower (forcing a higher speed-up to hold the runtime),
add steps to compensate — don't just re-encode faster, or the turns snap again.
Worked example: at 12 steps a turn is 816 ms of wall clock; encoded at 2.7×
that is 302 ms on screen. At 9 steps (610 ms) the matching speed-up is 2.0×.

Other rules:

- Steps are **eased** (smoothstep `t²(3-2t)`), so a turn accelerates out of rest
  and settles onto the target. A linear pan of the same duration still reads as
  a machine snapping between angles.
- Corrections are spread over 2–3 steps, never dumped into one event, and
  residuals under ~1.5 px are dropped entirely.
- The **opening** pan gets double the steps and a still hold before it, because
  it is the first thing the viewer sees with no context.

## Pacing

The bow's own cadence is fast — `bow.shot` is snap 0.024 + reload 0.12 + nock
0.06 ≈ **0.2 s**. Any longer gap between shots is script overhead, not the game.
Every `page.evaluate` costs a rendered frame, which is expensive at 5–15 fps, so:

- Fetch enemies + yaw + pitch + ready flag in **one** round trip per shot.
- Sample enemy velocity **once per stage**, not per shot — every walker closes
  at the same rate.
- No idle pause between releases; the next shot waits only on `state.canShoot`.

That took the on-screen cadence from one arrow every ~4 s to a continuous
string.

Spawn distances matter too: with three-shot strings, spawning inside ~z = 12
lets the pack reach the perch mid-volley (the camera ends up buried in a
goblin). Spawn around `z = 1..12` and let them close.

## Encode

Raw capture is a 1280×720 VP8 `.webm`. Speed-up is essential — the headless
capture runs slow — but pick the factor from the turn-duration rule above, not
by taste, and add pan steps if a slower capture forces a higher factor.

Trim per stage rather than dropping shots from the script. The recorder logs
`CUTS` (the timestamp of every in-place stage switch) alongside `SHOTS`, so the
boundaries come from the run itself instead of being hunted frame by frame:

```python
SEGS = [(cut + open_hold, last_shot + tail)
        for cut, last_shot in zip(CUTS, LAST_SHOT_PER_STAGE)]
# open_hold ~2.6 s on the first stage (settle before the opening pan), ~0.6 s after
# each cut; tail ~1.6 s to let an impact read, ~3 s for the volcano bursts.
```

Each segment runs from just after the stage cut through the last kept shot's
impact. Splices land on stage boundaries, so they read as the hard cuts already
in the clip. Spot-check a boundary frame — it must be a settled frame of the new
stage, not mid-pan.

Drop shots per stage (three → two) to hit a length target; that is cheaper than
re-recording and leaves the motion untouched. Target ~26 s.

```bash
# concat the segments, then speed up (setpts=1/factor, e.g. 0.37 for 2.7x)
ffmpeg -i raw.webm -filter_complex \
  "[0:v]trim=12.27:26.02,setpts=PTS-STARTPTS[s0]; ... \
   [s0][s1][s2][s3][s4]concat=n=5:v=1:a=0[cat];[cat]setpts=0.37*PTS[out]" \
  -map "[out]" -r 30 -c:v libx264 -preset slow -crf 21 \
  -pix_fmt yuv420p -movflags +faststart -an archer-gameplay.mp4

# GIF via a two-pass palette (single-pass quantisation bands the sky badly)
ffmpeg -i archer-gameplay.mp4 -vf \
  "fps=12,scale=520:-1:flags=lanczos,palettegen=stats_mode=diff:max_colors=144" palette.png
ffmpeg -i archer-gameplay.mp4 -i palette.png -lavfi \
  "fps=12,scale=520:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle" \
  archer-gameplay.gif
```

Target: ~26 s, GIF ≈ 3.7 MB at 520 px / 12 fps, MP4 ≈ 2.4 MB.

## Posting note

LinkedIn renders *uploaded* GIF files as static images (animated GIFs only via
the Giphy picker in comments). Upload the MP4 for the feed — it autoplays and
gives the GIF experience — and keep the GIF for places that animate it. Test in
a draft post before committing to either.
