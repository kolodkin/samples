Archer — technical spec
---

All gameplay tuning — arrow types, enemy stats, wave compositions, stage
speed multipliers, drop rates, score bonuses — lives in `web/config.js`.
This spec covers behavior the numbers don't show.

## Controls

| Input | Action |
|-------|--------|
| Click on the canvas (unlocked) | Acquire pointer lock — never fires |
| Mouse move (pointer lock) | Aim |
| Left click (pointer lock) | Shoot at the set power |
| HUD +/− buttons (left middle), or +/− keys | Adjust shot power (`CONFIG.bow.power`: min/max/step) |
| Esc (exits pointer lock) | Pause |
| Touch: drag on the canvas | Aim (no pointer lock; one finger owns the camera) |
| Touch: 🏹 button | Shoot at the set power |
| Touch: ❚❚ button | Pause |

Shot power is a persistent setting (it survives firing); the crosshair ring,
the 🏹 button ring, and the bow-string pull all show the current level.
Firing plays a release cycle on the bow viewmodel (`CONFIG.bow.shot`): the
string snaps forward and the nocked arrow vanishes (it became the
projectile), the bow sits empty for a beat, then a fresh arrow appears and
rides the string back out to the power draw. Shots are gated on that fresh
arrow (`Player.canShoot()`), so clicks mid-reload are swallowed.

The released projectile spawns visually at the nocked-arrow tip on the bow
and converges onto the aim line over ~0.12 s (`SPAWN_BLEND` in
`web/arrows.js`); physics always runs on the aim line, so accuracy is
unaffected. Every arrow also drags a fading tracer trail colored by its
type — first-person shots fly straight away from the eye, so without the
trail the arrow reads as a shrinking dot instead of an arc. The
dotted trajectory hint is visible below 85% power and fades as power rises,
so full-power shots stay skill-based; it integrates at the arrow's own
frame step and ends at the impact point — the ground, or the first
blocking obstacle. Only deliberate fire
inputs shoot: a click while pointer-locked on desktop, the 🏹 button on
touch. Stray clicks or taps elsewhere on the screen never loose an arrow.

There is no manual arrow selection: every shot automatically spends the
strongest special in stock — exploding, then freezing, then burning, the
declaration order of `CONFIG.arrow.types` — and falls back to the infinite
basic arrow once the quiver is dry. The HUD quiver is a read-only stock
display; the highlighted slot is what the next shot will fire.

Touch mode is detected via `(pointer: coarse)` or the first `touchstart`;
hybrid devices keep both input paths live. Touch aim sensitivity is mouse
sensitivity × `CONFIG.touch.lookScale`.

A monster radar sits in the HUD's top-left corner (above the HP bar): a
player-centered minimap rotated so "up" is the aim direction, with enemy
blips tinted like their meshes (ogres draw bigger). Contacts beyond
`CONFIG.radar.range` pin to the rim at half opacity, so an incoming wave
reads as bearings before it closes. The canvas lives in the HUD (Preact),
but `web/radar.js` redraws it imperatively every frame — like the
`--power` CSS variable, per-frame enemy motion is too chatty for the UI
store.

## Combat

- Headshots deal double damage plus a score bonus; kills in quick
  succession chain a multi-kill combo bonus.
- A fraction of kills drop a floating pickup — shoot it to collect.
  Usually it's a random special arrow type (glowing octahedron); a
  config-tunable fraction (`CONFIG.drops.heal`) is a heal potion (corked
  flask) that restores HP, clamped at max.
- Exploding splash ignores cover (no LOS check); freezing doubles the
  next hit (shatter); burning spreads to nearby enemies.
- Arrow collision is segment-vs-sphere per frame (no tunneling at 70 m/s).
- Obstacles (trees, cacti, rocks, ice pillars) block arrows in flight —
  the player's and skeleton projectiles alike. Each obstacle is a
  collision cylinder (`{radius, height}` from its maker in
  `web/stages.js`); `obstacleHit()` in `web/geom.js` clips the frame's
  travel segment at the first impact, so an enemy peeking in front of
  cover is still hittable while anything behind it is shielded, and
  arcing shots clear low cover. Exploding arrows detonate on the
  obstacle, so splash remains the counter to hidden enemies.

## Enemies

- **Goblin** — weaving melee rush.
- **Ogre** — slow tank.
- **Skeleton archer** — advances into range, hides behind the nearest
  obstacle on the player line, peeks to shoot, hides again (cover/peek
  point selection: `pickCover()`/`coverPoint()` in `web/enemies.js`).
  Projectiles drop at 4 m/s² with compensated aim, so long shots arc
  visibly; like player arrows they use segment-vs-sphere collision, so
  they cannot tunnel through the player even at low frame rates.
- Melee ignores the perch elevation: attacks reach the player from the
  perch base by design.
- A melee attacker is spent on contact: it lands one hit and disappears
  (no score, no drop), so every monster that leaks through costs HP
  exactly once.

## Stages and waves

Forest (dense cover) → desert (long sightlines) → iceberg (snow, fastest
enemies), five waves each per `CONFIG.stages`. HP refills between stages;
special ammo carries over. Death → retry the same stage with the ammo held
at its start. Best score/stage persist in `localStorage['archer.best']`.

The ground is a vertex-displaced, vertex-colored, flat-shaded plane
(`makeGround()` in `web/stages.js`): two-tone noise patches plus per-facet
tint jitter give a texture-gradient depth cue, and per-theme hills (dunes /
ridges) rise beyond the play area to frame the horizon inside the fog band.
The battlefield itself stays a flat y=0 plane (micro-relief ≤ ±0.12) —
arrows die at y<=0.05, enemies walk at y=0, and melee/cover logic assume
it. Terrain noise is hash-based on vertex position and deliberately does
NOT draw from the seeded rng stream, so per-seed obstacle layouts (which
tests pin) are unaffected.

Props and the player's gear share the same low-poly texturing recipe via
`web/relief.js` (`texturedMesh()`: hash-noise vertex relief + two-tone
mottling + flat shading): tree bark and boughs, cactus ribs, sandstone
rocks, ice crags, the bow's grained wood and leather grip, and arrow
shafts (a `grainY` stretch elongates the noise for lengthwise wood
grain). Monsters are deliberately NOT textured — smooth flat tints keep
them visually distinct from the terrain and props they move through.
The same no-rng rule applies — obstacle makers derive their texture seeds
from values already drawn (e.g. trunk height), never from fresh draws, so
the pinned layouts don't shift. Arrow fletching is three flat swept vanes
(`fletching()` in `web/arrows.js`), unlit so the ammo-type color stays
readable.

The sun is a shadow-casting directional light plus a fog-exempt disc and
halo in the sky. Ground, props, monsters, arrows, skeleton projectiles and
pickups all cast/receive (the bow viewmodel does not). The shadow camera
is a ±48 ortho box over the arena at 512²; `normalBias` handles
flat-shaded acne. Gotcha: geometries that receive shadows MUST keep their
`normal` attribute — deleting it (the old "dead weight" optimization)
silently disables shadow reception in three r160's Lambert path. Tree
canopies also sway in the wind: the stage handle owns the motion
(`animate(t)`, built by `buildStage()`), and the main tick just calls it —
visual only; collision radii and cover points stay static.

## Determinism and e2e

All gameplay randomness flows through one seeded mulberry32 stream
(`web/rng.js`, `?seed=N`); particles are visual-only and exempt.
`window.__ARCHER` (defined in `web/main.js`) exposes `ready`, a `state`
snapshot (screen, hp, score, wave, enemies, pickups, obstacles, best,
yaw/pitch/touch/power, nocked/canShoot, arrows with physics vs visual
positions and trail length), and test hooks: `fireAt()` (gravity-compensated),
`spawnEnemy()` (optional `inert` flag disables the AI), `skipToWave()`,
`killAll()`, `giveAmmo()`, `setDropChance()`, `setHealChance()`,
`setPlayerHp()`, `start()`,
`nextStage()`, `retryStage()`, `visiblePixelCount()`. Tests boot with
`?autostart=1&seed=42&waves=0` for a clean battlefield and park inert
target dummies at melee reach to avoid leading moving targets (live melee
enemies there would strike once and vanish); touch tests dispatch
synthetic `TouchEvent`s on the canvas (Chromium's `new Touch()`).

## Known simplifications

- No player movement; no sound.
- Stage geometry is regenerated (not disposed) on stage change — a small,
  bounded leak over a 3-stage run. The ground mesh is the exception: it is
  built once per theme and reused across loads.
