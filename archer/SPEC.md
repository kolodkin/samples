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
| Quiver slot click/tap, or keys 1–6 | Select ammo: ✨ Auto or a specific arrow type |
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

Ammo selection is a mode picked on the HUD quiver (click/tap a slot, or
keys 1–6 in slot order under pointer lock). The default ✨ Auto slot rides
the player's accuracy: a shot that damaged at least one enemy — a direct
strike, or an exploding arrow's splash — arms it, and the next shot spends
the best special in stock, strongest-first in the `CONFIG.arrow.types`
declaration order (exploding → lightning → freezing → burning);
`CONFIG.arrow.autoMissLimit` (3) consecutive shots that hurt nobody
(ground, cover, timeout, or a splash that reached no one) disarm it back
to the free normal arrow — a hit resets the streak — so a stray shot
doesn't bench a hot streak, but a cold streak still never drains the
quiver.
Each shot resolves exactly once through `game.onShotResolved`
(`ArrowSystem.resolve()`; a split volley aggregates its fragments — see
the volley bullet under Combat); shooting down a pickup is neutral —
collecting a drop neither arms nor disarms. Stage start and retry reset Auto to the free arrow. Picking an ammo slot instead pins every shot to that type:
normal shots then conserve specials, and a pinned special unpins back to
Auto when its last arrow is spent (or when a stage starts without it in
stock — retry restores the stage-start snapshot). An empty special slot
can't be pinned; the click is ignored. The highlighted ammo slot is always
what the next shot will fire; in Auto mode the ✨ slot highlights alongside
it, showing the choice is automatic (before the first hit that is the
normal slot — a full quiver stays dark until a hit arms it).

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
  flask) that restores HP, clamped at max. Stages may scale both odds
  (`dropMult`/`healMult` on the stage entry, over the global
  `CONFIG.drops` values) — the late arc drops more, and more of it heals.
- Exploding splash ignores cover (no LOS check); freezing doubles the
  next hit (shatter); burning spreads to nearby enemies; lightning chains
  from the struck enemy through a seeded-random number of jolts
  (`CONFIG.arrow.types.lightning.jolts`), each jumping to the nearest
  not-yet-struck enemy within the jolt radius.
- Freezing arrows roll a random chance on any impact — enemy, ground,
  cover, or timeout (`CONFIG.arrow.types.freezing.burst`) — to detonate
  into a snow-powder burst that freezes every enemy in the radius. No
  damage (so it can never shatter a freeze) and no LOS check, like
  splash. The roll draws from the seeded rng; a burst that froze anyone
  counts as a hit for auto ammo.
- A burning arrow lobbed high splits as it dives back down through
  `CONFIG.arrow.types.burning.split.height` into a volley of burning
  arrows: one fragment holds the flight line (a lob at a single target
  still connects), the rest tilt off it by `split.angle`, landing in a
  ring of roughly `height·tan(angle)` radius — tuned to about a
  burn-spread, so the fires chain. The height sits above eye level, so the
  split only happens when there is room for the fan to matter — flat
  shots never cross it and stay precise single ignites. The volley stays
  ONE shot for auto ammo: fragments share a verdict that reports through
  `game.onShotResolved` once, when the last fragment is spent — a hit if
  any fragment damaged someone (`ArrowSystem.resolve()` in
  `web/arrows.js`).
- Arrow collision is segment-vs-sphere per frame (no tunneling at 95 m/s).
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
- Monsters are articulated figures (`web/models.js`): legs and arms are
  pivot groups at the hip/shoulder joints, and
  `EnemySystem.animateRig()` swings them each frame. The walk cycle's
  phase advances with the distance actually covered that frame (never
  wall-clock), so limb swing always matches ground speed — no
  foot-sliding, and an enemy that stops (or an inert e2e dummy) eases
  back to a neutral stance instead of pacing in place. Footfalls drive a
  small bob; a frozen enemy holds its pose mid-stride. Skeleton archers
  carry their bow in the left hand and blend both arms into a raised aim
  pose while peeking, squaring up to face the player (walking faces the
  movement direction, which reads wrong with a drawn bow). Collision is
  untouched by any of this: hit spheres stay config-derived
  (bodyRadius/height/headRadius), with the visual head centered at
  y=height.
- **Skeleton archer** — advances into range, hides behind the nearest
  obstacle on the player line, peeks to shoot, hides again (cover/peek
  point selection: `pickCover()`/`coverPoint()` in `web/enemies.js`).
  Cover is only accepted if at least one peek point has a line of fire to
  the player (neighboring obstacles can bury both peek lanes — the archer
  would shuttle between trees permanently hidden and stall the wave); it
  commits to the exposed side. With no workable cover and no line of fire
  from where it stands, it keeps advancing until a shot line opens.
  Projectiles drop at 4 m/s² with compensated aim, so long shots arc
  visibly; like player arrows they use segment-vs-sphere collision, so
  they cannot tunnel through the player even at low frame rates.
- Walkers collide with obstacles: after each AI step,
  `pushOutOfObstacles()` in `web/geom.js` pushes the enemy's body circle
  (`bodyRadius`) out of any obstacle cylinder it overlaps. Only the
  radial part of the step is cancelled, so enemies slide around trees
  toward the player instead of clipping through (or sticking to) them.
  Skeleton cover points sit 0.7 m off the obstacle edge — outside every
  body radius — so cover-hugging is unaffected.
- Sliding alone deadlocks in the concave pocket between two adjacent
  obstacles (a pillar wall lined up across the approach — the pushes from
  the two circles cancel), so walkers also steer
  (`EnemySystem.steerAround()`): when the lane a few metres ahead is
  blocked (`firstBlockingObstacle()` in `web/geom.js`), they follow the
  blocking obstacle's tangent with a light outward bias, and the detour
  side sticks until the lane clears so a wall is followed to its end
  rather than re-decided (and reversed) in every pocket along it. Applies
  to melee advance and every skeleton `moveToward()` alike; the push-out
  stays as the collision safety net.
- Melee ignores the perch elevation: attacks reach the player from the
  perch base by design.
- A melee attacker is spent on contact: it lands one hit and disappears
  (no score, no drop), so every monster that leaks through costs HP
  exactly once.

## Stages and waves

Five stages, easiest first: meadow (goblins only, low cover) → forest
(dense cover, skeleton archers debut) → desert (long sightlines, first
ogre) → iceberg (sparse cover, ogre debut in pairs) → volcano (ember
dusk, obsidian crags — the finale at the top speed multiplier). A
stage's wave count is the length of its `waves` array in
`CONFIG.stages` (1 → 2 → 2 → 3 → 3); the HUD counter and the
stage-clear check both read it. Rounds are short by design: the late
arc has more waves, but each is smaller than the forest/desert waves —
difficulty comes from the mix and the speed multiplier. HP refills between
stages; special ammo carries over, and stages with a `grant` entry
top the quiver up to a floor at stage start (`max(current, grant)`,
before the retry snapshot, so retries keep it) — freezing arrives with
the first ogres, exploding from iceberg on. Death → retry the same stage
with the ammo held at its start. Best score/stage persist in
`localStorage['archer.best']`.

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
yaw/pitch/touch/power, selected/mode, nocked/canShoot, arrows with type,
physics vs visual positions and trail length), and test hooks: `fireAt()`
(gravity-compensated),
`spawnEnemy()` (optional `inert` flag disables the AI), `skipToWave()`,
`killAll()`, `giveAmmo()`, `selectAmmo()`, `setDropChance()`, `setHealChance()`,
`setFreezeBurstChance()`,
`setObstacles()` (replace the collision-obstacle list to build exact cover
layouts; stage meshes stay), `setPlayerHp()`, `start()`,
`nextStage()`, `retryStage()`, `visiblePixelCount()`. Tests boot with
`?autostart=1&seed=42&waves=0&stage=forest` (the combat suite predates the
meadow opener and pins the seed-42 forest layout) for a clean battlefield
and park inert
target dummies at melee reach to avoid leading moving targets (live melee
enemies there would strike once and vanish); touch tests dispatch
synthetic `TouchEvent`s on the canvas (Chromium's `new Touch()`).

## Known simplifications

- No player movement; no sound.
- Stage geometry is regenerated (not disposed) on stage change — a small,
  bounded leak over a 5-stage run. The ground mesh is the exception: it is
  built once per theme and reused across loads.
