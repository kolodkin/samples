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
arrow (`Player.canShoot()`), so clicks mid-reload are swallowed. The
dotted trajectory hint is visible below 85% power and fades as power rises,
so full-power shots stay skill-based. Only deliberate fire inputs shoot: a
click while pointer-locked on desktop, the 🏹 button on touch. Stray clicks
or taps elsewhere on the screen never loose an arrow.

There is no manual arrow selection: every shot automatically spends the
strongest special in stock — exploding, then freezing, then burning
(`AMMO_PRIORITY` in `web/main.js`) — and falls back to the infinite basic
arrow once the quiver is dry. The HUD quiver is a read-only stock display;
the highlighted slot is what the next shot will fire.

Touch mode is detected via `(pointer: coarse)` or the first `touchstart`;
hybrid devices keep both input paths live. Touch aim sensitivity is mouse
sensitivity × `CONFIG.touch.lookScale`.

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

## Determinism and e2e

All gameplay randomness flows through one seeded mulberry32 stream
(`web/rng.js`, `?seed=N`); particles are visual-only and exempt.
`window.__ARCHER` (defined in `web/main.js`) exposes `ready`, a `state`
snapshot (screen, hp, score, wave, enemies, pickups, obstacles, best,
yaw/pitch/touch/power, nocked/canShoot), and test hooks: `fireAt()` (gravity-compensated),
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
