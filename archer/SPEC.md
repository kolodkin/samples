Archer — technical spec
---

All gameplay tuning — arrow types, enemy stats, wave compositions, stage
speed multipliers, drop rates, score bonuses — lives in `web/config.js`.
This spec covers behavior the numbers don't show.

## Controls

| Input | Action |
|-------|--------|
| Mouse move (pointer lock) | Aim |
| Hold / release left mouse | Draw (power charges ~1 s) / loose arrow |
| Keys 1–4, mouse wheel, tap/click a quiver slot | Select arrow type |
| Esc (exits pointer lock) | Pause |
| Touch: drag on the canvas | Aim (no pointer lock; one finger owns the camera) |
| Touch: hold / release the 🏹 button | Draw / loose arrow |
| Touch: ❚❚ button | Pause |

Touch mode is detected via `(pointer: coarse)` or the first `touchstart`;
hybrid devices keep both input paths live. Touch aim sensitivity is mouse
sensitivity × `CONFIG.touch.lookScale`.

## Combat

- Headshots deal double damage plus a score bonus; kills in quick
  succession chain a multi-kill combo bonus.
- A fraction of kills drop a floating pickup of a random special arrow
  type — shoot it to collect.
- Exploding splash ignores cover (no LOS check); freezing doubles the
  next hit (shatter); burning spreads to nearby enemies.
- Arrow collision is segment-vs-sphere per frame (no tunneling at 55 m/s).

## Enemies

- **Goblin** — weaving melee rush.
- **Ogre** — slow tank.
- **Skeleton archer** — advances into range, hides behind the nearest
  obstacle on the player line, peeks to shoot, hides again (cover/peek
  point selection: `pickCover()`/`coverPoint()` in `web/enemies.js`).
  Projectiles drop at 4 m/s² with compensated aim, so long shots arc
  visibly; they use point (not segment) collision — at 20 m/s vs a 0.9 m
  player radius they cannot tunnel.
- Melee ignores the perch elevation: attacks reach the player from the
  perch base by design.

## Stages and waves

Forest (dense cover) → desert (long sightlines) → iceberg (snow, fastest
enemies), five waves each per `CONFIG.stages`. HP refills between stages;
special ammo carries over. Death → retry the same stage with the ammo held
at its start. Best score/stage persist in `localStorage['archer.best']`.

## Determinism and e2e

All gameplay randomness flows through one seeded mulberry32 stream
(`web/rng.js`, `?seed=N`); particles are visual-only and exempt.
`window.__ARCHER` (defined in `web/main.js`) exposes `ready`, a `state`
snapshot (screen, hp, score, wave, enemies, pickups, obstacles, best,
yaw/pitch/touch), and test hooks: `fireAt()` (gravity-compensated),
`spawnEnemy()`, `skipToWave()`, `killAll()`, `giveAmmo()`,
`setDropChance()`, `setPlayerHp()`, `start()`, `nextStage()`,
`retryStage()`, `visiblePixelCount()`. Tests boot with
`?autostart=1&seed=42&waves=0` for a clean battlefield and park enemies at
melee reach to avoid leading moving targets; touch tests dispatch
synthetic `TouchEvent`s on the canvas (Chromium's `new Touch()`).

## Known simplifications

- No player movement; no sound.
- Stage geometry is regenerated (not disposed) on stage change — a small,
  bounded leak over a 3-stage run.
