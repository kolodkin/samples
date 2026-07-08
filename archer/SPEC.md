Archer — technical spec
---

## Controls

| Input | Action |
|-------|--------|
| Mouse move (pointer lock) | Aim |
| Hold / release left mouse | Draw (power charges ~1 s) / loose arrow |
| Keys 1–4, mouse wheel, tap/click a quiver slot | Select arrow type |
| Esc (exits pointer lock) | Pause |
| Touch: drag on the canvas | Aim (no pointer lock; one finger owns the camera, tracked by touch identifier) |
| Touch: hold / release the 🏹 button | Draw / loose arrow |
| Touch: ❚❚ button | Pause |

Touch mode is detected via `(pointer: coarse)` or the first `touchstart` and
adds the fire and pause buttons to the HUD; hybrid devices keep both input
paths live. Touch aim sensitivity is mouse sensitivity × `CONFIG.touch.lookScale`.

## Arrow types

| Type | Ammo | Damage | Effect |
|------|------|--------|--------|
| Normal | ∞ | 34 | — |
| Exploding | drops | 20 + AoE 55, r=5, linear falloff | Splash ignores cover (no LOS check) |
| Freezing | drops | 18 | Freeze 3 s; next hit ×2 (shatter) |
| Burning | drops | 15 | 9 dps for 4 s; spreads within 2.5 m |

Headshots ×2 damage, +50 score. Multi-kill combo: +25 score per extra kill
landed within 1.5 s of the previous one. Drops: 20% of kills, +3–5 arrows
of a random special type, collected by shooting the floating pickup.

## Enemies

| Enemy | HP | Speed | Behavior |
|-------|----|-------|----------|
| Goblin | 40 | 5.5 | Weaving melee rush, 10 dmg per 1.2 s |
| Skeleton archer | 60 | 4.0 | Advances to 26 m, hides behind the nearest obstacle on the player line, peeks 1.4 s to shoot (8 dmg), hides 2 s |
| Ogre | 220 | 1.9 | Slow tank, 25 dmg per 1.8 s |

Cover selection: nearest obstacle within 18 m of the archer whose direction
from the player is within ~45° of the archer's and closer to the player than
the archer is; cover point = obstacle edge + 0.7 m on the far side; peek
point offsets sideways by the obstacle radius + 0.5 m. Skeleton projectiles
drop at 4 m/s² and the aim compensates for it, so long shots arc visibly.

## Stages and waves

Five waves per stage; enemy mixes and per-stage speed multipliers live in
`web/config.js` (`CONFIG.stages`). Forest (26 trees, dense cover) →
desert (14 cacti/rocks, long sightlines, ×1.15 speed) → iceberg (18 ice
pillars, snow particles, ×1.25 speed). HP refills between stages; special
ammo carries over. Death → retry the same stage with the ammo held at its
start. Best score/stage persist in `localStorage['archer.best']`.

## Determinism and e2e

All gameplay randomness flows through one seeded mulberry32 stream
(`web/rng.js`, `?seed=N`); particles are visual-only and exempt.
`window.__ARCHER` exposes `ready`, a `state` snapshot (screen, hp, score,
wave, enemies, pickups, obstacles, best) and test hooks: `fireAt(x, y, z,
type?, power?)` (gravity-compensated), `spawnEnemy(type, x, z)`,
`skipToWave(n)`, `killAll()`, `giveAmmo(type, n)`, `setDropChance(c)`,
`setPlayerHp(n)`, `start(i)`, `nextStage()`, `retryStage()`,
`visiblePixelCount()`. The state snapshot also exposes `yaw`/`pitch`/`touch`
so touch-aim tests can assert camera motion; touch e2e tests dispatch
synthetic `TouchEvent`s on the canvas (Chromium's `new Touch()`). Tests park enemies at melee reach (z=32) to avoid
leading moving targets, and boot with `?autostart=1&seed=42&waves=0` for a
clean battlefield. Arrow collision is segment-vs-sphere per frame (no
tunneling at 55 m/s). Enemy melee ignores the perch elevation (attacks
reach the player from the perch base by design).

## Known simplifications

- No player movement; no sound.
- Stage geometry is regenerated (not disposed) on stage change — a small,
  bounded leak over a 3-stage run.
- Enemy projectiles use point (not segment) collision: at 20 m/s vs a
  0.9 m player radius they cannot tunnel.
