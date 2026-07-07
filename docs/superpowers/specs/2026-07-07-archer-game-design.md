# Archer — 3D Fantasy Wave-Defense Archery Game (three.js)

**Date:** 2026-07-07
**Status:** Approved design, pending implementation plan

## Summary

`archer/` is a standalone browser game sample: a first-person, stationary
wave-defense archery game with a DnD-fantasy flavor. The player defends a
fixed vantage point with a charged, ballistic bow against waves of low-poly
fantasy enemies that advance across three themed stages (forest → desert →
iceberg). Special arrow types (exploding, freezing, burning) drop from killed
enemies as limited ammo. Built on three.js with the repo's no-build vendored
ES-module stack (pcl-viewer is the reference implementation), a Preact + htm
UI layer, and Python Playwright end-to-end tests.

## Core Gameplay

### Player

- Stationary first-person defender on a slightly elevated vantage point
  (rock outcrop / dune ridge / ice shelf, per stage). No WASD movement.
- Mouse-look via the Pointer Lock API.
- 100 HP, shown as a health bar. Melee enemies deal contact damage on a
  cooldown when they reach the player; enemy archers fire projectiles with
  travel time and imperfect accuracy.

### Bow

- Hold left mouse to draw; draw power charges over ~1 s. The bow model at
  the bottom of the screen visibly pulls back; a subtle FOV zoom lands at
  full draw.
- Release to fire. Arrow launch speed scales with draw power; arrows are
  real projectiles under gravity (ballistic arcs, lead moving targets).
- A faint dotted trajectory hint shows at partial draw and fades out at full
  draw: beginners learn the arc, full-draw shots stay skill-based.
- Headshots (upper hitbox sphere) deal 2× damage.

### Arrow Types

Switch with keys 1–4 or the mouse wheel.

| # | Type | Ammo | Effect |
|---|------|------|--------|
| 1 | Normal | Infinite | Moderate single-target damage. |
| 2 | Exploding | Limited | AoE damage in a radius on impact; splash reaches targets behind cover. Best vs groups and covered archers. |
| 3 | Freezing | Limited | Target frozen solid ~3 s (cannot move or shoot); next hit on a frozen target deals bonus shatter damage. Small slow-field splash. |
| 4 | Burning | Limited | Damage-over-time burn ~4 s; burn spreads to enemies in very close proximity to a burning enemy. Best vs tanky single targets. |

### Arrow Economy

- Player starts with only normal arrows (stage 1, forest).
- ~20% of kills drop a floating special-arrow pickup icon granting +3–5
  arrows of one type. Pickups are collected by **shooting them** (the player
  cannot walk). Drop weights are tunable in `config.js`.
- Special-arrow inventory carries across stages.

## Enemies

Low-poly procedural meshes built from three.js primitives, flat-shaded,
animated by mesh-group bobbing / limb swings (no skeletons, no model files).
AI is deliberately simple per-enemy state machines.

| Enemy | Role | Behavior |
|-------|------|----------|
| Goblin rusher | Fast, low HP, melee | Advances with slight zigzag; attacks on contact. States: `advance → attack`. |
| Skeleton archer | Ranged, uses cover | Advances to firing range, picks the nearest obstacle roughly between itself and the player, hides behind it, peeks on a timer to shoot, re-hides. Only the exposed side is hittable while peeking. States: `advance → (cover ↔ peek/shoot)`. Countered by exploding arrows. |
| Ogre tank | Slow, high HP, heavy melee | Soaks normal arrows; burn and freeze-shatter are its counters. Appears from later forest waves onward. |

## Stages & Progression

Three procedurally-scattered arenas (~80×80 m), each with a distinct ground,
obstacle set, skybox color, fog, and lighting:

1. **Forest** — green ground, pine trees (cover-rich), soft fog. Baseline.
2. **Desert** — sand, cacti + rock formations (sparser cover, longer
   sightlines), bright light. Faster enemies, more rushers.
3. **Iceberg** — ice floor, glacial pillars, blue-white palette, snow
   particles. Toughest mix, more ogres.

- **Waves:** 5 per stage. Each wave spawns a budgeted enemy mix at the arena
  edge; the spawn budget grows per wave and per stage (all in `config.js`).
- **Advance:** clearing wave 5 shows a stage-complete screen with score,
  then loads the next scene.
- **Death:** HP 0 → game-over screen with score → retry the current stage,
  keeping the special-arrow inventory held when the stage started.
- **Score:** points per kill, headshot and multi-kill bonuses.
- **Persistence:** highest stage reached and best score in `localStorage`.

## UI / HUD

Preact + htm (vendored, no build step), rendering into a DOM overlay above
the WebGL canvas. A small hand-rolled observable store (plain object +
subscribe) carries game state; `main.js` pushes discrete events into it.

- **Screens:** title, pause (auto-shown on pointer-lock exit), stage
  complete, game over. Declarative overlay state machine.
- **Event-driven HUD:** health bar, arrow-type selector with ammo counts,
  wave/stage indicator, score. Re-renders only on discrete events (kill,
  shot, hit).
- **Per-frame escape hatch:** the draw-power ring around the crosshair (and
  the damage-direction flash) update via a CSS custom property through a
  ref, bypassing the vdom.
- UI elements carry `data-testid` attributes for Playwright.

## Technical Architecture

Reference stack: `pcl-viewer/` (vendored ESM, `serve.py`, Playwright e2e).

### Project Layout

```
archer/
├── archer.sh            # user entry point: vendor.sh once, then serve.py
├── vendor.sh            # pins three@0.160.0, preact, preact/hooks, htm into web/vendor/
├── serve.py             # minimal static server (same pattern as pcl-viewer)
├── web/
│   ├── index.html       # import map → ./vendor/*, canvas + UI mount point
│   ├── styles.css       # HUD/overlay styling
│   ├── main.js          # game loop, top-level state machine (title → playing → stage-clear → game-over)
│   ├── config.js        # all tuning constants: wave budgets, damage, speeds, drop rates
│   ├── rng.js           # seeded mulberry32 RNG (seedable via ?seed=)
│   ├── player.js        # camera rig, pointer-lock look, bow draw/charge, HP
│   ├── arrows.js        # projectile integration (gravity), impact resolution, arrow-type effects
│   ├── enemies.js       # enemy meshes + state-machine AI, cover logic
│   ├── stages.js        # forest/desert/iceberg scene builders
│   ├── waves.js         # spawn budgets, wave sequencing, pickup drops
│   ├── effects.js       # particles: explosion, fire, ice shatter, hit flashes
│   └── ui.js            # Preact app: screens + HUD, observable store
├── tests/
│   ├── conftest.py      # server fixture (serve.py subprocess), pcl-viewer pattern
│   └── test_e2e.py      # Playwright e2e
├── pyproject.toml       # dev deps: pytest, pytest-playwright
├── pytest.ini
├── README.md            # repo README convention (title / paragraph / run command)
└── SPEC.md              # mechanics + tuning catalog
```

### Data Flow

`main.js` owns the frame loop; each frame steps, in order: player input →
arrow physics → enemy AI → collision resolution → effects → UI store flush.
Modules communicate through a shared `game` state object passed explicitly —
no globals, no event bus. Collision detection is sphere-vs-sphere distance
checks; each enemy has a body sphere and a head sphere (headshots).

### Determinism & Test Handle

- All gameplay randomness flows through one seeded `mulberry32` RNG,
  seedable via `?seed=` for reproducible runs.
- Query params select scenarios: `?stage=desert&seed=42`.
- `window.__ARCHER` exposes `ready`, current state (score, wave, HP, enemy
  count) and test-only methods: `spawnEnemy(type, x, z)`, `fireAt(target)`,
  `skipToWave(n)`.

### Error Handling

- WebGL unavailable → friendly message overlay instead of a black canvas.
- Pointer-lock denied or escaped → auto-pause menu.
- `vendor.sh` is idempotent and the only network-dependent step; the served
  game is fully offline afterwards.

## Testing (Python Playwright)

- Boot: forest scene renders (visible-pixel check on the canvas, pcl-viewer
  style) and the title screen shows.
- Combat: spawn a goblin via the test handle, fire at it, assert kill +
  score increment.
- Arrow effects: explosion damages a group; freeze stops a target's
  movement; burn ticks damage over time.
- AI: skeleton archer ends up behind an obstacle (cover position assert).
- Flow: `skipToWave` → clear final wave → stage-complete screen; HP to 0 →
  game-over screen; retry restores stage-start inventory.
- Persistence: best score survives a reload via `localStorage`.

## Out of Scope (YAGNI)

- Player movement, jumping, dodging.
- Loaded 3D model files, skeletal animation, external assets.
- Physics engine (cannon-es / rapier) — ballistic integration + sphere
  checks suffice.
- Shop/economy beyond drops; difficulty settings; sound design beyond a few
  procedural WebAudio blips (stretch goal, not required for v1).
- Mobile/touch controls.
