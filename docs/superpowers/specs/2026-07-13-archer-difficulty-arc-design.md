Archer difficulty arc redesign — design spec
---

Date: 2026-07-13. Approved in conversation (5-stage arc, volcano finale,
target-aware Auto).

## Problem

The current game has three stages (forest → desert → iceberg) and the
difficulty step into desert is a wall: total enemy HP nearly doubles
(~1,780 → ~3,400), speed rises 15%, and the player's damage stays flat.
Auto ammo mode makes it worse by spending the strongest special arrow on
every shot, so AoE stock is wasted on single targets.

## Design

### 1. Five-stage arc (was three)

`STAGE_ORDER = ['meadow', 'forest', 'desert', 'iceberg', 'volcano']`.
Two new themes; the finale is exactly as hard as today's desert — nothing
in the game is ever harder than the old stage 2.

| # | Stage | Theme | speedMult | Waves | ~Total HP |
|---|-------|-------|-----------|-------|-----------|
| 1 | meadow  | new: bright grass, boulders + bushes | 0.80 | 3 | 360 |
| 2 | forest  | existing | 0.90 | 4 | 1,000 |
| 3 | desert  | existing; first ogre | 1.00 | 4 | 1,780 |
| 4 | iceberg | existing | 1.10 | 5 | 2,840 |
| 5 | volcano | new: charred ground, ember sky, glowing obsidian crags | 1.15 | 5 | 3,400 |

Wave tables (goblin/skeleton/ogre):

- meadow:  2g · 3g · 4g
- forest:  3g+1s · 4g+1s · 4g+2s · 5g+2s
- desert:  4g+2s · 5g+2s · 5g+2s+1o · 6g+3s+1o
- iceberg: 5g+2s · 6g+2s · 6g+2s+1o · 7g+3s+1o · 7g+3s+2o
- volcano: 6g+2s · 8g+2s · 8g+3s+1o · 10g+3s+1o · 10g+4s+2o  (old desert table)

Wave count is per stage (the length of its waves array); the global
`CONFIG.waves.perStage` is removed. The HUD "wave x/y" and the
stage-clear check read the current stage's own length. The default
starting stage becomes `STAGE_ORDER[0]` (meadow).

New themes reuse the existing prop recipe (`texturedMesh`) with new
palettes: meadow = grey boulders + leafy bushes; volcano = dark obsidian
crags with a lava-orange emissive (ice-pillar silhouette, recolored).

### 2. Stage-start ammo grants (floor semantics)

Optional per-stage `grant: { type: n }` in `CONFIG.stages`. Applied when
a stage begins, before the retry snapshot, as a floor:
`ammo[t] = max(ammo[t], n)` — it tops up a dry quiver but never reduces
or inflates carry-over above the floor. Retry restores the granted
snapshot.

- desert:  freezing 10 (first ogre)
- iceberg: exploding 15, freezing 10
- volcano: exploding 20, freezing 10

### 3. Per-stage drop tuning (multipliers)

Optional per-stage `dropMult` / `healMult` over the global
`CONFIG.drops.chance` (0.4) and `CONFIG.drops.heal.chance` (0.25),
following the existing `speedMult` idiom:

- desert:  dropMult 1.1,  healMult 1.2  → 0.44 / 0.30
- iceberg: dropMult 1.25, healMult 1.3  → 0.50 / 0.325
- volcano: dropMult 1.25, healMult 1.4  → 0.50 / 0.35

Multipliers compose with the `setDropChance()`/`setHealChance()` test
hooks (which patch the global base). Resolved values are exposed in the
`__ARCHER.state` snapshot for tests.

### 4. Target-aware smart Auto

Auto mode no longer spends the strongest special on every shot. Each
shot (and the HUD highlight) picks by reading the battlefield:

1. Find the aimed enemy: nearest enemy along the aim ray, matched in the
   XZ plane (perpendicular distance < bodyRadius + slack) so pitch/arc
   compensation never unselects a target.
2. exploding — if in stock and ≥3 enemies (aimed included) sit within
   the blast radius (5) of the aimed enemy.
3. freezing — if in stock and the aimed enemy is an unfrozen ogre.
4. burning — if in stock, the aimed enemy is not burning, and ≥2 enemies
   sit within the spread radius (2.5).
5. otherwise normal — including when nothing is aimed at.

Pinned slots are unchanged. The HUD quiver highlight must stay live: the
main tick resyncs the UI when the auto pick changes (aim moves).

## Testing

Playwright e2e, test-first: 5-stage build/determinism, meadow wave mix,
per-stage wave counts ending in victory on volcano, grant floor +
carry-over + retry, resolved drop tuning + guaranteed-drop behavior via
multiplier, and smart-Auto selection for each rule (replacing the two
strongest-first tests). Existing combat tests keep their forest layout
via an explicit `stage=forest` boot param.

## Out of scope

Player movement, sound, new enemy types, per-stage arrow damage tuning.
