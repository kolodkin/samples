# Archer: split arrow — design

## Problem

The game has four special arrows (exploding, lightning, freezing, burning)
but no multishot: a fired arrow never splits into a "quiver of arrows".
Requested feature: a split arrow that bursts mid-flight into a fan of
splinter arrows.

## Approaches considered

1. **New special ammo type `split` (chosen).** One arrow that, after a
   short flight, fans out horizontally into several splinters. Fits the
   existing type system (config entry + quiver slot + drop type) with no
   new subsystems; deterministic (no rng), so e2e-testable.
2. **Split behavior bolted onto the burning ("fire") arrow.** Overloads
   one type with two unrelated mechanics (ignite + multishot) and breaks
   the one-mechanic-per-type pattern every other arrow follows.
3. **Every arrow splits at high power.** Changes core balance of all five
   existing types and all existing tests; far beyond the request.

## Design

New special type `split` in `CONFIG.arrow.types`, declared between
`lightning` and `freezing` — that declaration order is the ✨ Auto
priority, and a fan of splinters sits below lightning's guided chain but
above freezing's utility:

```js
split: { damage: 20, color: 0xcc66ff, count: 5, splitTime: 0.25, spread: 0.12 }
```

- **Flight**: the fired arrow flies normally for `splitTime` seconds, then
  bursts (particle flash in the type color) into `count` splinters sharing
  its speed, fanned by yaw rotations evenly spaced across ±`spread`
  radians about world up — the middle splinter keeps the original line.
  Deterministic: no rng draw, so seeded runs replay exactly.
- **Splinters** are ordinary arrows of type `split` flagged as children:
  they never re-split, deal `damage` each (headshot multiplier applies),
  and carry no status effect. Each has its own mesh and tracer trail.
- **Pre-split contact**: a split arrow that hits an enemy, obstacle, or
  the ground before `splitTime` resolves like any other arrow and spawns
  no splinters (point-blank shots don't fan out behind the target).
- **Auto ammo accounting**: one trigger pull = one outcome. Splinters
  share a group record; the shot reports hit to `onShotResolved` if any
  splinter damaged an enemy, once, when the last splinter resolves.
  Splinters spent on pickups stay neutral (a fully-neutral fan reports
  nothing), matching the existing pickup rule.
- **HUD**: quiver slot 🔱 "Split" between ⚡ and ❄️; digit keys extend to
  1–7 automatically (`AMMO_KEYS` derives from `SLOTS`).
- **Economy**: `split` joins the pickup drop pool (`DROP_TYPES` +
  `DROP_COLORS` in waves.js) and the volcano stage grant (`split: 12`) so
  the finale showcases it; `game.stats.ammo` gains a `split: 0` slot.

## Testing

Playwright e2e, same determinism conventions as the suite:

- fan-out: one fired split arrow becomes exactly `count` splinters.
- multi-target: three spread goblins each damaged by one splinter of a
  single shot (geometry pinned by `splitTime`/`spread`).
- auto outcome: a fan whose center splinter hits keeps Auto armed even
  though the outer splinters later miss into the ground.
- slot/keys: 🔱 slot pins with stock; Digit5 selects split, Digit6 now
  freezing.
- grant: volcano stage starts with 12 split arrows.

Docs: SPEC.md (controls table, ammo priority, combat bullet), README
paragraph (drop list, keys 1–7).
