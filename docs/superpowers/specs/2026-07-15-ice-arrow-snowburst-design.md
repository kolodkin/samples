Freezing-arrow snowburst — design
---

Date: 2026-07-15
Project: `archer`
Status: approved design, pre-implementation

## Summary

A freezing arrow has a random chance to detonate into a snow-powder burst
when it resolves. The burst freezes every enemy within its radius — no
damage, pure control. The other rolls behave exactly as today: direct hits
freeze their single target, ground/cover shots are duds.

## Mechanic

- Every freezing arrow rolls once at resolution — direct enemy hit, ground,
  obstacle, or lifetime timeout.
- On success (`chance`, default 25%), the arrow snowbursts: every enemy
  within `radius` is frozen with the standard freeze (`freezeTime` 3 s,
  shatter-primed, quenches burn). No AoE damage.
- The burst ignores line of sight, same as the exploding arrow's splash:
  it freezes enemies hiding behind cover, so it remains a counter to
  skeleton archers.
- On a direct hit that rolls a burst, the arrow's usual single-target
  `freeze()` is skipped — the burst freezes everyone in radius, and the
  target is at distance ~0, so it is always included. Direct-hit damage
  itself lands as normal, before the burst (unchanged `hit()` order:
  damage, then freeze).
- Because the burst deals no damage, it can never shatter a freeze —
  its own or a pre-existing one. No ordering hazards.

## Config

New block under `CONFIG.arrow.types.freezing` in `web/config.js`:

```js
burst: { chance: 0.25, radius: 4 }
```

Radius sits just under the exploding arrow's 5 — a control effect, not a
second bomb. All values config-tunable like every other gameplay number.

## Code shape

- `ArrowSystem.snowburst(pos)` in `web/arrows.js`, a sibling of
  `explode(pos)`: loop enemies, freeze those inside `radius`, return
  whether anyone was frozen.
- The roll happens where freezing arrows resolve in `web/arrows.js`
  (`hit()` for direct strikes; the blocked/ground/timeout branch of
  `update()` for everything else), mirroring how `exploding` detonates
  on whatever stopped it.

## Determinism

The roll draws from the seeded rng stream (`game.rng.random() < chance`),
never `Math.random()` — it is gameplay-affecting and must replay
identically per seed. The draw only occurs when a freezing arrow resolves,
so existing seed-pinned tests that fire no freezing arrows are unaffected.
The particle visual stays exempt (visual-only rule in `web/effects.js`).

## Auto-ammo

Auto-ammo's arm/disarm is currently damage-based. The snowburst extends
the convention: a burst that froze at least one enemy resolves the shot as
a hit (`onShotResolved(true)`), because the shot materially affected
enemies. A dud roll on the ground stays a miss. A direct hit is a hit
regardless, as today.

## Visual

Snow-powder burst in `web/effects.js`: a white/pale-ice variant of
`burst()` with more particles, lower speed, and gentle fall so it hangs
like powder instead of spraying like debris. Implemented as a small
options extension to `burst()` (gravity/size overrides), not a new
particle system.

## Testing

e2e additions in `archer/tests/test_e2e.py`, driven by existing hooks
(config is read at use time, so tests patch it mid-run):

- `burst.chance = 1`: a ground shot near parked inert dummies freezes all
  dummies inside the radius and none outside it.
- `burst.chance = 0`: a ground shot freezes nobody (today's behavior).
- `burst.chance = 1`, direct hit: the target ends frozen, not shattered,
  and took only the direct-hit damage.
- Auto-ammo: with `burst.chance = 1`, a ground snowburst that froze an
  enemy arms Auto (next shot spends a special).

## Docs

One-line update to the combat section of `archer/SPEC.md` describing the
random snowburst.
