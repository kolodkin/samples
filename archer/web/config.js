// All gameplay tuning in one place. Values are read at use time, so the
// test handle may patch them mid-run (e.g. drop chance).
export const CONFIG = {
  arena: { size: 80, spawnZ: -34, spawnXSpread: 28 },
  player: { hp: 100, pos: { x: 0, y: 3.2, z: 34 } },
  bow: {
    minSpeed: 20,
    maxSpeed: 70,
    power: { min: 0.2, max: 1, step: 0.1, initial: 0.6 }, // +/- adjusted shot power
    // Shot animation timing (s): string snap on release, empty-bow reload,
    // then the new arrow rides the string back out to the power draw.
    // Firing is gated until the fresh arrow is fully nocked.
    shot: { snap: 0.024, reload: 0.12, nock: 0.06 },
    baseFov: 70,
    zoomFov: 62,         // FOV eased in at max power
  },
  arrow: {
    gravity: -15,
    lifetime: 6,
    radius: 0.12,
    headshotMult: 2,
    types: {
      normal:    { damage: 34, color: 0xd8c9a3 },
      exploding: { damage: 20, color: 0xff7733, radius: 5, aoeDamage: 55 },
      freezing:  { damage: 18, color: 0x66ddff, freezeTime: 3, shatterMult: 2 },
      burning:   { damage: 15, color: 0xff4422, dps: 9, burnTime: 4, spreadRadius: 2.5 },
    },
  },
  touch: { lookScale: 2.2 }, // touch-drag aim, relative to mouse sensitivity
  drops: {
    chance: 0.2, min: 3, max: 5, lifetime: 20, radius: 0.6,
    heal: { chance: 0.25, amount: 25 }, // fraction of drops that are a potion instead of ammo
  },
  enemies: {
    goblin:   { hp: 40,  speed: 5.5, damage: 10, score: 100,
                bodyRadius: 0.55, height: 1.3, headRadius: 0.28, color: 0x4a8f3c },
    skeleton: { hp: 60,  speed: 4.0, damage: 8, score: 150, range: 26,
                peekTime: 1.4, hideTime: 2.0, projectileSpeed: 20, spread: 0.06,
                bodyRadius: 0.5, height: 1.6, headRadius: 0.26, color: 0xd9d4c5 },
    ogre:     { hp: 220, speed: 1.9, damage: 25, score: 400,
                bodyRadius: 1.0, height: 2.5, headRadius: 0.45, color: 0x7a6652 },
  },
  attackRange: 2.2,      // melee reach measured from the player
  headshotBonus: 50,     // extra score on a headshot kill
  multiKillBonus: 25,    // extra score per combo step (kills ≤1.5 s apart)
  multiKillWindow: 1.5,  // seconds between kills to sustain a combo
  waves: {
    perStage: 5,
    clearDelay: 2.0,     // seconds after a cleared wave before the next
    spawnInterval: 0.8,  // stagger between spawns within a wave
  },
  stages: {
    forest:  { speedMult: 1.0, waves: [
      { goblin: 4 },
      { goblin: 6 },
      { goblin: 5, skeleton: 2 },
      { goblin: 6, skeleton: 3 },
      { goblin: 6, skeleton: 3, ogre: 1 },
    ] },
    desert:  { speedMult: 1.15, waves: [
      { goblin: 6, skeleton: 2 },
      { goblin: 8, skeleton: 2 },
      { goblin: 8, skeleton: 3, ogre: 1 },
      { goblin: 10, skeleton: 3, ogre: 1 },
      { goblin: 10, skeleton: 4, ogre: 2 },
    ] },
    iceberg: { speedMult: 1.25, waves: [
      { goblin: 6, skeleton: 3, ogre: 1 },
      { goblin: 8, skeleton: 3, ogre: 1 },
      { goblin: 8, skeleton: 4, ogre: 2 },
      { goblin: 10, skeleton: 4, ogre: 2 },
      { goblin: 12, skeleton: 5, ogre: 3 },
    ] },
  },
};
