LinkedIn Post — Archer
---

Draft post announcing the archer game.

---

After playing with point clouds in three.js, I had to try the fun side of 3D — so I built a simple first-person shooter (archer) game. 🏹

You hold the line against waves of goblins, ogres and skeleton archers across five low-poly arenas. Arrows obey gravity, and kills drop special pickups — exploding, lightning, freezing, and a burning arrow that splits mid-dive into a volley of fire arrows. 🔥 The animation is naive, yet everything flows surprisingly well.

It's a few-minute game. Give it a go and let me know what you think:
👉 https://kolodkin.github.io/samples/archer/

The backbone is deliberately simple:
• one static page, no build step, no bundler
• three.js for the 3D, a small Preact SPA for the UI
• plain ES modules with vendored dependencies
• seeded RNG, so every run is reproducible — the e2e tests literally play the game with Playwright

It turned out way more fun than I expected. And the real takeaway: agentic AI has dropped the barrier to building this low. A 3D game used to mean engines, tooling and months of ramp-up — now it's a clear idea and a few focused sessions. Everybody can be a builder now.

Enjoyed the game? Comment "fun" 🎯
Want a write-up on how it's built? Comment "details" 👇

#threejs #gamedev #webdev #javascript #preact
