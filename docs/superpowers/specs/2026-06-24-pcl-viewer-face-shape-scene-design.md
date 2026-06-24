# PCL Viewer — Face shape scene (replace the flat table scene)

## Goal

Replace the viewer's flat "PCL table scene" (`table_scene_lms400.pcd`) with a more
complex 3D *shape* cloud: the **BIWI face scan** (`biwi_face_database/model.pcd`,
100k points, `DATA binary`, ~1.2 MB), hot-linked from the same
`PointCloudLibrary/data` repo. Orient and frame it so the face reads as a face on
load, not the flat-scene look.

## Why the cloud, and why orientation matters

Inspected `biwi_face_database/model.pcd` directly:

- 100,000 points, `FIELDS x y z`, `DATA binary`, **no NaNs** (unorganized, dense —
  unlike the organized `640×480` Kinect captures such as `person.pcd`, which carry
  invalid-depth NaNs that would break bounding-box normalization).
- Pre-centered near the origin. Extents: `x ≈ ±0.09` (ear-to-ear), `y ≈ ±0.13`
  (chin-to-crown), `z ∈ [-0.063, 0.148]` (depth, nose protruding at **+Z**).

So the face is **already Y-up, facing +Z**. The viewer's `normalizeGeometry`
hardcodes `rotateX(-90°)` (KITTI z-up→y-up) and `frameCamera` uses a street-chase
camera. Applied unchanged, the face would render **lying on its back, viewed from a
road-chase angle** — recognizable only after manual orbiting. The change therefore
needs per-scene orientation + camera, not just a URL swap.

## Approach — a per-scene "profile" in `viewer.js`

Introduce a minimal profile distinguishing the two orientation conventions already
in play. Profiles are keyed by scene id inside `viewer.js`; `app.js` stays pure UI.

| Profile  | Scenes        | Orientation             | Scale metric                  | Camera                                        |
|----------|---------------|-------------------------|-------------------------------|-----------------------------------------------|
| `kitti`  | city, movie   | `rotateX(-90°)` (z-up→y-up) | robust horizontal (x,z) radius | street-chase (unchanged)                      |
| `object` | face          | none (already y-up)     | robust 3D radius              | front-on: eye toward +Z, slight up/right, look at center |

Everything else is unchanged: center on origin, `sceneRadius = 0.5`, the blue→red
ramp buffers (height / distance; intensity absent → falls back to flat), and the
point shape/size controls. Keeping `sceneRadius = 0.5` means the size slider
(0.002–0.05) stays meaningful across all scenes.

The `kitti` profile reproduces today's exact behavior, so the city and movie scenes
are byte-for-byte unaffected.

## Changes

1. **`web/config.js`** — repoint `PCL_URL` at `biwi_face_database/model.pcd`. Keep
   the `pclUrl` query-override key (the e2e contract for pointing the scene at a
   local fixture).
2. **`web/viewer.js`** —
   - Add a `SCENE_PROFILES` map (`city`/`movie` → `kitti`, `face` → `object`).
   - Parametrize `normalizeGeometry(geom, profile)` — rotation gated on the profile;
     scale by 3D radius for `object`, horizontal radius for `kitti`.
   - Parametrize `frameCamera(profile)` — front-on preset for `object`, the existing
     chase preset for `kitti`.
   - Rename the scene id `table` → `face` in `loadScene`; thread the active profile
     through `loadStatic`.
3. **`web/app.js`** — SCENES entry `{ id: 'table', label: 'PCL table scene' }` →
   `{ id: 'face', label: 'PCL face scan (BIWI)' }`.
4. **`tests/test_e2e.py`** — `test_static_scene_from_url` selects `face` instead of
   `table` (it overrides `pclUrl` to a local model and only asserts pixels were
   drawn, so it stays robust to the orientation change).
5. **`README.md` / `SPEC.md`** — update the scene description, the Scenes table, and
   the licensing note.

## Licensing

The BIWI Kinect Head Pose Database is free for **research/academic use** (Fanelli
et al., *Random Forests for Real Time 3D Face Analysis*, IJCV 2013) — not
permissively licensed. This is attributed the same way the project already
attributes its CC BY-NC-SA KITTI clouds, replacing the BSD-3-Clause table-scene
note in `SPEC.md`.

## Testing

Run the existing offline Playwright suite. It points the static PCL scene at a
local KITTI model via `?pclUrl=`, never the real URL — exactly as the current table
scene is exercised — and asserts `visiblePixelCount() > 1000`. The real face URL is
only fetched in production, like the table scene today. Verify the suite is green.

## Out of scope

No new committed assets or fixtures (the cloud is hot-linked at runtime). No change
to the city/movie scenes, the movie streaming pipeline, or any control beyond the
scene relabel.
