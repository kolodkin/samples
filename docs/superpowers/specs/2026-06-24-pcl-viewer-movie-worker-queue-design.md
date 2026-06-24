# PCL Viewer — Movie worker-queue streaming + transport controls

## Goal

Make the KITTI movie scene **stream**: decode frames through a concurrent worker
queue, start rendering as soon as frames are ready (don't block on all 154), play
at ~15 fps, and add transport controls (Play/Pause, Stop, frame −/+ step).

## Current behavior (baseline)

- `viewer.js` `loadMovie(count)` awaits each `.drc` frame **one at a time**
  (`dracoLoad → normalizeMovieFrame → computeColorBuffers`) and only renders the
  first frame after **all** frames are decoded.
- Playback is a `setInterval(1000 / MOVIE_FPS)` with `MOVIE_FPS = 10`, advancing
  `(index + 1) % frames.length`.
- UI (`app.js`) exposes a single **Play/Pause** toggle for the movie.

## Design

### 1. Concurrent decode queue (`viewer.js`)

Replace the sequential loop in `loadMovie`:

- `frames = new Array(count).fill(null)`; each slot becomes
  `{ geometry, buffers }` once decoded. A separate `decodedCount` tracks progress.
- **Frame 0 is awaited first** — it produces the `shared` normalization transform
  that every other frame reuses (so the world stays put). Install frame 0, set
  `state.ready = true`, start the playback clock.
- Launch a **bounded-concurrency queue**: `CONCURRENCY = 4` async workers each pull
  the next undecoded index from a shared cursor and run
  `dracoLoad → normalizeMovieFrame(shared) → computeColorBuffers`, writing the slot
  and bumping `state.loadProgress`. DRACOLoader's WASM worker performs the decode;
  the queue keeps ~4 requests in flight instead of 1. Normalize + color buffers stay
  on the main thread (cheap at ~30k pts/frame).
- `loadToken` cancellation preserved: a stale token disposes any decoded slots and
  bails (both in frame-0 await and inside the queue workers).
- Error handling: a failed frame sets `state.error` and aborts the load (disposing
  decoded slots), matching today's failure semantics.

### 2. Early-start, hold-on-stall playback

- `MOVIE_FPS: 10 → 15`.
- `play()` keeps `setInterval(1000 / MOVIE_FPS)`. Each tick:
  - `next = (movie.index + 1) % count`
  - if `frames[next]` is decoded → advance + `installGeometry`
  - else (queue hasn't caught up) → **hold** the current frame (no advance, no skip,
    no error)
- Playback begins immediately after frame 0 installs. Hold-on-stall makes a larger
  pre-buffer unnecessary; at 4× concurrent decode the queue stays ahead of a 15 fps
  playhead. Once the queue fills, looping is seamless.

### 3. Transport controls

New `viewer.js` handle methods:

- `stop()` — pause and reset to frame 0 (`showFrame(0)`), `state.playing = false`.
- `stepFrame(delta)` — pause, then move to `(index + delta + count) % count`,
  **only if that slot is decoded** (ignore otherwise). Used by the −/+ buttons.

`app.js` movie control row becomes:

- **Play/Pause** toggle (existing `onPlayPause`).
- **Stop** button → `viewer.stop()`.
- **− / +** buttons → `viewer.stepFrame(-1)` / `viewer.stepFrame(+1)` (each pauses
  then steps one frame).

New `data-testid`s: `stop`, `frame-prev`, `frame-next`.

### 4. Tests + docs

- `tests/test_e2e.py`: drive the new controls via the `window.__PCL` hook —
  - step changes `frameIndex` and leaves `playing === false`,
  - stop resets `frameIndex` to 0 and pauses.
  The 4 committed fixtures (`tests/fixtures/movie/*.drc`) already exercise the movie
  path; `?movieCount=` points the run at them.
- `SPEC.md`: update fps (10→15), the movie pipeline description (concurrent queue,
  streaming start), and the Controls table (Stop, frame step).
- `README.md`: adjust the movie sentence if wording shifts (play/pause →
  play/pause/stop + frame step).

## Out of scope

- A full custom Web Worker that also fetches + normalizes + builds color buffers off
  the main thread (decided against — DRACOLoader's worker plus a concurrent queue is
  lower-risk and keeps offline e2e intact).
- A scrubber/timeline slider (only discrete −/+ stepping requested).

## Acceptance

- Movie scene shows its first frame quickly without waiting for all frames; HUD
  `Loading n / total` climbs while playback runs.
- Plays at ~15 fps and loops.
- Play/Pause, Stop (→ frame 1/total, paused), and −/+ stepping all work and are
  covered by e2e.
- Offline e2e (`uv run --group dev pytest`) passes.
