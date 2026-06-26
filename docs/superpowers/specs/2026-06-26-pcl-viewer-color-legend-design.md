PCL Viewer — top-right color legend
---

## Goal

Add an always-on **color legend** in the top-right corner that shows what the
active color mode encodes: a colormap gradient bar with min/max value labels for
the scalar ramps (height / distance / intensity), and a read-only class swatch
list when coloring by class. Flat shading shows nothing (there is no mapping to
explain).

## Current state

`web/viewer.js` colors points three ways: the blue→red HSL ramp (`rampColors`,
used by height/distance), the "hot" thermal colormap (`hotRampColors` over
`HOT_STOPS`, used by intensity), and the per-class palettes (`SEG_PALETTE` for
seg, `generateClassPalette` for loaded files). `web/colorModes.js` is the shared
single source of truth for the ordered mode list. `web/app.js` renders an
always-on stats **HUD** (`.panel.hud`) at `top:12px; right:12px`, plus a
clickable class legend inside the **Filters** tab. There is no overlay telling
the user what the current colors mean.

## Design

### Shared colormap source (`colorModes.js`)

To keep the legend's swatch in lockstep with the colors points actually get, the
colormap stops move into `colorModes.js` (already the shared module):

- Export `HOT_STOPS` (moved out of `viewer.js`, which now imports it) — the
  intensity thermal stops.
- Export `RAMP_GRADIENT`: an `{id → CSS linear-gradient(...)}` map for the
  continuous modes. `height`/`distance` sample the same `hsl(0.7-0.7t … 90% 50%)`
  sweep the per-point ramp uses; `intensity` is built from `HOT_STOPS`. `flat`
  and `class` have no entry (no gradient).

`viewer.js`'s `HOT_STOPS` definition is deleted and replaced by the import; its
per-point coloring is otherwise unchanged.

### Legend overlay (`app.js`)

A new `.corner` flex column wraps the existing HUD and the new legend panel,
anchored top-right (the HUD keeps its look; positioning moves to the wrapper).
The legend renders from the already-available `colorMode`, `stats.scalarRanges`,
and the class legend items (`SEG_LEGEND` for seg, `stats.classLegend` for loaded
files — the same `legendItems` the Filters tab uses):

- **flat** → legend hidden entirely.
- **height / distance / intensity** → a gradient bar (`RAMP_GRADIENT[mode]`)
  with the field's min and max from `stats.scalarRanges[mode]` beneath it (the
  same true min/max the filter steppers use; blank until the first stats tick).
- **class** → a read-only list of swatch + name chips (reusing the swatch
  styling, without the toggle behavior — toggling stays in the Filters tab).

The panel carries `data-testid="color-legend"`; the gradient bar
`data-testid="legend-ramp"`. It is always on like the HUD (and likewise dims
behind the menu backdrop).

### CSS (`styles.css`)

`.corner` is the absolutely-positioned top-right flex column (`gap` between HUD
and legend); `.corner .panel { position: static }` so the wrapped panels flow.
New rules for the legend title, the gradient bar (a short full-width strip), the
min/max scale row, and the read-only class chips (reuse `.swatch`).

## Tests

A new `test_color_legend_*` in `tests/test_e2e.py`:

- On the default movie scene (distance mode) the legend shows the ramp bar;
  switching color mode to `flat` hides the legend; switching to `height` /
  `intensity` keeps the ramp bar present.
- On the seg scene (by-class) the legend shows class swatches, not a ramp bar.

## Docs

Update `README.md` (one clause noting the top-right color legend) and `SPEC.md`
(a short note under the color-modes section + the controls table / overlay row).
