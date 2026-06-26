// The viewer's color modes, in display order — the single source of truth shared
// by the three.js viewer and the Preact UI. `viewer.js` offers the subset a given
// cloud can supply (by `id`, from the ramp/class buffers it builds) and reports
// the applied mode; `app.js` renders each offered id with its `label`.
export const COLOR_MODES = [
  { id: 'flat', label: 'Flat' },
  { id: 'class', label: 'By class' },
  { id: 'height', label: 'By height' },
  { id: 'distance', label: 'By distance' },
  { id: 'intensity', label: 'By intensity' },
];

// "Hot" (thermal) colormap stops (t in [0,1] -> rgb in [0,1]), the single source
// of truth for the intensity colormap: the viewer (`hotRampColors`) samples it per
// point and the UI legend renders it as a CSS gradient (RAMP_GRADIENT below). Only
// the lowest values read as cool purple; the bulk climbs magenta -> red -> orange
// -> yellow -> near-white, so the cloud is mostly bright and reflective surfaces pop.
export const HOT_STOPS = [
  { t: 0.00, c: [0.50, 0.16, 0.70] }, // bright purple (low intensity only)
  { t: 0.15, c: [0.90, 0.22, 0.55] }, // magenta
  { t: 0.35, c: [1.00, 0.38, 0.28] }, // red
  { t: 0.60, c: [1.00, 0.64, 0.22] }, // orange
  { t: 0.82, c: [1.00, 0.88, 0.45] }, // yellow
  { t: 1.00, c: [1.00, 1.00, 0.92] }, // near-white    (high intensity)
];

// CSS `color pos%` stops sampling the viewer's blue->red HSL ramp (hue 0.7->0,
// sat 0.9, light 0.5 — mirror the per-point formula in rampColors, viewer.js; keep
// the two in sync), so the legend bar matches the per-point coloring. Sampled at 9
// stops since CSS interpolates linearly in sRGB, not in hue, and wouldn't otherwise
// trace the same blue->cyan->green->yellow->red arc.
function blueRedStops() {
  const stops = [];
  for (let i = 0; i <= 8; i++) {
    const t = i / 8;
    const hue = Math.round((0.7 - 0.7 * t) * 360);
    stops.push(`hsl(${hue} 90% 50%) ${Math.round(t * 100)}%`);
  }
  return stops;
}

// CSS `color pos%` stops for the hot colormap, straight off HOT_STOPS.
function hotStops() {
  return HOT_STOPS.map(({ t, c }) => {
    const rgb = c.map((v) => Math.round(v * 255)).join(' ');
    return `rgb(${rgb}) ${Math.round(t * 100)}%`;
  });
}

// id -> a left(low)-to-right(high) CSS linear-gradient for the continuous scalar
// modes, used by the UI legend's gradient bar. height/distance share the blue->red
// ramp (built once); only scalar ramps have an entry — `flat` (no color) and
// `class` (a discrete palette, shown as swatches) have none.
const blueRedGradient = `linear-gradient(to right, ${blueRedStops().join(', ')})`;
export const RAMP_GRADIENT = {
  height: blueRedGradient,
  distance: blueRedGradient,
  intensity: `linear-gradient(to right, ${hotStops().join(', ')})`,
};
