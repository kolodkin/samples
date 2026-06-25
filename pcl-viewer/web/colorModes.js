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
