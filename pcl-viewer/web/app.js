// Preact UI (no JSX build — htm). Owns UI state and drives the viewer handle.
import { h, render } from 'preact';
import { useEffect, useRef, useState } from 'preact/hooks';
import htm from 'htm';
import { createViewer } from './viewer.js';
import { COLOR_MODES } from './colorModes.js';

const html = htm.bind(h);

const SCENES = [
  { id: 'movie', label: 'KITTI movie' },
  { id: 'seg', label: 'KITTI seg' },
  { id: 'lucy', label: 'Stanford Lucy' },
];

// id -> label lookup over the shared mode list. Which modes are actually offered
// per scene (and the applied one) is decided by the viewer and pushed in via the
// onColorState callback below; the dropdown renders only those.
const COLOR_MODE_LABEL = Object.fromEntries(COLOR_MODES.map((m) => [m.id, m.label]));
const ALL_MODE_IDS = COLOR_MODES.map((m) => m.id);

// A few representative classes for the seg-scene legend (hex/cls match SEG_PALETTE
// indices in viewer.js). `cls` is the class id each swatch toggles in the filter.
const SEG_LEGEND = [
  { name: 'car', hex: '6496F5', cls: 1 }, { name: 'person', hex: 'FF1E1E', cls: 6 },
  { name: 'road', hex: 'FF00FF', cls: 9 }, { name: 'sidewalk', hex: '4B004B', cls: 11 },
  { name: 'building', hex: 'FFC800', cls: 13 }, { name: 'vegetation', hex: '00AF00', cls: 15 },
  { name: 'pole', hex: 'FFF096', cls: 18 }, { name: 'traffic-sign', hex: 'FF0000', cls: 19 },
];

// Scalar fields offered as range filters. Height and distance exist on every
// scene; intensity only where the cloud supplies it (gated on the offered modes).
const FILTER_FIELDS = [
  { key: 'height', label: 'Height' },
  { key: 'distance', label: 'Distance' },
  { key: 'intensity', label: 'Intensity' },
];
const EMPTY_FILTERS = {
  height: { min: '', max: '' },
  distance: { min: '', max: '' },
  intensity: { min: '', max: '' },
};

// A "nice" step (1/2/5 × 10^k) near the requested magnitude, so the filter +/-
// buttons move each field by a clean amount scaled to its own data range
// (≈5 for 0–255 intensity, ≈0.005 for the tiny normalized height span, etc.).
function niceStep(raw) {
  if (!(raw > 0) || !Number.isFinite(raw)) return 1;
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const norm = raw / mag; // 1..10
  return (norm < 1.5 ? 1 : norm < 3 ? 2 : norm < 7 ? 5 : 10) * mag;
}

function App() {
  const canvasRef = useRef(null);
  const viewerRef = useRef(null);
  const [pointSize, setPointSize] = useState(0.004);
  // Color state is pushed by the viewer (onColorState), not polled: `colorMode`
  // is the mode actually applied, `colorModes` the subset the live scene offers.
  const [colorMode, setColorMode] = useState('distance');
  const [colorModes, setColorModes] = useState(ALL_MODE_IDS);
  const [pointShape, setPointShape] = useState('ball');
  const [sceneId, setSceneId] = useState('movie');
  const [menuOpen, setMenuOpen] = useState(false);
  // Which menu tab is showing. The controls are split into a "View" tab (scene,
  // transport, appearance) and a "Filters" tab (range filters + seg class
  // legend); only the active tab's body is rendered. Persists across open/close.
  const [tab, setTab] = useState('view');
  const [frame, setFrame] = useState(0);
  const [showBoxes, setShowBoxes] = useState(true);
  // Filter UI state mirrors the viewer's filters. Range bounds are kept as raw
  // strings ('' = unbounded) so the inputs stay editable; hiddenClasses tracks the
  // class ids toggled off via the legend.
  const [filters, setFilters] = useState(EMPTY_FILTERS);
  const [hiddenClasses, setHiddenClasses] = useState([]);
  const origin = { x: 0, y: 0, z: 0 }; // placeholder until the first stats tick
  const fileInputRef = useRef(null);
  const [stats, setStats] = useState({
    ready: false, pointCount: 0, fps: 0, cameraDistance: 0, eye: origin, target: origin,
    scene: 'movie', frameIndex: 0, frameCount: 0, playing: false,
    loading: false, loadProgress: { loaded: 0, total: 0 }, error: null,
    visibleCount: 0, scalarRanges: {}, fileName: null, classLegend: [],
    pointSize: 0.004,
  });

  useEffect(() => {
    // The viewer pushes the applied color mode + the modes the live scene offers
    // whenever they change (scene switch, fallback, or a manual pick), so the
    // dropdown follows the viewer without polling.
    const viewer = createViewer(canvasRef.current, {
      onColorState: ({ mode, modes }) => { setColorMode(mode); setColorModes(modes); },
    });
    viewerRef.current = viewer;

    let frames = 0, last = performance.now(), fps = 0, raf = 0;
    const loop = () => {
      raf = requestAnimationFrame(loop);
      frames++;
      const now = performance.now();
      if (now - last >= 500) {
        fps = Math.round((frames * 1000) / (now - last));
        frames = 0; last = now;
        const s = viewer.getStats();
        setStats({ ...s, fps });
      }
    };
    loop();
    return () => { cancelAnimationFrame(raf); viewer.dispose(); };
  }, []);

  // Mirror the playhead into local state so the frame-select slider tracks
  // playback (and scene resets to 0) between manual seeks.
  useEffect(() => { setFrame(stats.frameIndex); }, [stats.frameIndex]);

  // The viewer auto-sizes points to a loaded file's density; mirror that into the
  // slider so it shows the applied size. (A user drag sets both, so the next tick
  // matches and this is a no-op then.)
  useEffect(() => { if (stats.pointSize) setPointSize(stats.pointSize); }, [stats.pointSize]);

  const onSize = (e) => {
    const v = parseFloat(e.target.value);
    setPointSize(v); viewerRef.current.setPointSize(v);
  };
  // Drive the viewer; the onColorState callback echoes the applied mode back into
  // local state (so a fallback would be reflected too).
  const onColor = (e) => viewerRef.current.setColorMode(e.target.value);
  const onShape = (e) => {
    setPointShape(e.target.value); viewerRef.current.setPointShape(e.target.value);
  };
  const onScene = (e) => {
    const id = e.target.value;
    if (id === 'file') return; // the "loaded file" entry isn't a re-selectable scene
    setSceneId(id); viewerRef.current.loadScene(id);
    // Per-scene color defaults (e.g. seg → "by class") live in the viewer and
    // arrive via onColorState once the new scene's first frame installs.
  };
  // "Load PCL…" opens the OS file picker via the hidden input; picking a file
  // loads it as the transient "file" scene.
  const onLoadClick = () => fileInputRef.current.click();
  const onFile = (e) => {
    const file = e.target.files && e.target.files[0];
    e.target.value = ''; // allow re-picking the same file later
    if (!file) return;
    setSceneId('file');
    // loadFile resets the viewer's filters (the new cloud is differently scaled
    // and enumerated); mirror that in the UI so the inputs/legend match.
    setFilters(EMPTY_FILTERS);
    setHiddenClasses([]);
    viewerRef.current.loadFile(file);
  };
  const onToggleBoxes = (e) => {
    setShowBoxes(e.target.checked); viewerRef.current.setShowBoxes(e.target.checked);
  };
  const onPlayPause = () => {
    // Decide from the viewer's live state, not the 500ms-throttled `stats`
    // snapshot — otherwise a click landing before the next stats tick can
    // read a stale `playing` and call the wrong action.
    if (viewerRef.current.getStats().playing) viewerRef.current.pause();
    else viewerRef.current.play();
  };
  const onStep = (delta) => {
    viewerRef.current.step(delta);
    setFrame(viewerRef.current.getStats().frameIndex);
  };
  const onSeek = (e) => {
    const i = parseInt(e.target.value, 10);
    setFrame(i); // keep the slider responsive ahead of the throttled stats tick
    viewerRef.current.seek(i);
  };
  const onFrameInput = (e) => {
    // The input is 1-based to match the readout; seek() is 0-based and clamps.
    const v = parseInt(e.target.value, 10);
    if (Number.isNaN(v)) return;
    const i = v - 1;
    setFrame(i);
    viewerRef.current.seek(i);
  };
  const onReset = () => viewerRef.current.resetCamera();
  // Empty / unparseable input means "no bound on this side" (the default, max = ∞).
  const toBound = (s) => (s === '' || Number.isNaN(parseFloat(s)) ? null : parseFloat(s));
  const commitFilter = (field, range) => {
    setFilters({ ...filters, [field]: range });
    viewerRef.current.setFilter(field, { min: toBound(range.min), max: toBound(range.max) });
  };
  const onFilter = (field, bound, e) => {
    commitFilter(field, { ...filters[field], [bound]: e.target.value });
  };
  // The −/+ buttons nudge a bound by a range-scaled step. An empty (unbounded)
  // bound first materializes at its data extreme — a full-range handle that clips
  // nothing — then steps inward; values stay clamped to the (step-aligned) data
  // envelope so the buttons never run off into meaningless territory.
  const onFilterStep = (field, bound, dir) => {
    // Read the live ranges from the viewer, not the 500ms-throttled `stats`
    // snapshot: a click landing before the first post-load stats tick would
    // otherwise see an empty `scalarRanges` and no-op (flaky under slow CI).
    const rng = viewerRef.current.getStats().scalarRanges[field];
    if (!rng) return;
    const step = niceStep((rng.max - rng.min) / 40) || 1;
    const lo = Math.floor(rng.min / step) * step;
    const hi = Math.ceil(rng.max / step) * step;
    const cur = filters[field][bound];
    let v;
    if (cur === '' || Number.isNaN(parseFloat(cur))) {
      v = bound === 'max' ? hi : lo;
    } else {
      v = Math.round((parseFloat(cur) + dir * step) / step) * step;
      v = Math.min(hi, Math.max(lo, v));
    }
    const decimals = Math.max(0, -Math.floor(Math.log10(step)));
    commitFilter(field, { ...filters[field], [bound]: v.toFixed(decimals) });
  };
  const onToggleClass = (cls) => {
    const hidden = hiddenClasses.includes(cls);
    setHiddenClasses(hidden ? hiddenClasses.filter((c) => c !== cls) : [...hiddenClasses, cls]);
    viewerRef.current.setClassHidden(cls, !hidden);
  };
  const onResetFilters = () => {
    setFilters(EMPTY_FILTERS);
    setHiddenClasses([]);
    viewerRef.current.resetFilters();
  };
  const fmt = (v) => `${v.x.toFixed(2)}  ${v.y.toFixed(2)}  ${v.z.toFixed(2)}`;

  const isMovie = sceneId === 'movie';
  const isSeg = sceneId === 'seg';
  const isFile = sceneId === 'file';
  const isMovieLike = isMovie || isSeg; // both stream frames with transport controls
  // The tickable class legend (each swatch toggles its class id in/out of the
  // cloud): seg uses a fixed representative subset; a loaded file uses the dynamic
  // enumeration the viewer derived from its class column, where the class id is
  // the entry's index (matching scalars.classId in the viewer).
  const legendItems = isSeg
    ? SEG_LEGEND
    : (stats.classLegend || []).map((c, i) => ({ ...c, cls: i }));
  // While a file is loaded the Scene dropdown shows a temporary entry for it, so
  // its value matches sceneId and the user can still switch to a built-in scene.
  const sceneOptions = isFile
    ? [...SCENES, { id: 'file', label: stats.fileName || 'Loaded file' }]
    : SCENES;
  const progressText = stats.loadProgress.total
    ? `Loading ${stats.loadProgress.loaded} / ${stats.loadProgress.total}…`
    : 'Loading…';
  // Until the first frame is on screen the canvas is just background colour — a
  // blank black screen with no feedback. A boot loader covers that gap for every
  // scene (the static initial load reports no `loading` state of its own).
  const booting = !stats.ready && !stats.error;
  // The corner line is only for work that continues *after* first render — i.e.
  // the movie streaming its remaining frames while it already plays.
  const loadingText = stats.loading && stats.ready ? progressText : null;

  return html`
    <canvas id="scene" ref=${canvasRef}></canvas>

    ${booting && html`
      <div class="boot" data-testid="boot-loading">
        <div class="spinner"></div>
        <div>${progressText}</div>
      </div>
    `}

    <button class="menu-toggle" data-testid="menu-toggle"
            aria-label=${menuOpen ? 'Close controls' : 'Open controls'}
            onClick=${() => setMenuOpen((o) => !o)}>${menuOpen ? '✕' : '☰'}</button>

    ${menuOpen && html`
      <div class="backdrop" data-testid="backdrop" onClick=${() => setMenuOpen(false)}></div>
      <div class="panel controls" data-testid="controls">
        <h1>PCL Viewer</h1>
        <div class="tabs" role="tablist">
          <button class=${`tab${tab === 'view' ? ' active' : ''}`} role="tab"
                  data-testid="tab-view" aria-selected=${tab === 'view' ? 'true' : 'false'}
                  onClick=${() => setTab('view')}>View</button>
          <button class=${`tab${tab === 'filters' ? ' active' : ''}`} role="tab"
                  data-testid="tab-filters" aria-selected=${tab === 'filters' ? 'true' : 'false'}
                  onClick=${() => setTab('filters')}>Filters</button>
        </div>
        ${tab === 'view' && html`
          <label>Scene</label>
          <select data-testid="scene" value=${sceneId} onChange=${onScene}>
            ${sceneOptions.map((s) => html`<option value=${s.id}>${s.label}</option>`)}
          </select>
          <button class="load-pcl" data-testid="load-file" onClick=${onLoadClick}>
            <svg class="load-pcl-icon" viewBox="0 0 16 16" width="15" height="15"
                 fill="none" stroke="currentColor" stroke-width="1.5"
                 stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
              <path d="M8 10.5V2.5" />
              <path d="M4.8 5.7 8 2.5l3.2 3.2" />
              <path d="M2.5 10v2.5a1 1 0 0 0 1 1h9a1 1 0 0 0 1-1V10" />
            </svg>
            <span>Load PCL…</span>
          </button>
          <input type="file" accept=".pcd,.csv,.parquet" data-testid="file-input"
                 ref=${fileInputRef} onChange=${onFile} style="display:none" />
          ${isMovieLike && stats.frameCount > 0 && html`
            <label>Frame</label>
            <div class="frame-jump">
              <input type="number" min="1" max=${stats.frameCount} step="1"
                     value=${frame + 1} data-testid="frame-input" onInput=${onFrameInput} />
              <span>/ ${stats.frameCount}</span>
            </div>
            <input type="range" min="0" max=${stats.frameCount - 1} step="1"
                   value=${frame} data-testid="frame-select" onInput=${onSeek} />
            <div class="row">
              <button data-testid="play-pause" onClick=${onPlayPause}>
                ${stats.playing ? 'Pause' : 'Play'}
              </button>
              <button data-testid="step-back" aria-label="Previous frame"
                      onClick=${() => onStep(-1)}>−</button>
              <button data-testid="step-forward" aria-label="Next frame"
                      onClick=${() => onStep(1)}>+</button>
            </div>
          `}
          <label>Point shape</label>
          <select data-testid="point-shape" value=${pointShape} onChange=${onShape}>
            <option value="ball">Ball (3D)</option>
            <option value="square">Square</option>
          </select>
          <label>Point size: ${pointSize.toFixed(3)}</label>
          <input type="range" min="0.002" max="0.05" step="0.001"
                 value=${pointSize} data-testid="point-size" onInput=${onSize} />
          <label>Color mode</label>
          <select data-testid="color-mode" value=${colorMode} onChange=${onColor}>
            ${colorModes.map((m) => html`
              <option value=${m}>${COLOR_MODE_LABEL[m] || m}</option>`)}
          </select>
          <div class="row">
            <button data-testid="reset" onClick=${onReset}>Reset camera</button>
          </div>
        `}
        ${tab === 'filters' && html`
          <label>Point filters (min / max, blank = ∞)</label>
          ${FILTER_FIELDS
            .filter((f) => f.key !== 'intensity' || colorModes.includes('intensity'))
            .map((f) => {
              const rng = stats.scalarRanges[f.key];
              const ph = { min: rng ? rng.min.toFixed(2) : 'min', max: rng ? rng.max.toFixed(2) : '∞' };
              return html`
                <div class="filter-row" data-testid=${`filter-${f.key}`}>
                  <span class="filter-name">${f.label}</span>
                  <div class="filter-bounds">
                    ${['min', 'max'].map((bound) => html`
                      <div class="stepper">
                        <button type="button" class="step-btn"
                                data-testid=${`filter-${f.key}-${bound}-dec`}
                                aria-label=${`Decrease ${f.label} ${bound}`}
                                onClick=${() => onFilterStep(f.key, bound, -1)}>−</button>
                        <input type="number" step="any" data-testid=${`filter-${f.key}-${bound}`}
                               placeholder=${ph[bound]}
                               value=${filters[f.key][bound]}
                               onInput=${(e) => onFilter(f.key, bound, e)} />
                        <button type="button" class="step-btn"
                                data-testid=${`filter-${f.key}-${bound}-inc`}
                                aria-label=${`Increase ${f.label} ${bound}`}
                                onClick=${() => onFilterStep(f.key, bound, 1)}>+</button>
                      </div>`)}
                  </div>
                </div>`;
            })}
          <div class="row">
            <button data-testid="reset-filters" onClick=${onResetFilters}>Reset filters</button>
          </div>
          ${isSeg && html`
            <label class="row">
              <input type="checkbox" data-testid="show-boxes"
                     checked=${showBoxes} onChange=${onToggleBoxes} />
              Show boxes
            </label>
          `}
          ${legendItems.length > 0 && html`
            <label>Classes (click to filter)</label>
            <div class="legend" data-testid="legend">
              ${legendItems.map((c) => {
                const off = hiddenClasses.includes(c.cls);
                return html`
                  <button type="button" class=${`legend-item${off ? ' off' : ''}`}
                          data-testid=${`class-toggle-${c.name}`}
                          aria-pressed=${off ? 'false' : 'true'}
                          title=${off ? `Show ${c.name}` : `Hide ${c.name}`}
                          onClick=${() => onToggleClass(c.cls)}>
                    <span class="swatch" style=${`background:#${c.hex}`}></span>${c.name}
                  </button>`;
              })}
            </div>
          `}
        `}
      </div>
    `}

    <div class="panel hud" data-testid="stats">
      <div><b data-testid="point-count">${stats.pointCount.toLocaleString()}</b> pts
           · <b>${stats.fps}</b> fps · d <b>${stats.cameraDistance.toFixed(2)}</b></div>
      ${stats.visibleCount < stats.pointCount && html`
        <div data-testid="visible-count"><b>${stats.visibleCount.toLocaleString()}</b> shown (filtered)</div>`}
      ${isMovieLike && stats.frameCount > 0 && html`
        <div>frame <b data-testid="frame-index">${stats.frameIndex + 1}</b> / ${stats.frameCount}</div>`}
      ${isFile && stats.fileName && html`
        <div>file <b data-testid="file-name">${stats.fileName}</b></div>`}
      ${loadingText && html`<div data-testid="loading">${loadingText}</div>`}
      ${stats.error && html`<div class="err" data-testid="error">${stats.error}</div>`}
      <div class="vec">eye <b data-testid="cam-eye">${fmt(stats.eye)}</b></div>
      <div class="vec">tgt <b data-testid="cam-target">${fmt(stats.target)}</b></div>
    </div>
  `;
}

render(html`<${App} />`, document.getElementById('app'));
