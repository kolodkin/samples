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

// A few representative classes for the seg-scene legend (hex matches SEG_PALETTE).
const SEG_LEGEND = [
  { name: 'car', hex: '6496F5' }, { name: 'person', hex: 'FF1E1E' },
  { name: 'road', hex: 'FF00FF' }, { name: 'sidewalk', hex: '4B004B' },
  { name: 'building', hex: 'FFC800' }, { name: 'vegetation', hex: '00AF00' },
  { name: 'pole', hex: 'FFF096' }, { name: 'traffic-sign', hex: 'FF0000' },
];

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
  const [frame, setFrame] = useState(0);
  const [showBoxes, setShowBoxes] = useState(true);
  const origin = { x: 0, y: 0, z: 0 }; // placeholder until the first stats tick
  const [stats, setStats] = useState({
    ready: false, pointCount: 0, fps: 0, cameraDistance: 0, eye: origin, target: origin,
    scene: 'movie', frameIndex: 0, frameCount: 0, playing: false,
    loading: false, loadProgress: { loaded: 0, total: 0 }, error: null,
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
    setSceneId(id); viewerRef.current.loadScene(id);
    // Per-scene color defaults (e.g. seg → "by class") live in the viewer and
    // arrive via onColorState once the new scene's first frame installs.
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
  const fmt = (v) => `${v.x.toFixed(2)}  ${v.y.toFixed(2)}  ${v.z.toFixed(2)}`;

  const isMovie = sceneId === 'movie';
  const isSeg = sceneId === 'seg';
  const isMovieLike = isMovie || isSeg; // both stream frames with transport controls
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
        <label>Scene</label>
        <select data-testid="scene" value=${sceneId} onChange=${onScene}>
          ${SCENES.map((s) => html`<option value=${s.id}>${s.label}</option>`)}
        </select>
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
        ${isSeg && html`
          <label class="row">
            <input type="checkbox" data-testid="show-boxes"
                   checked=${showBoxes} onChange=${onToggleBoxes} />
            Show boxes
          </label>
          <div class="legend" data-testid="legend">
            ${SEG_LEGEND.map((c) => html`
              <span class="legend-item">
                <span class="swatch" style=${`background:#${c.hex}`}></span>${c.name}
              </span>`)}
          </div>
        `}
        <div class="row">
          <button data-testid="reset" onClick=${onReset}>Reset camera</button>
        </div>
      </div>
    `}

    <div class="panel hud" data-testid="stats">
      <div><b data-testid="point-count">${stats.pointCount.toLocaleString()}</b> pts
           · <b>${stats.fps}</b> fps · d <b>${stats.cameraDistance.toFixed(2)}</b></div>
      ${isMovieLike && stats.frameCount > 0 && html`
        <div>frame <b data-testid="frame-index">${stats.frameIndex + 1}</b> / ${stats.frameCount}</div>`}
      ${loadingText && html`<div data-testid="loading">${loadingText}</div>`}
      ${stats.error && html`<div class="err" data-testid="error">${stats.error}</div>`}
      <div class="vec">eye <b data-testid="cam-eye">${fmt(stats.eye)}</b></div>
      <div class="vec">tgt <b data-testid="cam-target">${fmt(stats.target)}</b></div>
    </div>
  `;
}

render(html`<${App} />`, document.getElementById('app'));
