// Preact UI (no JSX build — htm). Owns UI state and drives the viewer handle.
import { h, render } from 'preact';
import { useEffect, useRef, useState } from 'preact/hooks';
import htm from 'htm';
import { createViewer } from './viewer.js';

const html = htm.bind(h);

const SCENES = [
  { id: 'city', label: 'KITTI city view' },
  { id: 'table', label: 'PCL table scene' },
  { id: 'movie', label: 'KITTI movie' },
];

function App() {
  const canvasRef = useRef(null);
  const viewerRef = useRef(null);
  const [pointSize, setPointSize] = useState(0.004);
  const [colorMode, setColorMode] = useState('height');
  const [sceneId, setSceneId] = useState('city');
  const [menuOpen, setMenuOpen] = useState(false);
  const origin = { x: 0, y: 0, z: 0 }; // placeholder until the first stats tick
  const [stats, setStats] = useState({
    pointCount: 0, fps: 0, cameraDistance: 0, eye: origin, target: origin,
    scene: 'city', frameIndex: 0, frameCount: 0, playing: false,
    loading: false, loadProgress: { loaded: 0, total: 0 }, error: null,
  });

  useEffect(() => {
    const viewer = createViewer(canvasRef.current);
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

  const onSize = (e) => {
    const v = parseFloat(e.target.value);
    setPointSize(v); viewerRef.current.setPointSize(v);
  };
  const onColor = (e) => {
    setColorMode(e.target.value); viewerRef.current.setColorMode(e.target.value);
  };
  const onScene = (e) => {
    const id = e.target.value;
    setSceneId(id); viewerRef.current.loadScene(id);
  };
  const onPlayPause = () => {
    if (stats.playing) viewerRef.current.pause(); else viewerRef.current.play();
  };
  const onReset = () => viewerRef.current.resetCamera();
  const fmt = (v) => `${v.x.toFixed(2)}  ${v.y.toFixed(2)}  ${v.z.toFixed(2)}`;

  const isMovie = sceneId === 'movie';
  const loadingText = stats.loading
    ? `Loading ${stats.loadProgress.loaded} / ${stats.loadProgress.total}…`
    : null;

  return html`
    <canvas id="scene" ref=${canvasRef}></canvas>

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
        ${isMovie && html`
          <div class="row">
            <button data-testid="play-pause" onClick=${onPlayPause}>
              ${stats.playing ? 'Pause' : 'Play'}
            </button>
          </div>
        `}
        <label>Point size: ${pointSize.toFixed(3)}</label>
        <input type="range" min="0.002" max="0.05" step="0.001"
               value=${pointSize} data-testid="point-size" onInput=${onSize} />
        <label>Color mode</label>
        <select data-testid="color-mode" value=${colorMode} onChange=${onColor}>
          <option value="flat">Flat</option>
          <option value="height">By height</option>
        </select>
        <div class="row">
          <button data-testid="reset" onClick=${onReset}>Reset camera</button>
        </div>
      </div>
    `}

    <div class="panel hud" data-testid="stats">
      <div><b data-testid="point-count">${stats.pointCount.toLocaleString()}</b> pts
           · <b>${stats.fps}</b> fps · d <b>${stats.cameraDistance.toFixed(2)}</b></div>
      ${isMovie && stats.frameCount > 0 && html`
        <div>frame <b data-testid="frame-index">${stats.frameIndex + 1}</b> / ${stats.frameCount}</div>`}
      ${loadingText && html`<div data-testid="loading">${loadingText}</div>`}
      ${stats.error && html`<div class="err" data-testid="error">${stats.error}</div>`}
      <div class="vec">eye <b data-testid="cam-eye">${fmt(stats.eye)}</b></div>
      <div class="vec">tgt <b data-testid="cam-target">${fmt(stats.target)}</b></div>
    </div>
  `;
}

render(html`<${App} />`, document.getElementById('app'));
