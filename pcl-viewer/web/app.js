// Preact UI (no JSX build — htm). Owns UI state and drives the viewer handle.
import { h, render } from 'preact';
import { useEffect, useRef, useState } from 'preact/hooks';
import htm from 'htm';
import { createViewer } from './viewer.js';

const html = htm.bind(h);

function App() {
  const canvasRef = useRef(null);
  const viewerRef = useRef(null);
  const [pointSize, setPointSize] = useState(0.004);
  const [colorMode, setColorMode] = useState('height');
  const [menuOpen, setMenuOpen] = useState(false);
  const origin = { x: 0, y: 0, z: 0 }; // placeholder until the first stats tick
  const [stats, setStats] = useState({
    pointCount: 0, fps: 0, cameraDistance: 0, eye: origin, target: origin,
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
        setStats({
          pointCount: s.pointCount, fps, cameraDistance: s.cameraDistance,
          eye: s.eye, target: s.target,
        });
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
  const onReset = () => viewerRef.current.resetCamera();
  const fmt = (v) => `${v.x.toFixed(2)}  ${v.y.toFixed(2)}  ${v.z.toFixed(2)}`;

  return html`
    <canvas id="scene" ref=${canvasRef}></canvas>

    <button class="menu-toggle" data-testid="menu-toggle"
            aria-label=${menuOpen ? 'Close controls' : 'Open controls'}
            onClick=${() => setMenuOpen((o) => !o)}>${menuOpen ? '✕' : '☰'}</button>

    ${menuOpen && html`
      <div class="backdrop" data-testid="backdrop" onClick=${() => setMenuOpen(false)}></div>
      <div class="panel controls" data-testid="controls">
        <h1>PCL Viewer</h1>
        <label>Point size: ${pointSize.toFixed(3)}</label>
        <input type="range" min="0.002" max="0.05" step="0.001"
               value=${pointSize} data-testid="point-size" onInput=${onSize} />
        <label>Color mode</label>
        <select data-testid="color-mode" value=${colorMode} onChange=${onColor}>
          <option value="flat">Flat</option>
          <option value="height">By height</option>
          <option value="distance">By distance</option>
          <option value="intensity">By intensity</option>
        </select>
        <div class="row">
          <button data-testid="reset" onClick=${onReset}>Reset camera</button>
        </div>
      </div>
    `}

    <div class="panel hud" data-testid="stats">
      <div><b data-testid="point-count">${stats.pointCount.toLocaleString()}</b> pts
           · <b>${stats.fps}</b> fps · d <b>${stats.cameraDistance.toFixed(2)}</b></div>
      <div class="vec">eye <b data-testid="cam-eye">${fmt(stats.eye)}</b></div>
      <div class="vec">tgt <b data-testid="cam-target">${fmt(stats.target)}</b></div>
    </div>
  `;
}

render(html`<${App} />`, document.getElementById('app'));
