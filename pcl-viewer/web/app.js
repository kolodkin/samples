// Preact UI (no JSX build — htm). Owns UI state and drives the viewer handle.
import { h, render } from 'preact';
import { useEffect, useRef, useState } from 'preact/hooks';
import htm from 'htm';
import { createViewer } from './viewer.js';

const html = htm.bind(h);

function App() {
  const canvasRef = useRef(null);
  const viewerRef = useRef(null);
  const [pointSize, setPointSize] = useState(0.01);
  const [colorMode, setColorMode] = useState('height');
  const [showHelpers, setShowHelpers] = useState(false);
  const [stats, setStats] = useState({ pointCount: 0, fps: 0, cameraDistance: 0 });

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
        setStats({ pointCount: s.pointCount, fps, cameraDistance: s.cameraDistance });
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
  const onHelpers = (e) => {
    setShowHelpers(e.target.checked); viewerRef.current.toggleHelpers(e.target.checked);
  };
  const onReset = () => viewerRef.current.resetCamera();

  return html`
    <canvas id="scene" ref=${canvasRef}></canvas>
    <div class="panel controls">
      <h1>PCL Viewer</h1>
      <label>Point size: ${pointSize.toFixed(3)}</label>
      <input type="range" min="0.002" max="0.05" step="0.001"
             value=${pointSize} data-testid="point-size" onInput=${onSize} />
      <label>Color mode</label>
      <select data-testid="color-mode" value=${colorMode} onChange=${onColor}>
        <option value="flat">Flat</option>
        <option value="height">By height</option>
      </select>
      <div class="toggle">
        <input type="checkbox" id="helpers" data-testid="helpers"
               checked=${showHelpers} onChange=${onHelpers} />
        <label for="helpers" style="margin:0">Show box + axes</label>
      </div>
      <div class="row">
        <button data-testid="reset" onClick=${onReset}>Reset camera</button>
      </div>
    </div>
    <div class="panel stats" data-testid="stats">
      <div>Points: <b data-testid="point-count">${stats.pointCount.toLocaleString()}</b></div>
      <div>FPS: <b>${stats.fps}</b></div>
      <div>Camera dist: <b>${stats.cameraDistance.toFixed(2)}</b></div>
    </div>
  `;
}

render(html`<${App} />`, document.getElementById('app'));
