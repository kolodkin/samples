import { h, render } from 'preact';
import { useEffect, useState } from 'preact/hooks';
import htm from 'htm';

const html = htm.bind(h);

export function createStore(initial) {
  let state = { ...initial };
  const subs = new Set();
  return {
    get: () => state,
    set(patch) {
      state = { ...state, ...patch };
      subs.forEach((fn) => fn(state));
    },
    subscribe(fn) {
      subs.add(fn);
      return () => subs.delete(fn);
    },
  };
}

function useStore(store) {
  const [s, setS] = useState(store.get());
  useEffect(() => {
    setS(store.get()); // catch any set() that fired before this effect subscribed
    return store.subscribe(setS);
  }, [store]);
  return s;
}

const SLOTS = [
  ['normal', 'Normal', '1'],
  ['exploding', 'Explode', '2'],
  ['freezing', 'Freeze', '3'],
  ['burning', 'Burn', '4'],
];

function Hud({ s }) {
  return html`
    <div class="hud" data-testid="hud">
      <div class="top-left">
        <div class="hp-bar"><div class="hp-fill" style="width:${(100 * s.hp) / s.maxHp}%" /></div>
        <div class="hp-text" data-testid="hp-text">${s.hp} HP</div>
      </div>
      <div class="top-right">
        <div class="score" data-testid="score">${s.score}</div>
        <div class="wave" data-testid="wave">${s.stage} — wave ${s.wave}/${s.totalWaves}</div>
      </div>
      <div class="quiver">
        ${SLOTS.map(([type, label, key]) => html`
          <div
            class="slot ${s.selected === type ? 'active' : ''} ${type !== 'normal' && !s.ammo[type] ? 'empty' : ''}"
            data-testid="slot-${type}"
          >
            <span class="key">${key}</span>
            <span class="label">${label}</span>
            <span class="count" data-testid="ammo-${type}">${type === 'normal' ? '∞' : s.ammo[type]}</span>
          </div>
        `)}
      </div>
      <div class="crosshair"><div class="draw-ring" /></div>
    </div>`;
}

function Screen({ testid, title, children }) {
  return html`
    <div class="screen" data-testid=${testid}>
      <h1>${title}</h1>
      ${children}
    </div>`;
}

function Screens({ s, actions }) {
  if (s.screen === 'playing') return null;
  if (s.screen === 'title') {
    return html`
      <${Screen} testid="title-screen" title="ARCHER">
        <p>Hold to draw, release to loose. Keys 1–4 switch arrows.</p>
        <p data-testid="best">Best: ${s.best.score} pts, stage ${s.best.stage}/3</p>
        <button data-testid="start-btn" onClick=${actions.start}>Start</button>
      <//>`;
  }
  if (s.screen === 'paused') {
    return html`
      <${Screen} testid="pause-screen" title="Paused">
        <button data-testid="resume-btn" onClick=${actions.resume}>Resume</button>
      <//>`;
  }
  if (s.screen === 'stageClear') {
    return html`
      <${Screen} testid="stageclear-screen" title="Stage cleared!">
        <p>Score: ${s.score}</p>
        <button data-testid="next-btn" onClick=${actions.next}>Next stage</button>
      <//>`;
  }
  if (s.screen === 'victory') {
    return html`
      <${Screen} testid="victory-screen" title="All three lands defended!">
        <p>Final score: ${s.score}</p>
        <button data-testid="restart-btn" onClick=${actions.restart}>Play again</button>
      <//>`;
  }
  return html`
    <${Screen} testid="gameover-screen" title="You fell.">
      <p>Score: ${s.score}</p>
      <button data-testid="retry-btn" onClick=${actions.retry}>Retry stage</button>
    <//>`;
}

function App({ store, actions }) {
  const s = useStore(store);
  if (!s.screen) return null; // before the first syncUI
  return html`
    <div class="ui-root">
      ${s.screen !== 'title' && html`<${Hud} s=${s} />`}
      <${Screens} s=${s} actions=${actions} />
    </div>`;
}

export function initUI(store, actions) {
  render(html`<${App} store=${store} actions=${actions} />`, document.getElementById('ui'));
}
