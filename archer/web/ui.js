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

// Quiver slots in HUD order — also the digit-key order (main.js derives its
// key map from this). 'auto' is a mode, not an ammo type: it picks per shot
// from the battlefield (see autoType in main.js), so it has no count.
export const SLOTS = [
  ['auto', 'Auto', '✨'],
  ['normal', 'Normal', '🏹'],
  ['exploding', 'Explode', '💥'],
  ['freezing', 'Freeze', '❄️'],
  ['burning', 'Burn', '🔥'],
];

// HUD button press handling: preventDefault stops the synthetic mouse events
// a tap would otherwise generate (and, on context-menu, the long-press menu).
const press = (fn) => (e) => { e.preventDefault(); fn(); };
const noMenu = (e) => e.preventDefault();

// Auto highlights while it's the chosen mode; an ammo slot highlights while
// it's what the next shot fires — so in auto mode the auto-picked type
// lights up alongside Auto, and a pinned slot lights up alone.
function slotClass(s, type) {
  const active = type === 'auto' ? s.mode === 'auto' : s.selected === type;
  const empty = type !== 'auto' && type !== 'normal' && !s.ammo[type];
  return `slot ${active ? 'active' : ''} ${empty ? 'empty' : ''}`;
}

// The one fire control on touch.
function TouchControls({ actions }) {
  return html`
    <button
      class="fire-btn" data-testid="fire-btn" aria-label="Shoot"
      onPointerDown=${press(actions.fire)} onContextMenu=${noMenu}
    ><span class="fire-ring" />🏹</button>`;
}

// Shot power stepper in the left thumb zone. HUD buttons cover touch and
// unlocked-mouse play; +/- keys mirror them under pointer lock (main.js).
function PowerControls({ s, actions }) {
  return html`
    <div class="power-controls">
      <button class="power-btn" data-testid="power-up" aria-label="Increase power"
        onPointerDown=${press(() => actions.power(1))} onContextMenu=${noMenu}>+</button>
      <div class="power-value" data-testid="power-value">${Math.round(s.power * 100)}%</div>
      <button class="power-btn" data-testid="power-down" aria-label="Decrease power"
        onPointerDown=${press(() => actions.power(-1))} onContextMenu=${noMenu}>−</button>
    </div>`;
}

function Hud({ s, actions }) {
  return html`
    <div class="hud" data-testid="hud">
      <div class="top-left">
        <canvas id="radar" class="radar" data-testid="radar" />
        <div class="hp-bar"><div class="hp-fill" style="width:${(100 * s.hp) / s.maxHp}%" /></div>
        <div class="hp-text" data-testid="hp-text">${s.hp} HP</div>
      </div>
      <div class="top-right">
        <div class="score" data-testid="score">${s.score}</div>
        <div class="wave" data-testid="wave">${s.stage} — wave ${s.wave}/${s.totalWaves}</div>
      </div>
      ${s.touch && html`
        <button class="pause-btn" data-testid="pause-btn" aria-label="Pause"
          onClick=${actions.pause}>❚❚</button>`}
      <${PowerControls} s=${s} actions=${actions} />
      <div class="quiver">
        ${SLOTS.map(([type, label, icon]) => html`
          <button
            class=${slotClass(s, type)}
            data-testid="slot-${type}" title=${label} aria-label=${label}
            onPointerDown=${press(() => actions.selectAmmo(type))} onContextMenu=${noMenu}
          >
            <span class="icon" aria-hidden="true">${icon}</span>
            ${type !== 'auto' && html`
              <span class="count" data-testid="ammo-${type}">${type === 'normal' ? '∞' : s.ammo[type]}</span>`}
          </button>
        `)}
      </div>
      ${s.touch && html`<${TouchControls} actions=${actions} />`}
      <div class="crosshair"><div class="power-ring" /></div>
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
        <p>Click to take aim, then click to shoot. Set power with the +/− buttons (or +/− keys). The ✨ Auto slot fires the strongest arrow in your quiver first; click a quiver slot (or press 1–5) to choose the arrow yourself.</p>
        <p>On touch: drag to aim, the 🏹 button shoots.</p>
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
      ${s.screen !== 'title' && html`<${Hud} s=${s} actions=${actions} />`}
      <${Screens} s=${s} actions=${actions} />
    </div>`;
}

export function initUI(store, actions) {
  render(html`<${App} store=${store} actions=${actions} />`, document.getElementById('ui'));
}
