PCL Viewer — View / Filters menu tabs
---

## Goal

Split the PCL Viewer's single long controls menu into two tabs — **View** and
**Filters** — so the panel is shorter and the two concerns are separated.

## Current state

`web/app.js` renders all controls in one vertical stack inside the `.controls`
modal: Scene selector, frame transport (movie/seg only), point shape, point
size, color mode, point range filters, reset-filters, seg show-boxes toggle +
class legend, and reset-camera. No behavioral changes are needed in
`web/viewer.js`.

## Design

A tab bar sits below the `PCL Viewer` heading with two segmented buttons,
**View** and **Filters**. New local state `tab` (`'view' | 'filters'`, default
`'view'`) selects which body renders; it persists across menu open/close. Only
the active tab's body is rendered (inactive controls leave the DOM), which
matches the tab model and keeps Playwright from trying to drive hidden inputs.

- **View tab** (default): Scene → frame transport (movie/seg) → Point shape →
  Point size → Color mode → Reset camera.
- **Filters tab**: Point range filters (height/distance/intensity steppers) →
  Reset filters → and, for the seg scene only, Show boxes toggle + class legend.

### Test IDs

Tab buttons: `data-testid="tab-view"` and `data-testid="tab-filters"`. All
existing control test IDs are unchanged — they just live under a tab now.

### CSS

A `.tabs` flex row of two buttons; the active one uses the existing blue accent,
the inactive one is muted — overriding the generic full-width blue
`.controls button` styling for these.

## Tests

E2e tests that drive Filters-tab controls (`filter-*`, `reset-filters`,
`show-boxes`, `class-toggle-*`, `legend`) click `tab-filters` first via a small
`_open_filters(page)` helper. View-tab controls stay on the default tab and only
need the menu open, as today. A new assertion confirms both tabs switch.

## Docs

One-line update to `pcl-viewer/README.md` noting the View/Filters tabs.
