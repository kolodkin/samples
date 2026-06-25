// Scene sources + movie parameters. Overridable (in priority order) by URL query
// params, then window.__PCL_CONFIG (set before module load), then these defaults.
// The overrides let e2e point scenes at local fixtures served by the test server.
const params = new URLSearchParams(location.search);
const cfg = (typeof window !== 'undefined' && window.__PCL_CONFIG) || {};

const pick = (key, def) => params.get(key) ?? cfg[key] ?? def;

export const CITY_URL = './models/kitti-velodyne-000000.pcd';

// The "shape" scene: Stanford Lucy — the famous winged-angel statue (decimated to
// ~50k vertices, binary PLY) hot-linked from the three.js model repo. A detailed,
// recognizable structure rather than a flat tabletop. Overridable via ?lucyUrl=
// (e2e points it at a local PLY fixture). The loader is picked by file extension.
export const LUCY_URL = pick(
  'lucyUrl',
  'https://raw.githubusercontent.com/mrdoob/three.js/dev/examples/models/ply/binary/Lucy100k.ply',
);

export const MOVIE_BASE = pick(
  'movieBase',
  'https://huggingface.co/datasets/kolodkin/pcl-viewer-kitti-movie/resolve/main/',
);

export const MOVIE_COUNT = parseInt(pick('movieCount', '154'), 10);

// Frame URL helper: MOVIE_BASE + zero-padded index + .drc
export const frameUrl = (i) => `${MOVIE_BASE}${String(i).padStart(6, '0')}.drc`;
