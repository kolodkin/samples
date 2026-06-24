// Scene sources + movie parameters. Overridable (in priority order) by URL query
// params, then window.__PCL_CONFIG (set before module load), then these defaults.
// The overrides let e2e point scenes at local fixtures served by the test server.
const params = new URLSearchParams(location.search);
const cfg = (typeof window !== 'undefined' && window.__PCL_CONFIG) || {};

const pick = (key, def) => params.get(key) ?? cfg[key] ?? def;

export const CITY_URL = './models/kitti-velodyne-000000.pcd';

// The "PCL shape" scene: a BIWI face scan (100k points, binary PCD) hot-linked
// from the PointCloudLibrary data repo — a recognizable 3D shape rather than a
// flat tabletop. Overridable via ?pclUrl= (e2e points it at a local model).
export const PCL_URL = pick(
  'pclUrl',
  'https://raw.githubusercontent.com/PointCloudLibrary/data/master/biwi_face_database/model.pcd',
);

export const MOVIE_BASE = pick(
  'movieBase',
  'https://huggingface.co/datasets/kolodkin/pcl-viewer-kitti-movie/resolve/main/',
);

export const MOVIE_COUNT = parseInt(pick('movieCount', '154'), 10);

// Frame URL helper: MOVIE_BASE + zero-padded index + .drc
export const frameUrl = (i) => `${MOVIE_BASE}${String(i).padStart(6, '0')}.drc`;
