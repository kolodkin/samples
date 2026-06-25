// Scene sources + movie parameters. Overridable (in priority order) by URL query
// params, then window.__PCL_CONFIG (set before module load), then these defaults.
// The overrides let e2e point scenes at local fixtures served by the test server.
const params = new URLSearchParams(location.search);
const cfg = (typeof window !== 'undefined' && window.__PCL_CONFIG) || {};

const pick = (key, def) => params.get(key) ?? cfg[key] ?? def;

export const CITY_URL = './models/kitti-velodyne-000000.pcd';

export const PCL_URL = pick(
  'pclUrl',
  'https://raw.githubusercontent.com/PointCloudLibrary/data/master/tutorials/table_scene_lms400.pcd',
);

export const MOVIE_BASE = pick(
  'movieBase',
  'https://huggingface.co/datasets/kolodkin/pcl-viewer-kitti-movie/resolve/main/geometry/',
);

export const MOVIE_COUNT = parseInt(pick('movieCount', '154'), 10);

// Frame URL helper: MOVIE_BASE + zero-padded index + .drc
export const frameUrl = (i) => `${MOVIE_BASE}${String(i).padStart(6, '0')}.drc`;

export const SEG_MOVIE_BASE = pick(
  'segMovieBase',
  'https://huggingface.co/datasets/kolodkin/pcl-viewer-kitti-movie/resolve/main/seg/',
);

export const SEG_MOVIE_COUNT = parseInt(pick('segMovieCount', '150'), 10);

export const SEG_BOXES_URL = pick('segBoxesUrl', `${SEG_MOVIE_BASE}boxes.json`);

// Seg frame URL helper: SEG_MOVIE_BASE + zero-padded index + .drc
export const segFrameUrl = (i) => `${SEG_MOVIE_BASE}${String(i).padStart(6, '0')}.drc`;
