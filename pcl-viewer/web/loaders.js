// Parse user-supplied local point-cloud files into a plain intermediate the
// viewer turns into geometry. No three.js scene state lives here — the only
// three dependency is PCDLoader, used purely as a parser for the .pcd path.
//
// Accepted inputs and their column schema (tabular formats):
//   x,y,z              x,y,z,i            x,y,z,c            x,y,z,i,c
// where `i` is a numeric intensity and `c` is a per-point class *string*; the
// distinct class strings form an enumeration (first-seen order) that drives the
// "by class" color mode. A ParsedCloud is:
//   { positions: Float32Array(n*3), intensity: Float32Array(n)|null,
//     classIds: Uint16Array(n)|null, classNames: string[]|null }
import { PCDLoader } from 'three/addons/loaders/PCDLoader.js';
import { parquetReadObjects } from 'hyparquet';

// Header aliases for the optional intensity/class columns (x,y,z are literal).
const INTENSITY_NAMES = new Set(['i', 'intensity', 'reflectance', 'intensities']);
const CLASS_NAMES = new Set(['c', 'class', 'label', 'category', 'cls', 'classification']);

const ext = (name) => {
  const m = /\.([^.]+)$/.exec(name || '');
  return m ? m[1].toLowerCase() : '';
};

export async function parseLocalFile(file) {
  const kind = ext(file.name);
  if (kind === 'pcd') return parsePcd(await file.arrayBuffer());
  if (kind === 'csv') return parseCsv(await file.text());
  if (kind === 'parquet') return parseParquet(await file.arrayBuffer());
  throw new Error(`Unsupported file type ".${kind}" (use .pcd, .csv, or .parquet)`);
}

// --- PCD ---------------------------------------------------------------------
// Reuse three's PCDLoader as a pure parser. It yields position and, when the
// source declares an `intensity` field, an intensity attribute. Standard PCD
// fields are numeric, so there is no class column on this path.
function parsePcd(arrayBuffer) {
  const points = new PCDLoader().parse(arrayBuffer, '');
  const geom = points.geometry;
  const pos = geom.getAttribute('position');
  if (!pos || pos.count === 0) throw new Error('PCD file has no points');
  const positions = pos.array instanceof Float32Array && pos.itemSize === 3
    ? Float32Array.from(pos.array.subarray(0, pos.count * 3))
    : flattenVec3(pos);
  const intAttr = geom.getAttribute('intensity');
  const intensity = intAttr
    ? Float32Array.from({ length: intAttr.count }, (_, i) => intAttr.getX(i))
    : null;
  return { positions, intensity, classIds: null, classNames: null };
}

function flattenVec3(attr) {
  const out = new Float32Array(attr.count * 3);
  for (let i = 0; i < attr.count; i++) {
    out[i * 3] = attr.getX(i);
    out[i * 3 + 1] = attr.getY(i);
    out[i * 3 + 2] = attr.getZ(i);
  }
  return out;
}

// --- CSV ---------------------------------------------------------------------
function parseCsv(text) {
  const lines = text.split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0 && !l.startsWith('#'));
  if (lines.length === 0) throw new Error('CSV file is empty');

  const delim = detectDelimiter(lines[0]);
  const rows = lines.map((l) => l.split(delim).map((t) => t.trim()));
  const first = rows[0];
  // A header row is one whose first three cells are not all numeric.
  const hasHeader = first.slice(0, 3).some((t) => !isFiniteNumber(t));
  const header = hasHeader ? first.map((t) => t.toLowerCase()) : null;
  const dataRows = hasHeader ? rows.slice(1) : rows;
  if (dataRows.length === 0) throw new Error('CSV file has no data rows');

  const map = resolveColumns(header, dataRows);
  return buildFromColumns(dataRows, map, (row, idx) => row[idx]);
}

function detectDelimiter(line) {
  let best = ',', bestCount = 0;
  for (const d of [',', ';', '\t']) {
    const n = line.split(d).length - 1;
    if (n > bestCount) { best = d; bestCount = n; }
  }
  return best;
}

// --- Parquet -----------------------------------------------------------------
async function parseParquet(arrayBuffer) {
  let rows;
  try {
    rows = await parquetReadObjects({ file: asyncBuffer(arrayBuffer) });
  } catch (e) {
    throw new Error(`Could not read Parquet (uncompressed/snappy only): ${e.message || e}`);
  }
  if (!rows || rows.length === 0) throw new Error('Parquet file has no rows');
  const names = Object.keys(rows[0]).map((n) => n.toLowerCase());
  const lcRows = rows; // keep original objects; access via original keys
  const origKeys = Object.keys(rows[0]);
  const map = resolveColumns(names, rows.map((r) => origKeys.map((k) => r[k])));
  // Columns are addressed positionally below, so flatten each row to an array
  // in the original key order (matching `names`/`map`).
  return buildFromColumns(
    lcRows.map((r) => origKeys.map((k) => r[k])), map, (row, idx) => row[idx]);
}

function asyncBuffer(arrayBuffer) {
  return {
    byteLength: arrayBuffer.byteLength,
    slice(start, end) { return arrayBuffer.slice(start, end ?? arrayBuffer.byteLength); },
  };
}

// --- Shared column resolution + assembly ------------------------------------
// Decide which columns are x,y,z,i,c. Prefer header names; otherwise infer by
// column count and the 4th column's value type. Returns {x,y,z,i,c} with i/c
// possibly -1 (absent).
function resolveColumns(header, dataRows) {
  const ncols = dataRows[0] ? dataRows[0].length : 0;
  if (header) {
    const idx = (set, lit) => {
      const byName = header.findIndex((n) => set.has(n));
      return byName >= 0 ? byName : header.indexOf(lit);
    };
    const x = header.indexOf('x'), y = header.indexOf('y'), z = header.indexOf('z');
    if (x >= 0 && y >= 0 && z >= 0) {
      return {
        x, y, z,
        i: header.findIndex((n) => INTENSITY_NAMES.has(n)),
        c: header.findIndex((n) => CLASS_NAMES.has(n)),
      };
    }
    // Header present but not x/y/z named — fall through to positional.
  }
  if (ncols < 3) throw new Error('Need at least x,y,z columns');
  if (ncols === 3) return { x: 0, y: 1, z: 2, i: -1, c: -1 };
  if (ncols === 4) {
    // 4th column: numeric across sampled rows ⇒ intensity, else class.
    return columnLooksNumeric(dataRows, 3)
      ? { x: 0, y: 1, z: 2, i: 3, c: -1 }
      : { x: 0, y: 1, z: 2, i: -1, c: 3 };
  }
  return { x: 0, y: 1, z: 2, i: 3, c: 4 }; // >=5: extras ignored
}

function columnLooksNumeric(rows, idx) {
  let seen = 0;
  for (let r = 0; r < rows.length && seen < 20; r++) {
    const v = rows[r][idx];
    if (v === undefined || v === null || v === '') continue;
    seen++;
    if (!isFiniteNumber(v)) return false;
  }
  return seen > 0;
}

// Walk the rows once, pulling x/y/z (required, finite) plus optional intensity
// and class. `get(row, idx)` reads a cell. Rows with a non-finite x/y/z are
// skipped. Class strings are enumerated in first-seen order.
function buildFromColumns(rows, map, get) {
  const positions = [];
  const intensity = map.i >= 0 ? [] : null;
  const wantClass = map.c >= 0;
  const classIdList = wantClass ? [] : null;
  const classIndex = new Map();
  const classNames = wantClass ? [] : null;

  for (const row of rows) {
    const x = toNum(get(row, map.x));
    const y = toNum(get(row, map.y));
    const z = toNum(get(row, map.z));
    if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z)) continue;
    positions.push(x, y, z);
    if (intensity) intensity.push(toNum(get(row, map.i)) || 0);
    if (wantClass) {
      const label = String(get(row, map.c) ?? '').trim() || '(none)';
      let id = classIndex.get(label);
      if (id === undefined) { id = classNames.length; classIndex.set(label, id); classNames.push(label); }
      classIdList.push(id);
    }
  }

  if (positions.length === 0) throw new Error('No valid x,y,z rows found');
  return {
    positions: Float32Array.from(positions),
    intensity: intensity ? Float32Array.from(intensity) : null,
    classIds: classIdList ? Uint16Array.from(classIdList) : null,
    classNames,
  };
}

function isFiniteNumber(v) {
  if (typeof v === 'number') return Number.isFinite(v);
  if (v === null || v === undefined || v === '') return false;
  return Number.isFinite(Number(v));
}

function toNum(v) {
  return typeof v === 'number' ? v : Number(v);
}
