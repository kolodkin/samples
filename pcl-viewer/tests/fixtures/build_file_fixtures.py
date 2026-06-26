"""Build the offline "Load PCL" fixtures (csv / csv-no-header / pcd / parquet).

One deterministic synthetic cloud written in four shapes so the e2e exercises
every loader + every schema variant without network. Three labeled clusters give
the class enumeration something to color. Run once and commit the outputs:
    uv run --group gen python tests/fixtures/build_file_fixtures.py
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
OUT = HERE / "file"

CLASSES = ["car", "tree", "road"]


def make_cloud() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Three offset gaussian blobs → (xyz[N,3], intensity[N], class_id[N])."""
    rng = np.random.default_rng(7)
    centers = np.array([[0.0, 0.0, 0.0], [3.0, 0.5, 1.0], [-2.0, -1.0, 2.0]])
    per = 700
    xyz, inten, cls = [], [], []
    for cid, ctr in enumerate(centers):
        pts = ctr + rng.normal(scale=0.6, size=(per, 3))
        xyz.append(pts)
        # intensity correlates with class so "by intensity" also varies visibly
        inten.append(rng.uniform(cid * 0.3, cid * 0.3 + 0.3, size=per))
        cls.append(np.full(per, cid))
    return (
        np.concatenate(xyz).astype(np.float32),
        np.concatenate(inten).astype(np.float32),
        np.concatenate(cls),
    )


def write_csv(path: Path, xyz, inten, names, header: bool) -> None:
    lines = []
    if header:
        lines.append("x,y,z,i,c")
    for (x, y, z), i, c in zip(xyz, inten, names):
        lines.append(f"{x:.4f},{y:.4f},{z:.4f},{i:.4f},{c}")
    path.write_text("\n".join(lines) + "\n")
    print(f"wrote {path}  ({path.stat().st_size} bytes, {len(xyz)} pts)")


def write_pcd(path: Path, xyz, inten) -> None:
    n = len(xyz)
    header = (
        "# .PCD v0.7 - Point Cloud Data file format\n"
        "VERSION 0.7\n"
        "FIELDS x y z intensity\n"
        "SIZE 4 4 4 4\nTYPE F F F F\nCOUNT 1 1 1 1\n"
        f"WIDTH {n}\nHEIGHT 1\nVIEWPOINT 0 0 0 1 0 0 0\n"
        f"POINTS {n}\nDATA ascii\n"
    )
    body = "".join(
        f"{x:.4f} {y:.4f} {z:.4f} {i:.4f}\n" for (x, y, z), i in zip(xyz, inten)
    )
    path.write_text(header + body)
    print(f"wrote {path}  ({path.stat().st_size} bytes, {n} pts)")


def write_parquet(path: Path, xyz, inten, names) -> None:
    table = pa.table({
        "x": xyz[:, 0], "y": xyz[:, 1], "z": xyz[:, 2],
        "i": inten, "c": names,
    })
    # SNAPPY is hyparquet's other built-in codec besides uncompressed; use it so
    # the browser-side snappy decode path is exercised by the e2e.
    pq.write_table(table, path, compression="snappy")
    print(f"wrote {path}  ({path.stat().st_size} bytes, {len(xyz)} pts)")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    xyz, inten, cls = make_cloud()
    names = [CLASSES[c] for c in cls]
    write_csv(OUT / "cloud.csv", xyz, inten, names, header=True)
    write_csv(OUT / "cloud_noheader.csv", xyz, inten, names, header=False)
    write_pcd(OUT / "cloud.pcd", xyz, inten)
    write_parquet(OUT / "cloud.parquet", xyz, inten, names)


if __name__ == "__main__":
    main()
