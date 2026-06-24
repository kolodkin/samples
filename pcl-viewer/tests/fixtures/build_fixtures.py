"""Build tiny Draco movie fixtures from the committed KITTI frame.

Run once (commit the outputs): produces 4 heavily-decimated .drc frames with a
small synthetic translation between them, so the e2e movie test has a real, light,
offline sequence. Requires the `gen` group:
    uv run --group gen python tests/fixtures/build_fixtures.py
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import DracoPy

HERE = Path(__file__).resolve().parent
SRC = HERE.parent.parent / "web" / "models" / "kitti-velodyne-000000.pcd"
OUT = HERE / "movie"


def read_binary_pcd_xyz(path: Path) -> np.ndarray:
    """Minimal binary-PCD reader for FIELDS x y z intensity (float32)."""
    data = path.read_bytes()
    marker = b"DATA binary\n"
    header_end = data.index(marker) + len(marker)
    header = data[:header_end].decode("ascii", "replace")
    count = next(int(line.split()[1]) for line in header.splitlines() if line.startswith("POINTS"))
    body = np.frombuffer(data[header_end:header_end + count * 16], dtype=np.float32).reshape(-1, 4)
    return body[:, :3].copy()


def voxel_downsample(pts: np.ndarray, voxel: float) -> np.ndarray:
    keys = np.floor(pts / voxel).astype(np.int64)
    _, idx = np.unique(keys, axis=0, return_index=True)
    return pts[np.sort(idx)]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    xyz = read_binary_pcd_xyz(SRC)
    base = voxel_downsample(xyz, 0.8)  # ~a few thousand points -> tiny .drc
    for i in range(4):
        frame = base + np.array([i * 0.5, 0.0, 0.0], dtype=np.float32)  # roll forward
        buf = DracoPy.encode(frame.astype(np.float32), quantization_bits=14)
        (OUT / f"{i:06d}.drc").write_bytes(buf)
        print(f"wrote {OUT / f'{i:06d}.drc'}  ({len(buf)} bytes, {len(frame)} pts)")


if __name__ == "__main__":
    main()
