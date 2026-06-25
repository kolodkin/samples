"""Build tiny seg movie fixtures from the committed KITTI frame.

Run once (commit the outputs): produces 4 heavily-decimated .drc frames whose
per-point COLOR attribute red channel carries a synthetic learning-class id, plus
a boxes.json with one moving box per frame. Gives the seg e2e a real, light,
offline sequence with both per-point classes and per-frame boxes.

    uv run --group gen python tests/fixtures/build_seg_fixtures.py
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import DracoPy

HERE = Path(__file__).resolve().parent
SRC = HERE.parent.parent / "web" / "models" / "kitti-velodyne-000000.pcd"
OUT = HERE / "seg"


def read_binary_pcd_xyz(path: Path) -> np.ndarray:
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
    base = voxel_downsample(xyz, 0.8)
    # Synthetic per-point class: ground-ish (low z) -> road(9); high -> building(13);
    # a small cluster near +x -> car(1) so "By class" shows >=3 colors.
    boxes = {}
    for i in range(4):
        frame = base + np.array([i * 0.5, 0.0, 0.0], dtype=np.float32)
        cls = np.full(len(frame), 9, dtype=np.uint8)            # road default
        cls[frame[:, 2] > np.median(frame[:, 2])] = 13          # building
        near = (np.abs(frame[:, 0] - (3.0 + i * 0.5)) < 1.5) & (np.abs(frame[:, 1]) < 1.5)
        cls[near] = 1                                           # car
        colors = np.zeros((len(frame), 3), dtype=np.uint8)
        colors[:, 0] = cls
        buf = DracoPy.encode(frame.astype(np.float32), colors=colors, quantization_bits=14)
        (OUT / f"{i:06d}.drc").write_bytes(buf)
        # One car box that rolls forward with the frame, in the source velodyne frame.
        boxes[f"{i:06d}"] = [
            {"cls": 1, "center": [3.0 + i * 0.5, 0.0, -1.0], "size": [4.0, 2.0, 1.6]}
        ]
        print(f"wrote {OUT / f'{i:06d}.drc'}  ({len(buf)} bytes, {len(frame)} pts)")
    (OUT / "boxes.json").write_text(json.dumps(boxes))
    print(f"wrote {OUT / 'boxes.json'} ({len(boxes)} frames)")


if __name__ == "__main__":
    main()
