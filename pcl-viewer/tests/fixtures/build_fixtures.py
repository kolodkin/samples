"""Build tiny Draco movie fixtures from the committed KITTI frame.

Run once (commit the outputs): produces 4 heavily-decimated .drc frames with a
small synthetic translation between them, so the e2e movie test has a real, light,
offline sequence. Requires the `gen` group:
    uv run --group gen python tests/fixtures/build_fixtures.py
"""
from __future__ import annotations

import struct
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


def build_lucy_fixture() -> None:
    """Tiny PLY mesh fixture for the offline Lucy (object-profile) e2e.

    A small Fibonacci sphere with throwaway triangle faces: enough to make the PLY
    an *indexed mesh* (like the real Lucy), so the PLY loader path — index stripped,
    unique vertices rendered as points — is exercised without committing the 1.9 MB
    Lucy cloud. Written as `binary_little_endian` to match the real Lucy's encoding
    (so the e2e drives the same binary PLY parser) and to keep the committed fixture
    a compact ~65 KB blob rather than thousands of lines of ASCII."""
    out = HERE / "lucy"
    out.mkdir(parents=True, exist_ok=True)
    n = 4000
    i = np.arange(n)
    y = 1.0 - 2.0 * (i + 0.5) / n
    r = np.sqrt(np.maximum(0.0, 1.0 - y * y))
    theta = np.pi * (3.0 - np.sqrt(5.0)) * i  # golden angle
    verts = np.stack([r * np.cos(theta), y, r * np.sin(theta)], axis=1).astype("<f4")
    faces = np.array([(a, a + 1, a + 2) for a in range(0, n - 2, 3)], dtype="<i4")
    header = (
        "ply\n"
        "format binary_little_endian 1.0\n"
        f"element vertex {n}\n"
        "property float x\nproperty float y\nproperty float z\n"
        f"element face {len(faces)}\n"
        "property list uchar int vertex_indices\n"
        "end_header\n"
    ).encode("ascii")
    body = bytearray(verts.tobytes())
    for f in faces:
        body += struct.pack("<B", 3) + f.tobytes()  # uchar count + 3 int32 indices
    path = out / "lucy_fixture.ply"
    path.write_bytes(header + bytes(body))
    print(f"wrote {path}  ({path.stat().st_size} bytes, {n} verts, {len(faces)} faces)")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    xyz = read_binary_pcd_xyz(SRC)
    base = voxel_downsample(xyz, 0.8)  # ~a few thousand points -> tiny .drc
    for i in range(4):
        frame = base + np.array([i * 0.5, 0.0, 0.0], dtype=np.float32)  # roll forward
        buf = DracoPy.encode(frame.astype(np.float32), quantization_bits=14)
        (OUT / f"{i:06d}.drc").write_bytes(buf)
        print(f"wrote {OUT / f'{i:06d}.drc'}  ({len(buf)} bytes, {len(frame)} pts)")
    build_lucy_fixture()


if __name__ == "__main__":
    main()
