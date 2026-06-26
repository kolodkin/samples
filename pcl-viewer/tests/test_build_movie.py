"""Offline round-trip test for the geometry movie encoder: intensity must survive
into the Draco color attribute's green channel. Needs the `gen` deps; run with
`uv run --group gen pytest tests/test_build_movie.py`."""
import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("DracoPy")
pytest.importorskip("requests")
pytest.importorskip("huggingface_hub")
import DracoPy  # noqa: E402
import numpy as np  # noqa: E402

HERE = Path(__file__).resolve().parent
PCD = HERE.parent / "web" / "models" / "kitti-velodyne-000000.pcd"


def _load_script():
    path = HERE.parent / "scripts" / "build_movie_dataset.py"
    spec = importlib.util.spec_from_file_location("build_movie_dataset", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _read_pcd(path):
    data = path.read_bytes()
    marker = b"DATA binary\n"
    end = data.index(marker) + len(marker)
    header = data[:end].decode("ascii", "replace")
    count = next(int(l.split()[1]) for l in header.splitlines() if l.startswith("POINTS"))
    return np.frombuffer(data[end:end + count * 16], dtype=np.float32).reshape(-1, 4)


def test_encode_frame_round_trips_intensity():
    mod = _load_script()
    raw = _read_pcd(PCD)
    idx = mod.downsample_idx_to_target(raw[:, :3].copy(), 3000)
    xyz, inten = raw[idx, :3], raw[idx, 3]
    buf = mod.encode_frame(xyz, inten)
    out = DracoPy.decode(buf)
    colors = np.asarray(out.colors)
    assert colors.shape[0] == len(idx)
    expected = np.clip(inten * 255.0, 0, 255).astype(np.uint8)
    # Draco reorders vertices on encode, but intensity stays attached to its point,
    # so the multiset of green-channel values must survive (compare sorted). The
    # class/blue channels carry nothing for the geometry movie.
    assert np.array_equal(np.sort(colors[:, 1]), np.sort(expected))
    assert colors[:, 0].max() == 0 and colors[:, 2].max() == 0
