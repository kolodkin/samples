"""One-shot: build the KITTI movie HF dataset.

Downloads KITTI raw drive 0005, voxel-downsamples each Velodyne frame to ~30k
points, Draco-encodes (positions only, 14-bit quantization), and uploads the
.drc frames + a CC BY-NC-SA dataset card to a Hugging Face dataset.

Run (HF_TOKEN must be set):
  uv run --group gen python scripts/build_movie_dataset.py --repo-id kolodkin/pcl-viewer-kitti-movie
"""
from __future__ import annotations

import argparse
import io
import os
import zipfile
from pathlib import Path

import numpy as np
import requests
import DracoPy
from huggingface_hub import HfApi

DRIVE_ZIP = (
    "https://s3.eu-central-1.amazonaws.com/avg-kitti/raw_data/"
    "2011_09_26_drive_0005/2011_09_26_drive_0005_sync.zip"
)
VELO_DIR = "2011_09_26/2011_09_26_drive_0005_sync/velodyne_points/data/"
DRIVE = "2011_09_26_drive_0005"
TARGET_POINTS = 30000
QUANT_BITS = 14

CARD = """\
---
license: cc-by-nc-sa-3.0
task_categories:
  - other
tags:
  - point-cloud
  - lidar
  - kitti
  - draco
---

# pcl-viewer KITTI movie

Draco-compressed, downsampled LiDAR frames for the
[pcl-viewer](https://github.com/kolodkin/samples) demo's "KITTI movie" scene.

**This is a derivative work.** Each `NNNNNN.drc` is one Velodyne sweep from
**KITTI raw drive `2011_09_26_drive_0005`**, voxel-downsampled to ~30k points
(positions only) and Draco-encoded (14-bit position quantization).

## Attribution & license

Source: the KITTI dataset, **CC BY-NC-SA 3.0**. This derivative is released under
the **same license (CC BY-NC-SA 3.0)** per the ShareAlike term.

> A. Geiger, P. Lenz, C. Stiller, R. Urtasun. *Vision meets Robotics: The KITTI
> Dataset.* International Journal of Robotics Research (IJRR), 2013.
>
> A. Geiger, P. Lenz, R. Urtasun. *Are we ready for Autonomous Driving? The KITTI
> Vision Benchmark Suite.* CVPR, 2012.

Non-commercial use only.
"""

ANNOTATIONS = """\
# Annotations

**This dataset is geometry only — it carries no labels.** KITTI's own
annotations (3D object tracklets, semantic/instance segmentation) are **not**
included or derived here. Each frame is a downsampled, position-only LiDAR sweep
intended for visual playback in the
[pcl-viewer](https://github.com/kolodkin/samples) demo's "KITTI movie" scene.

## Frame index

| File         | Source                                                      |
|--------------|-------------------------------------------------------------|
| `NNNNNN.drc` | `{drive}` Velodyne sweep `NNNNNN`, in capture order         |

- Frames: **{count}** (`000000.drc` … `{last:06d}.drc`), in capture order.
- Format: Draco-encoded point **positions only** (`x y z`), {bits}-bit position
  quantization. No color, intensity, normals, or per-point labels.
- Each sweep is voxel-downsampled to ~{target:,} points before encoding, so a
  point in a frame does **not** correspond to a fixed physical return across
  frames (downsampling is independent per frame).

## Coordinate frame

Points are stored in the KITTI Velodyne frame (metres, z-up, sensor at the
origin). The viewer reorients to y-up and centers/scales at load time; the
stored `.drc` data is left in the source frame.

## License & attribution

See `README.md`. Source: KITTI (CC BY-NC-SA 3.0); this derivative is released
under the same license. Non-commercial use only.
"""


def voxel_downsample(pts: np.ndarray, voxel: float) -> np.ndarray:
    keys = np.floor(pts / voxel).astype(np.int64)
    _, idx = np.unique(keys, axis=0, return_index=True)
    return pts[np.sort(idx)]


def downsample_to_target(pts: np.ndarray, target: int = 30000) -> np.ndarray:
    """Pick a voxel size that lands near `target` points (a few bisection steps)."""
    lo, hi = 0.05, 2.0
    out = pts
    for _ in range(12):
        mid = (lo + hi) / 2
        out = voxel_downsample(pts, mid)
        if len(out) > target:
            lo = mid
        else:
            hi = mid
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-id", default="kolodkin/pcl-viewer-kitti-movie")
    ap.add_argument("--out", default="/tmp/kitti-movie")
    ap.add_argument("--limit", type=int, default=0, help="cap frame count (0 = all)")
    ap.add_argument("--no-upload", action="store_true")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {DRIVE_ZIP} …")
    resp = requests.get(DRIVE_ZIP, timeout=600)
    resp.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    bins = sorted(n for n in zf.namelist() if n.startswith(VELO_DIR) and n.endswith(".bin"))
    if args.limit:
        bins = bins[: args.limit]
    print(f"{len(bins)} velodyne frames")

    count = 0
    for i, name in enumerate(bins):
        raw = np.frombuffer(zf.read(name), dtype=np.float32).reshape(-1, 4)
        xyz = downsample_to_target(raw[:, :3].copy(), TARGET_POINTS)
        buf = DracoPy.encode(xyz.astype(np.float32), quantization_bits=QUANT_BITS)
        (out / f"{i:06d}.drc").write_bytes(buf)
        count += 1
        if i % 20 == 0:
            print(f"  frame {i}: {len(xyz)} pts -> {len(buf)} bytes")
    (out / "README.md").write_text(CARD)
    (out / "annotations.md").write_text(
        ANNOTATIONS.format(
            drive=DRIVE, count=count, last=max(count - 1, 0),
            bits=QUANT_BITS, target=TARGET_POINTS,
        )
    )
    print(f"wrote {count} frames to {out}")

    if args.no_upload:
        return
    token = os.environ["HF_TOKEN"]
    api = HfApi(token=token)
    api.create_repo(args.repo_id, repo_type="dataset", exist_ok=True, private=False)
    api.upload_folder(folder_path=str(out), repo_id=args.repo_id, repo_type="dataset")
    print(f"uploaded to https://huggingface.co/datasets/{args.repo_id}  (frames: {count})")


if __name__ == "__main__":
    main()
