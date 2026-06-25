"""One-shot: build the SemanticKITTI 'seg' movie under the shared HF dataset.

Downloads one SemanticKITTI sequence slice (KITTI Odometry velodyne + label
files), remaps each point to the 19-class learning set, joint voxel-downsamples
to ~30k points carrying the class, derives one axis-aligned 3D box per thing
instance, Draco-encodes positions with the class id packed into the color
attribute (red channel), writes boxes.json, and uploads everything under seg/ in
kolodkin/pcl-viewer-kitti-movie alongside the existing geometry/ movie.

Inputs (set SEMANTIC_KITTI_DIR to a local SemanticKITTI 'dataset/sequences' tree,
or pass --velodyne-dir / --label-dir):
  <seq>/velodyne/NNNNNN.bin   float32 [x y z remission]
  <seq>/labels/NNNNNN.label   uint32  (low16 = class, high16 = instance)

Run (HF_TOKEN must be set to upload):
  uv run --group gen python scripts/build_seg_dataset.py --seq 08 --start 0 --limit 150
  uv run --group gen python scripts/build_seg_dataset.py --seq 08 --limit 4 --no-upload
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import numpy as np
import DracoPy
from huggingface_hub import HfApi

REPO_ID = "kolodkin/pcl-viewer-kitti-movie"
TARGET_POINTS = 30000
QUANT_BITS = 14
THING_CLASSES = {1, 2, 3, 4, 5, 6, 7, 8}

# SemanticKITTI raw label id -> 19-class learning id (the official learning_map).
LEARNING_MAP = {
    0: 0, 1: 0, 10: 1, 11: 2, 13: 5, 15: 3, 16: 5, 18: 4, 20: 5, 30: 6, 31: 7,
    32: 8, 40: 9, 44: 10, 48: 11, 49: 12, 50: 13, 51: 14, 52: 0, 60: 9, 70: 15,
    71: 16, 72: 17, 80: 18, 81: 19, 99: 0, 252: 1, 253: 7, 254: 6, 255: 8,
    256: 5, 257: 5, 258: 4, 259: 5,
}

CARD = """\
---
license: cc-by-nc-sa-3.0
task_categories:
  - other
tags: [point-cloud, lidar, kitti, semantic-kitti, draco]
---

# pcl-viewer KITTI movies

Draco-compressed LiDAR frames for the
[pcl-viewer](https://github.com/kolodkin/samples) demo, in two folders:

- **`geometry/`** — positions-only sweeps from KITTI raw drive `2011_09_26_drive_0005`.
- **`seg/`** — SemanticKITTI sequence slice with a per-point **class id** packed in
  the Draco color attribute, plus `boxes.json` (one axis-aligned 3D box per thing
  instance per frame).

## Attribution & license

Source: KITTI / SemanticKITTI, **CC BY-NC-SA 3.0**; these derivatives keep the
same license (ShareAlike). Non-commercial use only.

> Geiger et al., *Vision meets Robotics: The KITTI Dataset*, IJRR 2013.
> Behley et al., *SemanticKITTI: A Dataset for Semantic Scene Understanding of
> LiDAR Sequences*, ICCV 2019.
"""

ANNOTATIONS = """\
# Annotations — seg/

Each `NNNNNN.drc` is a SemanticKITTI sweep, voxel-downsampled to ~{target:,}
points (positions only), Draco-encoded with the per-point **19-class learning id**
stored in the color attribute's red channel ({bits}-bit positions). `boxes.json`
maps each frame to a list of axis-aligned 3D boxes (one per thing instance):
`{{ "NNNNNN": [ {{"cls": id, "center": [x,y,z], "size": [sx,sy,sz]}} ] }}`, in the
source Velodyne frame (metres, z-up, sensor at origin).

Sequence: **{seq}**, frames {start}…{last}. License: see `../README.md`.
"""


def voxel_downsample_idx(pts: np.ndarray, voxel: float) -> np.ndarray:
    keys = np.floor(pts / voxel).astype(np.int64)
    _, idx = np.unique(keys, axis=0, return_index=True)
    return np.sort(idx)


def downsample_idx_to_target(pts: np.ndarray, target: int) -> np.ndarray:
    lo, hi = 0.05, 2.0
    idx = np.arange(len(pts))
    for _ in range(12):
        mid = (lo + hi) / 2
        idx = voxel_downsample_idx(pts, mid)
        if len(idx) > target:
            lo = mid
        else:
            hi = mid
    return idx


def remap_classes(raw: np.ndarray) -> np.ndarray:
    out = np.zeros_like(raw, dtype=np.uint8)
    for k, v in LEARNING_MAP.items():
        out[raw == k] = v
    return out


def derive_boxes(xyz: np.ndarray, cls: np.ndarray, inst: np.ndarray) -> list[dict]:
    boxes = []
    things = np.isin(cls, list(THING_CLASSES))
    if not things.any():
        return boxes
    keys = inst.astype(np.int64) * 100 + cls.astype(np.int64)
    for key in np.unique(keys[things]):
        m = (keys == key) & things
        if m.sum() < 10:
            continue
        pts = xyz[m]
        lo, hi = pts.min(0), pts.max(0)
        center = ((lo + hi) / 2).tolist()
        size = (hi - lo).tolist()
        boxes.append({"cls": int(cls[m][0]), "center": center, "size": size})
    return boxes


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-id", default=REPO_ID)
    ap.add_argument("--seq", default="08")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--limit", type=int, default=150)
    ap.add_argument("--velodyne-dir")
    ap.add_argument("--label-dir")
    ap.add_argument("--out", default="/tmp/kitti-seg")
    ap.add_argument("--no-upload", action="store_true")
    args = ap.parse_args()

    root = os.environ.get("SEMANTIC_KITTI_DIR", "")
    velo_dir = Path(args.velodyne_dir or f"{root}/{args.seq}/velodyne")
    label_dir = Path(args.label_dir or f"{root}/{args.seq}/labels")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    bins = sorted(velo_dir.glob("*.bin"))[args.start : args.start + args.limit]
    if not bins:
        raise SystemExit(f"no .bin frames under {velo_dir}")
    print(f"{len(bins)} frames from {velo_dir}")

    boxes_all = {}
    for i, binpath in enumerate(bins):
        raw = np.fromfile(binpath, dtype=np.float32).reshape(-1, 4)
        xyz = raw[:, :3]
        lab = np.fromfile(label_dir / f"{binpath.stem}.label", dtype=np.uint32)
        cls = remap_classes(lab & 0xFFFF)
        inst = (lab >> 16).astype(np.uint32)
        idx = downsample_idx_to_target(xyz.copy(), TARGET_POINTS)
        xyz_d, cls_d, inst_d = xyz[idx], cls[idx], inst[idx]
        colors = np.zeros((len(xyz_d), 3), dtype=np.uint8)
        colors[:, 0] = cls_d
        buf = DracoPy.encode(xyz_d.astype(np.float32), colors=colors, quantization_bits=QUANT_BITS)
        (out / f"{i:06d}.drc").write_bytes(buf)
        boxes_all[f"{i:06d}"] = derive_boxes(xyz_d, cls_d, inst_d)
        if i % 20 == 0:
            print(f"  frame {i}: {len(xyz_d)} pts, {len(boxes_all[f'{i:06d}'])} boxes -> {len(buf)} bytes")

    (out / "boxes.json").write_text(json.dumps(boxes_all))
    (out / "annotations.md").write_text(
        ANNOTATIONS.format(
            target=TARGET_POINTS, bits=QUANT_BITS, seq=args.seq,
            start=args.start, last=args.start + len(bins) - 1,
        )
    )
    print(f"wrote {len(bins)} frames + boxes.json to {out}")

    if args.no_upload:
        return
    api = HfApi(token=os.environ["HF_TOKEN"])
    api.create_repo(args.repo_id, repo_type="dataset", exist_ok=True, private=False)
    api.upload_folder(folder_path=str(out), path_in_repo="seg", repo_id=args.repo_id, repo_type="dataset")
    api.upload_file(path_or_fileobj=CARD.encode(), path_in_repo="README.md",
                    repo_id=args.repo_id, repo_type="dataset")
    print(f"uploaded seg/ ({len(bins)} frames) to https://huggingface.co/datasets/{args.repo_id}")


if __name__ == "__main__":
    main()
