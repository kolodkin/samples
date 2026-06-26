"""Build the SemanticKITTI 'seg' movie under the shared HF dataset, in stages.

Three stages, selectable via flags (default: run all three):

  --download   Stream the SemanticKITTI archive (Brainkite/semantickitti, a
               split tar.zst) and extract matched velodyne `.bin` + `.label`
               pairs for one sequence's first N frames into --src-dir. Velodyne
               and labels live in separate, randomly-ordered regions, so it keeps
               streaming until it holds all N of *both* (≈6 GB for 150 frames).
  --process    Read the raw frames from --src-dir, remap each point to the 19-class
               learning set, joint voxel-downsample to ~30k points carrying the
               class, derive one axis-aligned 3D box per thing instance, and
               Draco-encode positions with the class id packed into the color
               attribute (red channel). Writes .drc + boxes.json + annotations.md
               to --out.
  --upload     Upload --out to `seg/` in the HF dataset (alongside `geometry/`)
               and refresh the dataset card. Needs HF_TOKEN.

Examples:
  # full pipeline (download → process → upload), 150 frames of sequence 00
  uv run --group gen python scripts/build_seg_dataset.py --seq 00 --limit 150
  # just re-process already-downloaded frames, no upload
  uv run --group gen python scripts/build_seg_dataset.py --process --src-dir /tmp/seg-src
  # process a local SemanticKITTI tree + upload (skip the big download)
  SEMANTIC_KITTI_DIR=~/sk/dataset/sequences \\
    uv run --group gen python scripts/build_seg_dataset.py --process --upload --seq 08
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

import numpy as np
import DracoPy
from huggingface_hub import HfApi

REPO_ID = "kolodkin/pcl-viewer-kitti-movie"
TARGET_POINTS = 30000
QUANT_BITS = 14
THING_CLASSES = {1, 2, 3, 4, 5, 6, 7, 8}

# SemanticKITTI archive (full dataset/sequences tree as a split tar.zst).
ARCHIVE_BASE = ("https://huggingface.co/datasets/Brainkite/semantickitti/"
                "resolve/main/semantickitti.tar.zst.part.")
ARCHIVE_PARTS = ["aa", "ab", "ac", "ad", "ae", "af", "ag"]

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

- **`geometry/`** — sweeps from KITTI raw drive `2011_09_26_drive_0005`, positions
  plus per-point intensity (Draco color green channel).
- **`seg/`** — SemanticKITTI sequence slice with a per-point **class id** (Draco
  color red channel) and **intensity** (green channel), plus `boxes.json` (one
  axis-aligned 3D box per thing instance per frame).

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
points, Draco-encoded ({bits}-bit positions) with the per-point **19-class
learning id** in the color attribute's red channel and per-point **intensity**
(laser reflectance, 0–255) in the green channel. `boxes.json`
maps each frame to a list of axis-aligned 3D boxes (one per thing instance):
`{{ "NNNNNN": [ {{"cls": id, "center": [x,y,z], "size": [sx,sy,sz]}} ] }}`, in the
source Velodyne frame (metres, z-up, sensor at origin).

Sequence: **{seq}**, frames {start}…{last}. License: see `../README.md`.
"""


# ----------------------------------------------------------------------------
# --download : stream the split tar.zst, extract matched bin+label pairs
# ----------------------------------------------------------------------------
class _ChainedParts:
    """File-like over the concatenation of the split archive parts (one zstd
    stream); counts bytes downloaded for progress reporting."""

    def __init__(self, parts):
        import requests
        self._requests = requests
        self.parts, self.i, self.n = list(parts), 0, 0
        self.cur = None
        self._open()

    def _open(self):
        if self.i >= len(self.parts):
            self.cur = None
            return
        r = self._requests.get(ARCHIVE_BASE + self.parts[self.i], stream=True)
        r.raise_for_status()
        self.cur = r.raw

    def read(self, n):
        while True:
            if self.cur is None:
                return b""
            b = self.cur.read(n)
            if b:
                self.n += len(b)
                return b
            self.i += 1
            self._open()


def download(seq: str, n: int, src_dir: Path) -> None:
    import tarfile
    import zstandard

    velo_out = src_dir / seq / "velodyne"
    label_out = src_dir / seq / "labels"
    velo_out.mkdir(parents=True, exist_ok=True)
    label_out.mkdir(parents=True, exist_ok=True)
    bin_re = re.compile(rf"sequences/{seq}/velodyne/(\d+)\.bin$")
    lab_re = re.compile(rf"sequences/{seq}/labels/(\d+)\.label$")

    src = _ChainedParts(ARCHIVE_PARTS)
    reader = zstandard.ZstdDecompressor().stream_reader(src)
    tar = tarfile.open(fileobj=reader, mode="r|")
    bins, labels, scanned = set(), set(), 0
    print(f"streaming SemanticKITTI seq {seq}, collecting {n} frames…")
    for m in tar:
        scanned += 1
        if scanned % 1000 == 0:
            print(f"  scanned={scanned} dl={src.n/1e9:.2f}GB "
                  f"bins={len(bins)} labels={len(labels)}")
            sys.stdout.flush()
        mb = bin_re.search(m.name)
        if mb and int(mb.group(1)) < n:
            idx = int(mb.group(1))
            (velo_out / f"{idx:06d}.bin").write_bytes(tar.extractfile(m).read())
            bins.add(idx)
        ml = lab_re.search(m.name)
        if ml and int(ml.group(1)) < n:
            idx = int(ml.group(1))
            (label_out / f"{idx:06d}.label").write_bytes(tar.extractfile(m).read())
            labels.add(idx)
        if len(bins) >= n and len(labels) >= n:
            break
    if len(bins) < n or len(labels) < n:
        raise SystemExit(
            f"archive exhausted with bins={len(bins)} labels={len(labels)} "
            f"(< {n}); the sequence may have fewer frames")
    print(f"downloaded {len(bins)} pairs ({src.n/1e9:.2f}GB) -> {src_dir}")


# ----------------------------------------------------------------------------
# --process : remap + downsample + box derivation + Draco encode
# ----------------------------------------------------------------------------
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
        boxes.append({
            "cls": int(cls[m][0]),
            "center": ((lo + hi) / 2).tolist(),
            "size": (hi - lo).tolist(),
        })
    return boxes


def process(seq: str, start: int, limit: int, src_dir: Path, out: Path) -> int:
    # Read from a pre-existing local SemanticKITTI tree if one is configured,
    # else from where --download wrote the frames.
    root = Path(os.environ["SEMANTIC_KITTI_DIR"]) if os.environ.get("SEMANTIC_KITTI_DIR") else src_dir
    velo_dir = root / seq / "velodyne"
    label_dir = root / seq / "labels"
    out.mkdir(parents=True, exist_ok=True)

    bins = sorted(velo_dir.glob("*.bin"))[start:start + limit]
    if not bins:
        raise SystemExit(f"no .bin frames under {velo_dir}")
    print(f"processing {len(bins)} frames from {velo_dir}")

    boxes_all = {}
    for i, binpath in enumerate(bins):
        raw = np.fromfile(binpath, dtype=np.float32).reshape(-1, 4)
        xyz = raw[:, :3]
        intensity = raw[:, 3]
        lab = np.fromfile(label_dir / f"{binpath.stem}.label", dtype=np.uint32)
        cls = remap_classes(lab & 0xFFFF)
        inst = (lab >> 16).astype(np.uint32)
        idx = downsample_idx_to_target(xyz.copy(), TARGET_POINTS)
        xyz_d, cls_d, inst_d = xyz[idx], cls[idx], inst[idx]
        colors = np.zeros((len(xyz_d), 3), dtype=np.uint8)
        colors[:, 0] = cls_d
        colors[:, 1] = np.clip(intensity[idx] * 255.0, 0, 255).astype(np.uint8)
        buf = DracoPy.encode(xyz_d.astype(np.float32), colors=colors,
                             quantization_bits=QUANT_BITS)
        (out / f"{i:06d}.drc").write_bytes(buf)
        boxes_all[f"{i:06d}"] = derive_boxes(xyz_d, cls_d, inst_d)
        if i % 20 == 0:
            print(f"  frame {i}: {len(xyz_d)} pts, "
                  f"{len(boxes_all[f'{i:06d}'])} boxes -> {len(buf)} bytes")

    (out / "boxes.json").write_text(json.dumps(boxes_all))
    (out / "annotations.md").write_text(ANNOTATIONS.format(
        target=TARGET_POINTS, bits=QUANT_BITS, seq=seq,
        start=start, last=start + len(bins) - 1))
    print(f"wrote {len(bins)} frames + boxes.json to {out}")
    return len(bins)


# ----------------------------------------------------------------------------
# --upload : push seg/ + dataset card
# ----------------------------------------------------------------------------
def upload(repo_id: str, out: Path) -> None:
    api = HfApi(token=os.environ["HF_TOKEN"])
    api.create_repo(repo_id, repo_type="dataset", exist_ok=True, private=False)
    api.upload_folder(folder_path=str(out), path_in_repo="seg",
                      repo_id=repo_id, repo_type="dataset")
    api.upload_file(path_or_fileobj=CARD.encode(), path_in_repo="README.md",
                    repo_id=repo_id, repo_type="dataset")
    print(f"uploaded seg/ to https://huggingface.co/datasets/{repo_id}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="stream + extract raw frames")
    ap.add_argument("--process", action="store_true", help="remap/downsample/encode")
    ap.add_argument("--upload", action="store_true", help="push seg/ to HF (needs HF_TOKEN)")
    ap.add_argument("--repo-id", default=REPO_ID)
    ap.add_argument("--seq", default="00")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--limit", type=int, default=150)
    ap.add_argument("--src-dir", default="/tmp/kitti-seg-src",
                    help="raw velodyne/labels tree (download target / process source)")
    ap.add_argument("--out", default="/tmp/kitti-seg", help="processed .drc output dir")
    args = ap.parse_args()

    # No stage flag -> run the whole pipeline.
    stages = (args.download, args.process, args.upload)
    if not any(stages):
        args.download = args.process = args.upload = True

    src_dir, out = Path(args.src_dir), Path(args.out)
    if args.download:
        download(args.seq, args.limit, src_dir)
    if args.process:
        process(args.seq, args.start, args.limit, src_dir, out)
    if args.upload:
        upload(args.repo_id, out)


if __name__ == "__main__":
    main()
