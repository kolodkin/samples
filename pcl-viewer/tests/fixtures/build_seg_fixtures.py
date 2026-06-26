"""Build the committed seg movie fixture by slicing the first few frames of a
*real* processed seg dataset — so the offline e2e and the screenshot report show
the same density, classes, and boxes as the live HF scene, just with a handful of
frames instead of 150.

The heavy lifting (download SemanticKITTI + remap/downsample/encode) belongs to
scripts/build_seg_dataset.py; this just copies the first N `.drc` frames and the
matching boxes.json entries into tests/fixtures/seg/. Point --src at a processed
dir (default: the build_seg_dataset.py --process output):

    # produce a processed dataset once (downloads ~6 GB, writes ~/seg-proc)
    uv run --group gen python scripts/build_seg_dataset.py --process \\
        --src-dir /path/to/raw --out /tmp/seg-proc
    # then slice the committed fixture from it
    uv run --group gen python tests/fixtures/build_seg_fixtures.py --src /tmp/seg-proc
"""
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import numpy as np
import DracoPy

HERE = Path(__file__).resolve().parent
OUT = HERE / "seg"


def inject_intensity(out: Path) -> None:
    """Re-encode each committed seg .drc in place, adding a deterministic synthetic
    intensity (normalized radial distance) to the color green channel. The class id
    in the red channel is preserved. Use offline when the real ~6 GB processed
    source isn't available; real intensity comes from build_seg_dataset.py --process."""
    for drc in sorted(out.glob("*.drc")):
        m = DracoPy.decode(drc.read_bytes())
        pts = np.asarray(m.points, dtype=np.float32)
        colors = np.asarray(m.colors, dtype=np.uint8).copy()
        d = np.linalg.norm(pts, axis=1)
        span = (d.max() - d.min()) or 1.0
        colors[:, 1] = np.clip((d - d.min()) / span * 255.0, 0, 255).astype(np.uint8)
        buf = DracoPy.encode(pts, colors=colors, quantization_bits=14)
        drc.write_bytes(buf)
        print(f"injected intensity -> {drc} ({len(buf)} bytes, "
              f"green_max={int(colors[:,1].max())})")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="/tmp/kitti-seg",
                    help="processed seg dataset dir (build_seg_dataset.py --out)")
    ap.add_argument("--frames", type=int, default=4, help="how many frames to keep")
    ap.add_argument("--inject-intensity", action="store_true",
                    help="re-encode the committed fixtures in place with synthetic "
                         "intensity (offline; no processed source needed)")
    args = ap.parse_args()

    if args.inject_intensity:
        inject_intensity(OUT)
        return

    src = Path(args.src)
    drcs = sorted(src.glob("*.drc"))[:args.frames]
    if len(drcs) < args.frames:
        raise SystemExit(f"need {args.frames} .drc frames in {src}, found {len(drcs)}")
    boxes_src = json.loads((src / "boxes.json").read_text())

    OUT.mkdir(parents=True, exist_ok=True)
    for f in OUT.glob("*.drc"):
        f.unlink()
    boxes = {}
    for i, drc in enumerate(drcs):
        key = f"{i:06d}"
        shutil.copyfile(drc, OUT / f"{key}.drc")
        boxes[key] = boxes_src.get(drc.stem, [])
        print(f"{key}.drc  ({drc.stat().st_size} bytes, {len(boxes[key])} boxes)")
    (OUT / "boxes.json").write_text(json.dumps(boxes))
    print(f"wrote {len(drcs)} real frames + boxes.json to {OUT}")


if __name__ == "__main__":
    main()
