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

HERE = Path(__file__).resolve().parent
OUT = HERE / "seg"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="/tmp/kitti-seg",
                    help="processed seg dataset dir (build_seg_dataset.py --out)")
    ap.add_argument("--frames", type=int, default=4, help="how many frames to keep")
    args = ap.parse_args()

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
