"""One-shot: restructure the kitti-movie HF dataset into geometry/ + seg/ folders.

Phase 1 (this run): copy the 154 root-level NNNNNN.drc geometry frames into a
geometry/ subfolder using server-side copies (no re-download/re-upload), in a
single commit. Root copies are LEFT IN PLACE so the currently-deployed viewer,
which still serves from the root, keeps working until config.js is repointed and
redeployed. A later run (--delete-root) removes the root frames once geometry/ is
verified live.

Run (HF_TOKEN must be set):
  uv run --group gen python scripts/restructure_dataset.py
  uv run --group gen python scripts/restructure_dataset.py --delete-root
"""
from __future__ import annotations

import argparse
import os

from huggingface_hub import (
    CommitOperationCopy,
    CommitOperationDelete,
    HfApi,
)

REPO_ID = "kolodkin/pcl-viewer-kitti-movie"


def frame_names(api: HfApi) -> list[str]:
    files = api.list_repo_files(REPO_ID, repo_type="dataset")
    return sorted(f for f in files if f.endswith(".drc") and "/" not in f)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-id", default=REPO_ID)
    ap.add_argument(
        "--delete-root",
        action="store_true",
        help="delete the root-level .drc frames (run only after geometry/ is verified live)",
    )
    args = ap.parse_args()

    api = HfApi(token=os.environ["HF_TOKEN"])
    roots = frame_names(api)
    print(f"{len(roots)} root .drc frames")

    if args.delete_root:
        ops = [CommitOperationDelete(path_in_repo=n) for n in roots]
        api.create_commit(
            args.repo_id,
            repo_type="dataset",
            operations=ops,
            commit_message="Remove root-level geometry frames (now under geometry/)",
        )
        print(f"deleted {len(ops)} root frames")
        return

    ops = [
        CommitOperationCopy(src_path_in_repo=n, path_in_repo=f"geometry/{n}")
        for n in roots
    ]
    api.create_commit(
        args.repo_id,
        repo_type="dataset",
        operations=ops,
        commit_message="Copy geometry frames into geometry/ subfolder",
    )
    print(f"copied {len(ops)} frames into geometry/")


if __name__ == "__main__":
    main()
