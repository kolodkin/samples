# Sample asset attribution

`kitti-velodyne-000000.pcd` is **not** covered by the project's MIT license. It
is third-party data redistributed here under its original terms.

## Source

- **Dataset:** KITTI Vision Benchmark Suite — raw Velodyne LiDAR data, frame
  `000000` (a single 360° street-level scan, binary PCD, `FIELDS x y z intensity`,
  115,385 points).
- **Authors:** A. Geiger, P. Lenz, C. Stiller, R. Urtasun.
- **Obtained via:** the [`Qjizhi/kitti-velodyne-viewer`](https://github.com/Qjizhi/kitti-velodyne-viewer)
  repository's pre-converted PCD files.
- **Project page:** https://www.cvlibs.net/datasets/kitti/
- **Raw data (Velodyne):** https://www.cvlibs.net/datasets/kitti/raw_data.php

## License

**Creative Commons Attribution-NonCommercial-ShareAlike 3.0** (CC BY-NC-SA 3.0):

- KITTI's own license declaration (project page footer): https://www.cvlibs.net/datasets/kitti/
- License deed (summary): https://creativecommons.org/licenses/by-nc-sa/3.0/
- Legal code (full text): https://creativecommons.org/licenses/by-nc-sa/3.0/legalcode

- **BY** — credit the original authors (citation below).
- **NC** — non-commercial use only.
- **SA** — derivatives must be shared under the same license.

## Why this sample can be used here

This project is a free, open-source demonstration, not a commercial product, and
it satisfies all three CC BY-NC-SA 3.0 conditions:

- **Attribution (BY)** — the original authors, source, and citation are recorded
  in this file (and referenced from `SPEC.md`).
- **NonCommercial (NC)** — the viewer is published under the MIT license purely
  as an educational/demo example; it is not sold, monetized, or used to provide a
  commercial service, so the non-commercial restriction is met.
- **ShareAlike (SA)** — the `.pcd` is redistributed unmodified under its original
  CC BY-NC-SA 3.0 terms (this file preserves them). The viewer code is a separate,
  independently licensed work that reads the data at runtime rather than a
  derivative of it, so the project's own MIT license is unaffected.

The asset is included for demonstration purposes only. Anyone reusing this repo
commercially must remove or replace `kitti-velodyne-000000.pcd` with a
suitably licensed point cloud.

## Citation

> A. Geiger, P. Lenz, and R. Urtasun. *Are We Ready for Autonomous Driving? The
> KITTI Vision Benchmark Suite.* Conference on Computer Vision and Pattern
> Recognition (CVPR), 2012.

```bibtex
@inproceedings{Geiger2012CVPR,
  author    = {Andreas Geiger and Philip Lenz and Raquel Urtasun},
  title     = {Are we ready for Autonomous Driving? The {KITTI} Vision Benchmark Suite},
  booktitle = {Conference on Computer Vision and Pattern Recognition (CVPR)},
  year      = {2012}
}
```
