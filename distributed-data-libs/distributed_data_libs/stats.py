"""cgroup v2 readers for memory.peak and cpu.stat. Requires a cgroup-v2 host
(GitHub Actions ubuntu-22.04+ and most modern distros). Inside the container,
/sys/fs/cgroup is the container's own cgroup.

Note: on GitHub Actions runners (and many other Docker hosts), /sys/fs/cgroup
is bind-mounted read-only, so writing 0 to memory.peak to reset the peak
between ops fails with EROFS. We therefore use a 'delta of monotonic peak'
model: read memory.peak before and after each op. Since memory.peak only
increases, the delta is the *incremental* high-water this op contributed.
The first op's delta is its absolute peak; later ops that don't exceed the
running peak report 0 — semantically 'this op did not push the kernel's peak
higher than what an earlier op already reached.'"""

import os

CGROUP_ROOT = "/sys/fs/cgroup"
MEMORY_PEAK = f"{CGROUP_ROOT}/memory.peak"
CPU_STAT = f"{CGROUP_ROOT}/cpu.stat"


def read_memory_peak():
    with open(MEMORY_PEAK) as f:
        return int(f.read().strip())


def read_cpu_usec():
    """Cumulative CPU time in microseconds across all cores in this cgroup."""
    with open(CPU_STAT) as f:
        for line in f:
            key, _, val = line.partition(" ")
            if key == "usage_usec":
                return int(val.strip())
    return 0


def cgroup_v2_available():
    return os.path.exists(MEMORY_PEAK) and os.path.exists(CPU_STAT)
