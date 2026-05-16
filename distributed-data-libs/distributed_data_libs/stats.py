"""cgroup v2 readers for memory.peak and cpu.stat. Requires a cgroup-v2 host
(GitHub Actions ubuntu-22.04+ and most modern distros). Inside the container,
/sys/fs/cgroup is the container's own cgroup."""

import os

CGROUP_ROOT = "/sys/fs/cgroup"
MEMORY_PEAK = f"{CGROUP_ROOT}/memory.peak"
CPU_STAT = f"{CGROUP_ROOT}/cpu.stat"


def reset_memory_peak():
    """Write 0 to memory.peak to reset the kernel's high-water mark. cgroup v2
    feature. Requires the writer to own the cgroup (root in the container is
    fine since the container is its own cgroup root)."""
    with open(MEMORY_PEAK, "w") as f:
        f.write("0")


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
