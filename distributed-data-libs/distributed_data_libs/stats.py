"""cgroup v2 readers for memory.peak and cpu.stat.

Two modes:

* **Self-only** (Spark/Dask/Ray in-process, also the legacy path): read this
  container's own /sys/fs/cgroup. Works because Docker mounts each
  container's cgroup namespace as /sys/fs/cgroup by default.

* **Multi-container** (aaiclick distributed, and the upcoming Spark Connect
  / Dask cluster / Ray cluster topologies): the runner can't measure
  server-side work by reading its own cgroup — compute happens in sidecar
  containers. We mount the *host's* /sys/fs/cgroup at /host/cgroup:ro and
  /var/run/docker.sock:ro, look up each compose-service container by
  label, and sum memory.peak + cpu.stat across all of them. Activated by
  setting COMPUTE_CONTAINERS=svc1,svc2,... in the runner's env.

Note on memory.peak: on GitHub Actions runners (and many Docker hosts)
/sys/fs/cgroup is bind-mounted read-only, so we can't reset memory.peak
between ops. Use a 'delta of monotonic peak' model: read before/after each
op; the delta is the *incremental* high-water this op contributed. The
first op's delta is its absolute peak; later ops that don't exceed the
running peak report 0.
"""

import os

# Container's own cgroup (Docker mounts the container's cgroup namespace here).
SELF_CGROUP = "/sys/fs/cgroup"

# Host's /sys/fs/cgroup, mounted in for multi-container mode.
HOST_CGROUP = "/host/cgroup"


def _read_memory_peak(cgroup_dir):
    with open(f"{cgroup_dir}/memory.peak") as f:
        return int(f.read().strip())


def _read_cpu_usec(cgroup_dir):
    """Cumulative CPU time (microseconds, all cores) for this cgroup."""
    with open(f"{cgroup_dir}/cpu.stat") as f:
        for line in f:
            key, _, val = line.partition(" ")
            if key == "usage_usec":
                return int(val.strip())
    return 0


def cgroup_v2_available():
    return os.path.exists(f"{SELF_CGROUP}/memory.peak") \
        and os.path.exists(f"{SELF_CGROUP}/cpu.stat")


def _resolve_compose_container_cgroups(service_names, project=None):
    """Map compose service names to host-cgroup paths via the docker socket.

    Looks containers up by their compose labels so this works regardless of
    user-set container_name. Tries both systemd and cgroupfs cgroup driver
    layouts.
    """
    import docker

    if project is None:
        project = os.environ.get("COMPOSE_PROJECT_NAME", "distributed-data-libs")

    client = docker.from_env()
    paths = []
    for svc in service_names:
        cs = client.containers.list(
            all=True,
            filters={"label": [
                f"com.docker.compose.project={project}",
                f"com.docker.compose.service={svc}",
            ]},
        )
        if not cs:
            raise RuntimeError(
                f"No container found for compose service {svc!r} "
                f"in project {project!r}"
            )
        cid = cs[0].id
        candidates = [
            f"{HOST_CGROUP}/system.slice/docker-{cid}.scope",
            f"{HOST_CGROUP}/docker/{cid}",
        ]
        for cgroup_dir in candidates:
            if os.path.exists(f"{cgroup_dir}/memory.peak"):
                paths.append(cgroup_dir)
                break
        else:
            raise RuntimeError(
                f"Cannot locate host cgroup for {svc} (id={cid[:12]}); "
                f"tried {candidates}"
            )
    return paths


def resolve_measurement_targets():
    """Return list of cgroup directories to read for the active framework.

    If COMPUTE_CONTAINERS=svc1,svc2 is set, sum those containers'
    host-side cgroups. Otherwise fall back to this container's own cgroup
    (preserves the in-process-framework path: Spark/Dask/Ray today).
    """
    raw = os.environ.get("COMPUTE_CONTAINERS", "").strip()
    if not raw:
        return [SELF_CGROUP]
    services = [s.strip() for s in raw.split(",") if s.strip()]
    return _resolve_compose_container_cgroups(services)


def sum_memory_peak(cgroup_dirs):
    return sum(_read_memory_peak(d) for d in cgroup_dirs)


def sum_cpu_usec(cgroup_dirs):
    return sum(_read_cpu_usec(d) for d in cgroup_dirs)
