"""Ray Data adapter — Ray Jobs API client.

The runner ships the benchmark to ray-head as a Ray Job and polls for
completion. ray_job.py (running inside ray-head) does the actual ops,
measurement, and writes /data/results-ray.json. This bench module only
exists to override runner.py's default flow via main().

Why Ray Jobs and not the more common in-process / Ray Client / direct
ray.init paths:
- ray://     - Ray Client mode breaks Ray Data (no local core_worker).
- ray.init(address="<head>:<port>") - "No node info found".
- ray start + ray.init(auto) - silent hang on first distributed task.
The Ray Jobs API runs everything server-side on ray-head, sidestepping
all three failure modes."""

import os
import time

import ray
from ray.job_submission import JobStatus, JobSubmissionClient

VERSION = ray.__version__

_JOBS_URL = os.environ.get("RAY_JOBS_URL", "http://ray-head:8265")
_TERMINAL = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.STOPPED}


def main():
    """Override runner.py's default flow: submit a Ray Job and poll."""
    client = JobSubmissionClient(_JOBS_URL)

    # Retry submit_job: the dashboard accepts HTTP on 8265 (so the
    # healthcheck passes) several seconds before the job agent has
    # actually registered. In that window submit_job returns
    # `500 No available agent to submit job`.
    deadline = time.time() + 90
    while True:
        try:
            job_id = client.submit_job(
                entrypoint="python -m distributed_data_libs.ray_job",
            )
            break
        except RuntimeError as e:
            if "No available agent" in str(e) and time.time() < deadline:
                time.sleep(2)
                continue
            raise
    print(f"[ray client] submitted job {job_id}", flush=True)

    # The job's stdout isn't streamed in real time by Ray Jobs; we have
    # to poll get_job_logs and diff. Print incrementally so a hung job
    # shows up as the last op printed by ray_job.py. Also enforce our
    # own deadline shorter than the orchestrator's outer timeout (1500s
    # for ray in CI) so we can fetch logs and exit cleanly instead of
    # being SIGTERM'd mid-polling.
    job_deadline = time.time() + 1400
    last_logs = ""
    next_log_pull = time.time() + 5
    while True:
        status = client.get_job_status(job_id)
        if status in _TERMINAL:
            break
        if time.time() >= job_deadline:
            print(f"[ray client] job exceeded internal deadline; dumping logs", flush=True)
            try:
                print(client.get_job_logs(job_id), flush=True)
            except Exception as e:
                print(f"[ray client] log fetch failed: {e}", flush=True)
            raise RuntimeError(f"Ray job {job_id} hit internal deadline")
        if time.time() >= next_log_pull:
            try:
                logs = client.get_job_logs(job_id)
                delta = logs[len(last_logs):]
                if delta:
                    print(delta, end="", flush=True)
                    last_logs = logs
            except Exception:
                pass
            next_log_pull = time.time() + 5
        time.sleep(2)

    # Final flush of any remaining log output.
    try:
        logs = client.get_job_logs(job_id)
        print(logs[len(last_logs):], flush=True)
    except Exception as e:
        print(f"[ray client] final log fetch failed: {e}", flush=True)

    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(f"Ray job ended with status: {status}")
    print(f"[ray client] job succeeded", flush=True)
