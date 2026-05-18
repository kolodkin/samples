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
    job_id = client.submit_job(
        entrypoint="python -m distributed_data_libs.ray_job",
    )
    print(f"[ray client] submitted job {job_id}", flush=True)

    while True:
        status = client.get_job_status(job_id)
        if status in _TERMINAL:
            break
        time.sleep(2)

    logs = client.get_job_logs(job_id)
    print(logs, flush=True)

    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(f"Ray job ended with status: {status}")
    print(f"[ray client] job succeeded", flush=True)
