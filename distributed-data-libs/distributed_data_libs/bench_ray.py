"""Ray Data adapter (Jobs API client). See SPEC.md for the four
ways we tried driving Ray Data and why this is the only one that
works in our container topology."""

import asyncio
import os
import time

import ray
from ray.job_submission import JobStatus, JobSubmissionClient

VERSION = ray.__version__

_JOBS_URL = os.environ.get("RAY_JOBS_URL", "http://ray-head:8265")
# Stay under the orchestrator's per-framework timeout (RAY_TIMEOUT_SECS,
# default 1500s) so we can exit cleanly with diagnostics instead of
# being SIGTERM'd mid-poll.
_JOB_DEADLINE_S = int(os.environ.get("RAY_JOB_DEADLINE_S", "1400"))


def _submit_with_retry(client):
    # The dashboard accepts HTTP on 8265 (passing the container
    # healthcheck) several seconds before the Jobs agent has registered;
    # in that window submit_job returns 500 "No available agent".
    deadline = time.time() + 90
    while True:
        try:
            return client.submit_job(
                entrypoint="python -m distributed_data_libs.ray_job",
            )
        except RuntimeError as e:
            if "No available agent" in str(e) and time.time() < deadline:
                time.sleep(2)
                continue
            raise


async def _stream(client, job_id):
    async for line in client.tail_job_logs(job_id):
        print(line, end="", flush=True)


def run_as_client():
    client = JobSubmissionClient(_JOBS_URL)
    job_id = _submit_with_retry(client)
    print(f"[ray client] submitted job {job_id}", flush=True)

    try:
        asyncio.run(asyncio.wait_for(_stream(client, job_id), timeout=_JOB_DEADLINE_S))
    except asyncio.TimeoutError:
        raise RuntimeError(f"Ray job {job_id} hit internal deadline ({_JOB_DEADLINE_S}s)")

    status = client.get_job_status(job_id)
    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(f"Ray job ended with status: {status}")
    print(f"[ray client] job succeeded", flush=True)
