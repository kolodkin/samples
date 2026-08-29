"""Entry point for `python -m google_health_airtable_sync`.

Accepts ``--params '<JSON object>'`` to override pipeline kwargs (e.g.
``--params '{"days": 30, "publish_airtable": false}'``). With ``--run``, the
job is registered AND executed inline via ``ajob_test`` (useful for local
debugging without spinning up a worker). ``--auth`` runs the one-time OAuth
consent flow and prints a refresh token instead of registering a job.
"""

import argparse
import asyncio
import json

from aaiclick.orchestration import ajob_test

from . import main
from .auth import run_auth_flow


def _parse_argv() -> tuple[dict, bool, bool]:
    parser = argparse.ArgumentParser(prog="python -m google_health_airtable_sync")
    parser.add_argument(
        "--params",
        default="{}",
        help='JSON object of pipeline kwargs, e.g. \'{"days": 30, "publish_airtable": false}\'',
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Register the job AND execute it inline via ajob_test (skip worker).",
    )
    parser.add_argument(
        "--auth",
        action="store_true",
        help="Run the one-time OAuth consent flow and print a refresh token.",
    )
    args = parser.parse_args()
    return json.loads(args.params), args.run, args.auth


async def _register_and_run(**kwargs):
    created_job = await main(**kwargs)
    await ajob_test(created_job)


if __name__ == "__main__":
    params, run, auth = _parse_argv()
    if auth:
        run_auth_flow()
    elif run:
        asyncio.run(_register_and_run(**params))
    else:
        asyncio.run(main(**params))
