"""Entry point for `python -m movie_plot_rag`.

Accepts ``--params '<JSON object>'`` to override pipeline kwargs (e.g.
``--params '{"corpus_size": 200, "top_k": 5}'``). With ``--run``, the job is
registered AND executed inline via ``ajob_test`` — the whole DAG in one
process, which ``aaiclick run-job`` cannot do since it always dispatches to
a worker. That is the reason this entry point still exists; plain
registration is better served by ``run-job``.
"""

import argparse
import asyncio
import json

from aaiclick.orchestration import ajob_test

from . import main


def _parse_argv() -> tuple[dict, bool]:
    parser = argparse.ArgumentParser(prog="python -m movie_plot_rag")
    parser.add_argument(
        "--params",
        default="{}",
        help='JSON object of pipeline kwargs, e.g. \'{"corpus_size": 200, "top_k": 5}\'',
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Register the job AND execute it inline via ajob_test (skip worker).",
    )
    args = parser.parse_args()
    return json.loads(args.params), args.run


async def _register_and_run(**kwargs):
    created_job = await main(**kwargs)
    await ajob_test(created_job)


if __name__ == "__main__":
    params, run = _parse_argv()
    if run:
        asyncio.run(_register_and_run(**params))
    else:
        asyncio.run(main(**params))
