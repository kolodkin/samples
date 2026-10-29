"""Entry point for `python -m imdb_dataset_builder` — run the DAG in one process.

``ajob_test`` executes every task inline, so breakpoints work and there is no
worker log to tail. That is the one thing ``aaiclick run-job`` cannot do: it
records the job and a worker executes it. Real runs should go through
``run-job`` (see ``imdb-dataset-builder.sh``), which is why this entry point
does not offer a register-only mode.

Accepts ``--params '<JSON object>'`` to override pipeline kwargs, e.g.
``--params '{"limit": 500000, "year_from": 1990}'``.
"""

import argparse
import asyncio
import json

from aaiclick.orchestration import ajob_test

from . import main


def _parse_params() -> dict:
    parser = argparse.ArgumentParser(
        prog="python -m imdb_dataset_builder",
        description="Execute the IMDb dataset builder pipeline inline, without a worker.",
    )
    parser.add_argument(
        "--params",
        default="{}",
        help='JSON object of pipeline kwargs, e.g. \'{"limit": 500000, "year_from": 1990}\'',
    )
    return json.loads(parser.parse_args().params)


async def _run_inline(**kwargs) -> None:
    await ajob_test(await main(**kwargs))


if __name__ == "__main__":
    asyncio.run(_run_inline(**_parse_params()))
