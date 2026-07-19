import argparse
import asyncio
import json

from . import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Register the GitHub exposure scanner job")
    parser.add_argument("--params", type=str, default=None, help="JSON dict of job params")
    args = parser.parse_args()
    kwargs = json.loads(args.params) if args.params else {}
    asyncio.run(main(**kwargs))
