"""Expose download, inventory, build, and idempotency commands to operators."""

import argparse
import sys

from .config import load_config
from .download import download_all
from .run import run_inventory, run_pipeline, run_reload_test


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Claims silver pipeline")
    parser.add_argument("command", choices=["download", "inventory", "run", "reload-test"])
    args = parser.parse_args(argv)
    cfg = load_config()

    if args.command == "download":
        download_all(cfg)
    elif args.command == "inventory":
        run_inventory(cfg)
    elif args.command == "run":
        run_pipeline(cfg)
    elif args.command == "reload-test":
        run_reload_test(cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
