"""Backward-compatible entry point. Implementation lives in claims_pipeline/."""

from claims_pipeline.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
