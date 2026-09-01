"""Load configurable contract thresholds and AWS destinations from config.toml."""

import tomllib

from .constants import ROOT


def load_config() -> dict:
    with open(ROOT / "config.toml", "rb") as f:
        return tomllib.load(f)
