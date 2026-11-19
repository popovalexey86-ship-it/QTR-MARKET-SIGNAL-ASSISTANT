"""Explicit input and address-space budgets for history-wide offline commands."""

from __future__ import annotations

import argparse
import importlib
import math
import os
from pathlib import Path
from typing import Any, cast


def add_budget_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--max-input-mib", type=float, required=True)
    parser.add_argument("--max-address-space-mib", type=float, required=True)


def enforce_budget(
    parser: argparse.ArgumentParser,
    *,
    roots: tuple[Path, ...],
    max_input_mib: float,
    max_address_space_mib: float,
) -> None:
    if any(
        not math.isfinite(value) or value <= 0
        for value in (max_input_mib, max_address_space_mib)
    ):
        parser.error("Offline resource budgets must be finite and positive.")
    input_limit = int(max_input_mib * 1024**2)
    total = 0
    for root in roots:
        if root.is_file():
            total += root.stat().st_size
        elif root.is_dir():
            for directory, _names, files in os.walk(root, followlinks=False):
                for filename in files:
                    path = Path(directory) / filename
                    if path.is_file():
                        total += path.stat().st_size
                        if total > input_limit:
                            parser.error(
                                "Offline input exceeds the declared size budget."
                            )
        if total > input_limit:
            parser.error("Offline input exceeds the declared size budget.")

    try:
        resource = cast(Any, importlib.import_module("resource"))
    except ModuleNotFoundError:
        parser.error("Address-space containment is unavailable on this host.")
    address_limit = int(max_address_space_mib * 1024**2)
    _soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    if hard != resource.RLIM_INFINITY and address_limit > hard:
        parser.error("Declared address-space budget exceeds the host hard limit.")
    try:
        resource.setrlimit(resource.RLIMIT_AS, (address_limit, hard))
    except (OSError, ValueError) as error:
        parser.error(f"Cannot enforce offline address-space budget: {error}")
