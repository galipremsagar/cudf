# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""One module per TPC-DS query, each exposing ``query(run_config)``.

Split per query rather than gathered into one file so that translations can be
written and revised independently. A query that has not been translated yet is
simply an absent module, which ``available()`` reports and the runner counts
separately from a query that ran and failed -- the two mean different things.
"""

from __future__ import annotations

import importlib
from typing import Callable

__all__ = ["get", "available", "COUNT"]

COUNT = 99


def get(number: int) -> Callable | None:
    """The translation for ``number``, or None if it has not been written."""
    try:
        module = importlib.import_module(f"{__name__}.q{number}")
    except ModuleNotFoundError:
        return None
    return getattr(module, "query", None)


def available() -> list[int]:
    return [n for n in range(1, COUNT + 1) if get(n) is not None]
