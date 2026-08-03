# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Operations whose result at row *i* depends on rows in earlier chunks.

Two shapes appear here:

* **Scans** (``cumsum`` and friends) -- run the scan inside each chunk, take
  each chunk's final value, prefix-combine those few values on the host, then
  fold the carry into each chunk.  One extra pass, no data movement.
* **Boundary ops** (``shift``, ``diff``) -- a row near the start of a chunk
  needs the last few rows of the previous chunk.  Only those few rows move.

Both avoid the naive answer of gathering the whole frame onto one GPU.
"""

from __future__ import annotations

import itertools
from typing import Any, Sequence

import numpy as np
import pandas as pd

import cudf

from ._frame import ChunkedDataFrame, ChunkedFrame, ChunkedSeries, _to_host, _wrap_like

__all__ = ["cumulative", "shift", "diff", "pct_change"]

#: scan name -> (cuDF method, how to combine two carries)
_SCANS = {
    "cumsum": ("cumsum", lambda a, b: a + b),
    "cumprod": ("cumprod", lambda a, b: a * b),
    "cummax": ("cummax", lambda a, b: _elementwise(a, b, np.maximum)),
    "cummin": ("cummin", lambda a, b: _elementwise(a, b, np.minimum)),
}

_IDENTITY = {"cumsum": 0, "cumprod": 1, "cummax": None, "cummin": None}


def _elementwise(a, b, op):
    if isinstance(a, pd.Series) or isinstance(b, pd.Series):
        return op(a, b)
    return op(a, b)


def cumulative(frame: ChunkedFrame, how: str, **kwargs) -> ChunkedFrame:
    """Distributed ``cumsum``/``cumprod``/``cummax``/``cummin``."""
    if how not in _SCANS:
        raise ValueError(f"unknown scan {how!r}")
    method, combine = _SCANS[how]

    local = frame.map_chunks(lambda c: getattr(c, method)(**kwargs))

    # Each chunk's last row is what the next chunk must start from.
    tails = local._run_chunks(_last_row)

    carries: list[Any] = []
    running = None
    for tail in tails:
        carries.append(running)
        if tail is None:
            continue
        running = tail if running is None else combine(running, tail)

    jobs = []
    for i, (chunk, device) in enumerate(zip(local._chunks, local._devices)):
        jobs.append((device, _add_carry, (chunk, carries[i], how), {}))
    return _wrap_like(
        frame.runtime.run_many(jobs), local._devices, frame.runtime
    )


def _last_row(chunk):
    """The scan state a chunk hands to its successor (host-side, one row)."""
    if len(chunk) == 0:
        return None
    tail = chunk.iloc[len(chunk) - 1 :]
    host = _to_host(tail)
    if isinstance(host, pd.DataFrame):
        return host.iloc[0]
    return host.iloc[0]


def _add_carry(chunk, carry, how):
    """Fold the running value from all preceding chunks into this one."""
    if carry is None or len(chunk) == 0:
        return chunk
    if isinstance(chunk, cudf.Series):
        if how == "cumsum":
            return chunk + carry
        if how == "cumprod":
            return chunk * carry
        if how == "cummax":
            return chunk.clip(lower=carry)
        return chunk.clip(upper=carry)

    out = chunk.copy(deep=False)
    for column in out.columns:
        value = carry[column]
        if pd.isna(value):
            continue
        if how == "cumsum":
            out[column] = out[column] + value
        elif how == "cumprod":
            out[column] = out[column] * value
        elif how == "cummax":
            out[column] = out[column].clip(lower=value)
        else:
            out[column] = out[column].clip(upper=value)
    return out


# ----------------------------------------------------------------------
# boundary-crossing operations
# ----------------------------------------------------------------------
def shift(frame: ChunkedFrame, periods: int = 1, **kwargs) -> ChunkedFrame:
    """Distributed ``shift``: only the overlapping rows cross a device.

    For ``periods > 0`` chunk *i* is prefixed with the last ``periods`` rows of
    the chunks before it; for ``periods < 0`` it is suffixed with the first
    ``|periods|`` rows of the chunks after it.  The shift is then local.
    """
    if periods == 0:
        return frame.map_chunks(lambda c: c.copy(deep=True))

    n = abs(periods)
    runtime = frame.runtime
    forward = periods > 0

    # Collect the overlap rows on the host: at most n rows per chunk.
    edges = frame._run_chunks(
        lambda c: _to_host(c.tail(n) if forward else c.head(n))
    )

    jobs = []
    for i, (chunk, device) in enumerate(zip(frame._chunks, frame._devices)):
        neighbours = range(i - 1, -1, -1) if forward else range(i + 1, frame.nchunks)
        collected: list[Any] = []
        remaining = n
        for j in neighbours:
            if remaining <= 0:
                break
            edge = edges[j]
            if len(edge) == 0:
                continue
            take = edge.iloc[-remaining:] if forward else edge.iloc[:remaining]
            collected.append(take)
            remaining -= len(take)
        if collected:
            overlap = pd.concat(collected[::-1] if forward else collected, axis=0)
        else:
            overlap = edges[i].iloc[:0]
        jobs.append((device, _shift_with_overlap, (chunk, overlap, periods, kwargs), {}))

    return _wrap_like(runtime.run_many(jobs), frame._devices, runtime)


def _shift_with_overlap(chunk, overlap, periods, kwargs):
    """Runs on the chunk's device; ``overlap`` is a small host frame."""
    if len(chunk) == 0:
        return chunk
    n = abs(periods)
    if len(overlap) == 0:
        return chunk.shift(periods, **kwargs)

    device_overlap = cudf.from_pandas(overlap)
    if periods > 0:
        joined = cudf.concat([device_overlap, chunk])
        return joined.shift(periods, **kwargs).iloc[len(device_overlap) :]
    joined = cudf.concat([chunk, device_overlap])
    return joined.shift(periods, **kwargs).iloc[: len(chunk)]


def diff(frame: ChunkedFrame, periods: int = 1, **kwargs) -> ChunkedFrame:
    """``frame - frame.shift(periods)``, computed without a global gather."""
    shifted = shift(frame, periods)
    return frame.map_chunks(lambda a, b: a - b, shifted)


def pct_change(frame: ChunkedFrame, periods: int = 1, **kwargs) -> ChunkedFrame:
    shifted = shift(frame, periods)
    return frame.map_chunks(lambda a, b: (a - b) / b, shifted)
