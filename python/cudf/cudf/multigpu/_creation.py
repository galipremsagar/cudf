# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Constructors for multi-GPU chunked frames."""

from __future__ import annotations

import itertools
from typing import Any, Callable, Sequence

import numpy as np
import pandas as pd

import cudf

from . import _transfer
from ._frame import (
    ChunkedDataFrame,
    ChunkedFrame,
    ChunkedIndex,
    ChunkedSeries,
    _even_bounds,
    _wrap_like,
)
from ._runtime import DeviceRuntime, get_runtime

__all__ = [
    "from_pandas",
    "from_cudf",
    "from_chunks",
    "build",
    "concat",
]


def _default_layout(
    npartitions: int | None,
    devices: Sequence[int] | None,
    runtime: DeviceRuntime,
) -> tuple[int, list[int]]:
    devices = list(devices) if devices is not None else list(runtime.devices)
    npartitions = npartitions or len(devices)
    targets = [devices[i % len(devices)] for i in range(npartitions)]
    return npartitions, targets


def from_pandas(
    obj,
    npartitions: int | None = None,
    devices: Sequence[int] | None = None,
    runtime: DeviceRuntime | None = None,
) -> ChunkedFrame:
    """Distribute a host (pandas) object across GPUs.

    Each slice is uploaded directly to its destination GPU, so the full frame
    never has to fit on any single device.
    """
    runtime = runtime or get_runtime()
    npartitions, targets = _default_layout(npartitions, devices, runtime)
    bounds = _even_bounds(len(obj), npartitions)

    jobs = []
    for i in range(npartitions):
        # An Index has no .iloc; plain slicing works for Index, Series and
        # DataFrame alike (on a DataFrame it slices rows, which is what the
        # bounds mean).
        piece = _slice_rows(obj, bounds[i], bounds[i + 1])
        jobs.append((targets[i], _upload, (piece,), {}))
    chunks = runtime.run_many(jobs)
    return _wrap_like(chunks, targets, runtime)


def _slice_rows(obj, start: int, stop: int):
    """Rows ``start:stop`` of a pandas DataFrame, Series or Index."""
    iloc = getattr(obj, "iloc", None)
    if iloc is not None:
        return iloc[start:stop]
    return obj[start:stop]


def _upload(piece):
    return cudf.from_pandas(piece)


def from_cudf(
    obj,
    npartitions: int | None = None,
    devices: Sequence[int] | None = None,
    runtime: DeviceRuntime | None = None,
    source_device: int | None = None,
) -> ChunkedFrame:
    """Split a single-GPU cuDF object and spread it over several GPUs."""
    runtime = runtime or get_runtime()
    npartitions, targets = _default_layout(npartitions, devices, runtime)
    source_device = runtime.devices[0] if source_device is None else source_device
    bounds = _even_bounds(len(obj), npartitions)

    pieces = runtime.run(
        source_device,
        lambda o, b: [o.iloc[b[i] : b[i + 1]] for i in range(len(b) - 1)],
        obj,
        bounds,
    )
    moved = _transfer.move_batch(
        [(p, source_device, t) for p, t in zip(pieces, targets, strict=True)],
        runtime=runtime,
    )
    return _wrap_like(moved, targets, runtime)


def from_chunks(
    chunks: Sequence[Any],
    devices: Sequence[int],
    runtime: DeviceRuntime | None = None,
) -> ChunkedFrame:
    """Wrap chunks that already live on the given devices."""
    return _wrap_like(list(chunks), list(devices), runtime or get_runtime())


def build(
    npartitions: int,
    factory: Callable[..., Any],
    devices: Sequence[int] | None = None,
    runtime: DeviceRuntime | None = None,
    **kwargs,
) -> ChunkedFrame:
    """Create each chunk *on its own GPU* by calling ``factory(i, npartitions)``.

    This is the only way to materialize a frame larger than host RAM or larger
    than one GPU: nothing is ever assembled centrally.
    """
    runtime = runtime or get_runtime()
    _, targets = _default_layout(npartitions, devices, runtime)
    jobs = [
        (targets[i], factory, (i, npartitions), dict(kwargs))
        for i in range(npartitions)
    ]
    return _wrap_like(runtime.run_many(jobs), targets, runtime)


def concat(objs: Sequence[Any], **kwargs) -> ChunkedFrame:
    """Concatenate multi-GPU frames row-wise, preserving chunk placement.

    Chunks are not moved: the result simply owns every input's chunks in
    order, so concatenating is free.
    """
    from ._frame import unwrap_proxy

    chunks: list[Any] = []
    devices: list[int] = []
    runtime = None
    resolved = [unwrap_proxy(obj) for obj in objs]
    for obj in resolved:
        if not isinstance(obj, ChunkedFrame):
            raise TypeError(f"expected a chunked frame, got {type(obj).__name__}")
        runtime = runtime or obj.runtime
        chunks.extend(obj._chunks)
        devices.extend(obj._devices)
    out = _wrap_like(chunks, devices, runtime)
    if kwargs.get("ignore_index"):
        out = out.reset_index(drop=True)
    return out
