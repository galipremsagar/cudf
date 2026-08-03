# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""All-to-all repartitioning across GPUs.

Shuffling is what turns a pile of independent per-GPU chunks into something
that can answer key-based questions (group-by, join, distinct, sort).  Every
row is assigned a destination partition, each source chunk is split by that
assignment, and the pieces are moved to the GPU that owns the destination
partition.  Afterwards, all rows sharing a key live on one device, so the
final pass is an ordinary single-GPU cuDF call.
"""

from __future__ import annotations

from typing import Any, Sequence

import cudf

from . import _transfer
from ._frame import (
    ChunkedDataFrame,
    ChunkedFrame,
    ChunkedSeries,
    _concat_parts,
    _wrap_like,
)

__all__ = ["hash_shuffle", "map_shuffle", "assign_targets"]


def assign_targets(nparts: int, devices: Sequence[int]) -> list[int]:
    """Round-robin partition -> device assignment."""
    return [devices[i % len(devices)] for i in range(nparts)]


def _hash_partition_chunk(chunk, on: list, nparts: int):
    if len(chunk) == 0:
        return [chunk.head(0) for _ in range(nparts)]
    return chunk.partition_by_hash(on, nparts)


def _exchange(
    frame: ChunkedFrame,
    parts_per_chunk: list[list[Any]],
    nparts: int,
    targets: Sequence[int],
    meta,
) -> ChunkedFrame:
    """Move every ``parts_per_chunk[i][p]`` to ``targets[p]`` and concatenate."""
    runtime = frame.runtime
    moves: list[tuple[Any, int, int]] = []
    owners: list[list[int]] = [[] for _ in range(nparts)]
    for parts, src_device in zip(parts_per_chunk, frame._devices, strict=True):
        for p, part in enumerate(parts):
            if len(part) == 0:
                # Nothing to send; skip the transfer entirely.
                continue
            owners[p].append(len(moves))
            moves.append((part, src_device, targets[p]))

    moved = _transfer.move_batch(moves, runtime=runtime)

    jobs = []
    for p in range(nparts):
        pieces = [moved[k] for k in owners[p]]
        jobs.append((targets[p], _concat_parts, (pieces, meta), {}))
    new_chunks = runtime.run_many(jobs)
    return _wrap_like(new_chunks, list(targets), runtime)


def hash_shuffle(
    frame: ChunkedFrame,
    on: Sequence,
    nparts: int | None = None,
    devices: Sequence[int] | None = None,
) -> ChunkedFrame:
    """Repartition ``frame`` so equal values of ``on`` share a partition.

    Uses the same hash (murmur3, default seed) on every chunk, so two frames
    shuffled with the same ``nparts`` and key dtypes are co-partitioned and can
    be joined locally afterwards.
    """
    runtime = frame.runtime
    devices = list(devices) if devices is not None else list(runtime.devices)
    nparts = nparts or len(devices)
    targets = assign_targets(nparts, devices)
    on = list(on)

    parts_per_chunk = frame._run_chunks(
        lambda c: _hash_partition_chunk(c, on, nparts)
    )
    return _exchange(frame, parts_per_chunk, nparts, targets, frame._meta)


def map_shuffle(
    frame: ChunkedFrame,
    part_ids: ChunkedSeries,
    nparts: int,
    devices: Sequence[int] | None = None,
) -> ChunkedFrame:
    """Repartition using an explicit per-row destination partition id.

    Used by range-partitioning (distributed sort), where the destination is
    chosen from sampled splitters rather than a hash.
    """
    runtime = frame.runtime
    devices = list(devices) if devices is not None else list(runtime.devices)
    targets = assign_targets(nparts, devices)

    def _scatter(chunk, ids):
        if len(chunk) == 0:
            return [chunk.head(0) for _ in range(nparts)]
        # ``scatter_by_map`` is a DataFrame method; route Series through a
        # one-column frame and unwrap afterwards.
        series_name = None
        if isinstance(chunk, cudf.Series):
            series_name = chunk.name
            frame = chunk.to_frame(name="__mg_value")
        else:
            frame = chunk
        parts = frame.scatter_by_map(ids, map_size=nparts)
        # scatter_by_map may return fewer partitions than requested
        while len(parts) < nparts:
            parts.append(frame.head(0))
        if series_name is not None or isinstance(chunk, cudf.Series):
            parts = [p["__mg_value"].rename(series_name) for p in parts]
        return parts

    parts_per_chunk = frame._run_chunks(_scatter, part_ids)
    return _exchange(frame, parts_per_chunk, nparts, targets, frame._meta)
