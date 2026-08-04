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


#: bytes above which a shuffle is split into more partitions than devices, so
#: the streamed exchange has something to stream
LARGE_FRAME_BYTES = 4 << 30
PARTS_PER_DEVICE_WHEN_LARGE = 4


def assign_targets(nparts: int, devices: Sequence[int]) -> list[int]:
    """Round-robin partition -> device assignment."""
    return [devices[i % len(devices)] for i in range(nparts)]


def default_nparts(frame, devices: Sequence[int]) -> int:
    """How many partitions to shuffle into.

    One per device is the cheapest, but it also makes the exchange a single
    group, so the whole frame is in flight at once. Large frames get several
    partitions per device, which costs a few more kernel launches and buys a
    proportionally smaller peak.
    """
    base = max(getattr(frame, "nchunks", len(devices)), len(devices))
    try:
        large = frame.nbytes > LARGE_FRAME_BYTES
    except Exception:
        large = False
    return max(base, len(devices) * PARTS_PER_DEVICE_WHEN_LARGE) if large else base


def _hash_partition_chunk(chunk, on: list, nparts: int):
    if len(chunk) == 0:
        return [chunk.head(0) for _ in range(nparts)]
    return chunk.partition_by_hash(on, nparts)


def _release(parts: list, start: int, stop: int) -> None:
    """Drop source partitions on their own device's thread."""
    for p in range(start, stop):
        parts[p] = None


def _exchange(
    frame: ChunkedFrame,
    parts_per_chunk: list[list[Any]],
    nparts: int,
    targets: Sequence[int],
    meta,
) -> ChunkedFrame:
    """Move every ``parts_per_chunk[i][p]`` to ``targets[p]`` and concatenate.

    Done a group of destinations at a time rather than all at once. Moving
    everything in one go means the hash partitions, the serialization copies
    that ``device_serialize`` makes of them, the destination buffers, and the
    concatenated result are all live simultaneously -- about four times the
    frame, which is what made the widest joins fail at SF300. Working in groups
    of ``len(devices)`` keeps every GPU busy while holding only one group's
    worth in flight, and the source partitions are released (on their own
    device) as soon as their group lands.
    """
    runtime = frame.runtime
    group = max(1, len(set(targets)))
    new_chunks: list[Any] = [None] * nparts

    for start in range(0, nparts, group):
        stop = min(start + group, nparts)
        moves: list[tuple[Any, int, int]] = []
        owners: dict[int, list[int]] = {p: [] for p in range(start, stop)}
        for parts, src_device in zip(
            parts_per_chunk, frame._devices, strict=True
        ):
            for p in range(start, stop):
                part = parts[p]
                if part is None or len(part) == 0:
                    continue  # nothing to send; skip the transfer entirely
                owners[p].append(len(moves))
                moves.append((part, src_device, targets[p]))

        moved = _transfer.move_batch(moves, runtime=runtime)
        del moves

        # free the sources before allocating the concatenated result
        runtime.run_many([
            (device, _release, (parts, start, stop), {})
            for parts, device in zip(parts_per_chunk, frame._devices)
        ])

        jobs = [
            (targets[p], _concat_parts, ([moved[k] for k in owners[p]], meta), {})
            for p in range(start, stop)
        ]
        for p, chunk in zip(range(start, stop), runtime.run_many(jobs)):
            new_chunks[p] = chunk
        del moved

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
    nparts = nparts or default_nparts(frame, devices)
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
