# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Readers that land data directly on the GPU that will own it.

This is the part that makes a dataset larger than one GPU usable at all.
``cudf.read_parquet`` builds the whole table on the current device before you
can split it, so a 500 GB file simply cannot be opened.  Here the file's row
groups are assigned to devices *first*, and each device's pinned thread reads
only its own row groups straight into its own memory pool.  No device ever
holds more than its share.
"""

from __future__ import annotations

import glob
import math
import os
from typing import Any, Sequence

import pylibcudf as plc

import cudf

from ._frame import ChunkedDataFrame, _wrap_like
from ._runtime import DeviceRuntime, get_runtime

__all__ = ["read_parquet", "read_csv", "to_parquet", "parquet_row_group_plan"]


def _expand(paths) -> list[str]:
    if isinstance(paths, (str, os.PathLike)):
        text = os.fspath(paths)
        if any(ch in text for ch in "*?["):
            return sorted(glob.glob(text))
        if os.path.isdir(text):
            return sorted(glob.glob(os.path.join(text, "*.parquet")))
        return [text]
    return [os.fspath(p) for p in paths]


# ----------------------------------------------------------------------
# planning
# ----------------------------------------------------------------------
def parquet_row_group_plan(
    paths: Sequence[str], nparts: int
) -> tuple[list[list[list[int]]], list[int], list[int]]:
    """Split a parquet dataset into ``nparts`` contiguous row-group runs.

    Returns ``(per_part_row_groups, per_part_rows, per_part_bytes)`` where
    ``per_part_row_groups[p]`` is a ``list[list[int]]`` -- one inner list per
    source file, as ``ParquetReaderOptions.set_row_groups`` expects.

    Runs are contiguous in global row-group order so the concatenation of the
    parts reproduces the file's row order, and they are balanced by
    *uncompressed bytes* rather than row count so that wide/narrow row groups
    do not skew placement.
    """
    source = plc.io.SourceInfo(list(paths))
    metadata = plc.io.parquet_metadata.read_parquet_metadata(source)
    per_file = list(metadata.num_rowgroups_per_file())
    row_groups = list(metadata.rowgroup_metadata())

    # global row-group index -> (file index, index within that file)
    locations: list[tuple[int, int]] = []
    for file_index, count in enumerate(per_file):
        locations.extend((file_index, i) for i in range(count))

    sizes = [int(rg["total_byte_size"]) for rg in row_groups]
    rows = [int(rg["num_rows"]) for rg in row_groups]
    total_bytes = sum(sizes) or 1

    # Assign each row group to the part whose byte-share contains the row
    # group's midpoint.  Midpoints increase monotonically, so every part gets a
    # contiguous run (preserving row order) and the split is balanced by bytes.
    plans: list[list[list[int]]] = [[[] for _ in paths] for _ in range(nparts)]
    part_rows = [0] * nparts
    part_bytes = [0] * nparts
    cursor = 0
    for global_index, (size, nrows) in enumerate(zip(sizes, rows, strict=True)):
        midpoint = cursor + size / 2
        part = min(nparts - 1, int(midpoint / total_bytes * nparts))
        file_index, local_index = locations[global_index]
        plans[part][file_index].append(local_index)
        part_rows[part] += nrows
        part_bytes[part] += size
        cursor += size
    return plans, part_rows, part_bytes


# ----------------------------------------------------------------------
# parquet
# ----------------------------------------------------------------------
def read_parquet(
    paths,
    columns: Sequence[str] | None = None,
    npartitions: int | None = None,
    devices: Sequence[int] | None = None,
    runtime: DeviceRuntime | None = None,
    **kwargs,
) -> ChunkedDataFrame:
    """Read a parquet dataset spread across GPUs, one row-group run per chunk.

    Parameters
    ----------
    paths
        A file, directory, glob, or list of files.
    columns
        Optional projection, pushed down to the reader.
    npartitions
        Number of chunks.  Defaults to one per device.
    """
    runtime = runtime or get_runtime()
    devices = list(devices) if devices is not None else list(runtime.devices)
    npartitions = npartitions or len(devices)
    files = _expand(paths)
    if not files:
        raise FileNotFoundError(f"no parquet files matched {paths!r}")

    plans, part_rows, _part_bytes = parquet_row_group_plan(files, npartitions)
    targets = [devices[i % len(devices)] for i in range(npartitions)]

    jobs = [
        (targets[i], _read_parquet_part, (files, plans[i], columns, kwargs), {})
        for i in range(npartitions)
    ]
    chunks = runtime.run_many(jobs)
    frame = _wrap_like(chunks, targets, runtime)
    frame._lengths_cache = list(part_rows)
    return frame


def _read_parquet_part(files, row_groups, columns, kwargs):
    """Runs on the destination device's thread; allocates only there."""
    source = plc.io.SourceInfo(list(files))
    builder = plc.io.parquet.ParquetReaderOptions.builder(source)
    options = builder.build()
    if any(row_groups):
        options.set_row_groups(row_groups)
    else:
        # This part owns no row groups; read the schema only.
        options.set_num_rows(0)
    if columns is not None:
        options.set_column_names(list(columns))
    table = plc.io.parquet.read_parquet(options)
    return cudf.DataFrame.from_pylibcudf(table)


# ----------------------------------------------------------------------
# csv
# ----------------------------------------------------------------------
def read_csv(
    paths,
    npartitions: int | None = None,
    devices: Sequence[int] | None = None,
    runtime: DeviceRuntime | None = None,
    **kwargs,
) -> ChunkedDataFrame:
    """Read a CSV by byte range, one range per GPU.

    libcudf snaps each byte range to record boundaries, so the ranges tile the
    file exactly once.  Only the range containing byte 0 sees the header.
    """
    runtime = runtime or get_runtime()
    devices = list(devices) if devices is not None else list(runtime.devices)
    npartitions = npartitions or len(devices)
    files = _expand(paths)
    if len(files) != 1:
        raise NotImplementedError(
            "multi-GPU read_csv currently handles a single file; pass one path"
        )
    path = files[0]
    size = os.path.getsize(path)
    step = math.ceil(size / npartitions)

    header_names = list(
        cudf.read_csv(path, nrows=0, **kwargs).columns
    )
    targets = [devices[i % len(devices)] for i in range(npartitions)]
    jobs = [
        (
            targets[i],
            _read_csv_part,
            (path, i * step, min(step, max(0, size - i * step)), i == 0,
             header_names, kwargs),
            {},
        )
        for i in range(npartitions)
    ]
    return _wrap_like(runtime.run_many(jobs), targets, runtime)


def _read_csv_part(path, offset, length, is_first, names, kwargs):
    if length <= 0:
        return cudf.read_csv(path, nrows=0, **kwargs)
    if is_first:
        return cudf.read_csv(
            path, byte_range=(offset, length), **kwargs
        )
    return cudf.read_csv(
        path, byte_range=(offset, length), header=None, names=names, **kwargs
    )


# ----------------------------------------------------------------------
# writing
# ----------------------------------------------------------------------
def to_parquet(frame: ChunkedDataFrame, path: str, **kwargs) -> list[str]:
    """Write one parquet file per chunk into ``path``, from each chunk's GPU."""
    os.makedirs(path, exist_ok=True)
    width = len(str(max(frame.nchunks - 1, 0)))
    names = [
        os.path.join(path, f"part.{str(i).zfill(width)}.parquet")
        for i in range(frame.nchunks)
    ]
    jobs = [
        (device, _write_parquet_part, (chunk, names[i], kwargs), {})
        for i, (chunk, device) in enumerate(
            zip(frame._chunks, frame._devices, strict=True)
        )
    ]
    frame.runtime.run_many(jobs)
    return names


def _write_parquet_part(chunk, name, kwargs):
    chunk.to_parquet(name, **kwargs)
    return name
