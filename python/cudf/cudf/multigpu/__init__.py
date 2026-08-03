# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Multi-GPU chunked cuDF (experimental).

Treats the memory of every GPU in the box as one pool by partitioning a
DataFrame by rows across devices.  Each chunk is an ordinary
:class:`cudf.DataFrame` living on one GPU; the wrapper dispatches work to the
right device and moves data between devices when an operation needs it.

    >>> import cudf.multigpu as mgpu
    >>> mgpu.init()                              # one worker thread per GPU
    >>> df = mgpu.read_parquet("data/*.parquet") # chunks land on all GPUs
    >>> df.groupby("key").agg({"value": "sum"})  # shuffle + local aggregate

No changes to libcudf are required: everything is built from public
pylibcudf/cuDF primitives plus peer-to-peer copies.
"""

from cudf.multigpu._creation import (
    build,
    concat,
    from_chunks,
    from_cudf,
    from_pandas,
)
from cudf.multigpu._frame import (
    ChunkedDataFrame,
    ChunkedFrame,
    ChunkedIndex,
    ChunkedSeries,
)
from cudf.multigpu._runtime import (
    DeviceRuntime,
    get_runtime,
    init,
    is_initialized,
    shutdown,
)
from cudf.multigpu._shuffle import hash_shuffle, map_shuffle
from cudf.multigpu._transfer import broadcast, gather_concat, move, move_batch

__all__ = [
    # runtime
    "DeviceRuntime",
    "get_runtime",
    "init",
    "is_initialized",
    "shutdown",
    # types
    "ChunkedDataFrame",
    "ChunkedFrame",
    "ChunkedIndex",
    "ChunkedSeries",
    # constructors
    "build",
    "concat",
    "from_chunks",
    "from_cudf",
    "from_pandas",
    # data movement
    "broadcast",
    "gather_concat",
    "move",
    "move_batch",
    "hash_shuffle",
    "map_shuffle",
]


def __getattr__(name):
    # IO entry points are imported lazily so that ``import cudf.multigpu`` does
    # not pull in the parquet/csv machinery unless it is used.
    if name in ("read_parquet", "read_csv", "read_orc", "to_parquet"):
        from cudf.multigpu import _io

        return getattr(_io, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
