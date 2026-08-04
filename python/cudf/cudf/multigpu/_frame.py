# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Row-partitioned DataFrame/Series spread across several GPUs.

A :class:`ChunkedDataFrame` holds an ordered list of ordinary
:class:`cudf.DataFrame` chunks, each resident on a (possibly different) GPU.
Concatenating the chunks in order reproduces the logical frame.  No chunk ever
has to fit on one device, so the aggregate memory of every GPU in the box is
usable as a single pool.

Every operation on a chunk is dispatched to that chunk's pinned worker thread,
because touching device memory from a thread whose current device differs is an
illegal access rather than a slow path.
"""

from __future__ import annotations

import functools
import itertools
import warnings
from typing import Any, Callable, Hashable, Iterable, Sequence

import numpy as np
import pandas as pd

import cudf

from . import _transfer
from ._runtime import DeviceRuntime, get_runtime

__all__ = ["ChunkedFrame", "ChunkedDataFrame", "ChunkedSeries", "ChunkedIndex"]

#: Prefix repr() with a chunk-placement banner. Useful when working with these
#: types directly; turned off by the cudf.pandas backend, where the object is
#: meant to be indistinguishable from a pandas frame.
SHOW_PLACEMENT_IN_REPR = True

#: Chunks to split into when a frame is constructed directly (e.g.
#: ``pd.DataFrame({...})`` under the accelerated-pandas backend). ``None``
#: means one chunk per GPU. Set by ``pandas_compat.install``.
DEFAULT_NPARTITIONS: int | None = None


def _banner(label: str, frame) -> str:
    return f"<{label} {frame._repr_summary()}>\n" if SHOW_PLACEMENT_IN_REPR else ""


# ----------------------------------------------------------------------
# helpers executed on a device worker thread
# ----------------------------------------------------------------------
def _call(obj, name: str, args: tuple, kwargs: dict):
    return getattr(obj, name)(*args, **kwargs)


def _getattr(obj, name: str):
    return getattr(obj, name)


def _apply(fn: Callable, *objs):
    return fn(*objs)


def _len(obj) -> int:
    return len(obj)


#: index types vary by cuDF version; resolve once at import time
_INDEX_TYPES = tuple(
    t
    for t in (
        getattr(cudf, "Index", None),
        getattr(cudf, "MultiIndex", None),
        getattr(cudf, "RangeIndex", None),
    )
    if isinstance(t, type)
)
_FRAME_TYPES = (cudf.DataFrame, cudf.Series, *_INDEX_TYPES)


def _to_host(obj):
    """Bring a small result off the GPU, preserving structure."""
    if isinstance(obj, _FRAME_TYPES):
        return obj.to_pandas()
    return obj


def _construct(cudf_type, data, kwargs):
    """Build a single-GPU cuDF object on the calling thread's device."""
    if data is None:
        return cudf_type(**kwargs)
    return cudf_type(data, **kwargs)


def _construct_distributed(cudf_type, data, kwargs, runtime):
    """Build from user data and spread the result over the GPUs.

    Direct construction has to distribute like ``from_pandas`` does; leaving
    the result on one device would quietly make everything derived from it
    single-GPU.
    """
    from . import _transfer

    source = runtime.devices[0]
    built = runtime.run(source, _construct, cudf_type, data, kwargs)
    nrows = len(built)
    npartitions = DEFAULT_NPARTITIONS or runtime.n_devices
    npartitions = max(1, min(npartitions, nrows))
    if npartitions <= 1:
        return [built], [source]

    devices = list(runtime.devices)
    targets = [devices[i % len(devices)] for i in range(npartitions)]
    bounds = _even_bounds(nrows, npartitions)
    pieces = runtime.run(
        source,
        lambda o, b: [o.iloc[b[i] : b[i + 1]] for i in range(len(b) - 1)],
        built,
        bounds,
    )
    moved = _transfer.move_batch(
        [(piece, source, target) for piece, target in zip(pieces, targets)],
        runtime=runtime,
    )
    return moved, targets


def _to_device(obj):
    """Upload a host object to the calling thread's device."""
    if isinstance(obj, (pd.DataFrame, pd.Series)):
        return cudf.from_pandas(obj)
    return obj


def _map_to_host(fn: Callable, obj):
    return _to_host(fn(obj))


def unwrap_proxy(obj):
    """Return the chunked frame behind a cudf.pandas proxy, if there is one.

    Under the accelerated-pandas backend our frames arrive wrapped in proxy
    objects, which are not instances of ChunkedFrame. Without unwrapping,
    operand checks like ``isinstance(right, ChunkedFrame)`` in ``merge`` see a
    stranger and take the wrong path.
    """
    if isinstance(obj, ChunkedFrame):
        return obj
    fast = getattr(obj, "_fsproxy_fast", None)
    return fast if isinstance(fast, ChunkedFrame) else obj


class _ChunkedMeta(type):
    """Resolves un-implemented cuDF names at the *class* level.

    ``_FallbackMixin.__getattr__`` handles instances, but cudf.pandas looks
    attributes up on the fast *class* (``getattr(owner._fsproxy_fast, name)``
    in cudf/pandas/fast_slow_proxy.py). Without a class-level hook every
    unimplemented name would resolve to ``_Unusable`` and silently take the
    pandas path, so the whole frame would round-trip to host memory.
    """

    def __getattr__(cls, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        cudf_type = getattr(cls, "_cudf_type", None)
        if cudf_type is None or not hasattr(cudf_type, name):
            raise AttributeError(
                f"type object {cls.__name__!r} has no attribute {name!r}"
            )
        target = getattr(cudf_type, name)
        if isinstance(target, property) or not callable(target):
            descriptor = property(
                lambda self, _name=name: self._single_gpu_fallback_property(_name)
            )
        else:
            def descriptor(self, *args, _name=name, **kwargs):
                return self._single_gpu_fallback(_name, args, kwargs)

            descriptor.__name__ = name
            descriptor.__doc__ = getattr(target, "__doc__", None)
        # Cache on the class so the next lookup is a plain attribute hit.
        setattr(cls, name, descriptor)
        return getattr(cls, name)


# ----------------------------------------------------------------------
class ChunkedFrame(metaclass=_ChunkedMeta):
    """Base class for row-partitioned multi-GPU frames."""

    #: the single-GPU cuDF type this wraps
    _cudf_type: type = object

    #: a not-yet-executed join, or None. Kept on this class rather than a
    #: subclass on purpose: cudf.pandas decides whether to wrap a result with
    #: `type(result) in get_final_type_map()`, an *exact* type test, so a
    #: subclass would silently escape the proxy -- taking --strict's
    #: fallback detection with it.
    _plan = None

    def __init__(
        self,
        chunks: Sequence[Any] = None,
        devices: Sequence[int] | None = None,
        runtime: DeviceRuntime | None = None,
        lengths: Sequence[int] | None = None,
        *,
        plan=None,
        **kwargs: Any,
    ) -> None:
        if plan is not None:
            # a planned join: no chunks until something needs them
            self._plan = plan
            self._chunks_: list | None = None
            self._devices_: list | None = None
            self._runtime = runtime or plan.runtime
            self._lengths_cache = None
            self._meta_cache = None
            return
        # Two calling conventions. Internally this is (chunks, devices, ...).
        # Externally -- e.g. `pd.DataFrame({...})` under the accelerated-pandas
        # backend -- it is a normal constructor, and `devices` is absent. In
        # that case build the object with cuDF and keep it as a single chunk.
        if devices is None:
            runtime = runtime or get_runtime()
            chunks, devices = _construct_distributed(
                type(self)._cudf_type, chunks, kwargs, runtime
            )
        chunks = list(chunks)
        devices = [int(d) for d in devices]
        if len(chunks) != len(devices):
            raise ValueError(
                f"got {len(chunks)} chunks but {len(devices)} device "
                "assignments; they must correspond one-to-one"
            )
        if not chunks:
            raise ValueError("a ChunkedFrame needs at least one chunk")
        self._chunks_ = chunks
        self._devices_ = devices
        self._runtime = runtime or get_runtime()
        self._lengths_cache: list[int] | None = (
            list(lengths) if lengths is not None else None
        )
        self._meta_cache: Any = None

    # ------------------------------------------------------------------
    # deferred execution
    # ------------------------------------------------------------------
    @property
    def _chunks(self):
        if self._plan is not None:
            self._materialize()
        return self._chunks_

    @_chunks.setter
    def _chunks(self, value) -> None:
        self._chunks_ = value
        self._plan = None

    @property
    def _devices(self):
        if self._plan is not None:
            self._materialize()
        return self._devices_

    @_devices.setter
    def _devices(self, value) -> None:
        self._devices_ = value

    def _materialize(self) -> "ChunkedFrame":
        """Run the pending plan in place. Idempotent.

        ``_plan`` is cleared *before* executing, so that a plan which re-enters
        ``_chunks`` while running cannot recurse forever -- but it is put back
        if execution fails. A frame with neither a plan nor chunks is corrupt:
        every later access returns ``None`` and raises somewhere unrelated. That
        matters most for the failure this guards, an out-of-memory join, because
        cudf.pandas responds to it by falling back and calling ``to_pandas()`` --
        so the corrupt state is reached immediately, and a ``TypeError`` about
        ``NoneType`` is what surfaces instead of the allocation failure.
        """
        plan, self._plan = self._plan, None
        if plan is not None:
            try:
                result = plan.execute()
            except BaseException:
                self._plan = plan
                raise
            self._chunks_ = result._chunks
            self._devices_ = result._devices
            self._lengths_cache = result._lengths_cache
            # the schema was derived from the plan's inputs; re-derive it from
            # the real result so a stale guess cannot outlive the plan
            self._meta_cache = None
        return self

    @property
    def is_pending(self) -> bool:
        """True while a planned join has not been executed."""
        return self._plan is not None

    # ------------------------------------------------------------------
    # structure
    # ------------------------------------------------------------------
    @property
    def nchunks(self) -> int:
        return len(self._chunks)

    @property
    def devices(self) -> tuple[int, ...]:
        """The device each chunk lives on."""
        return tuple(self._devices)

    @property
    def runtime(self) -> DeviceRuntime:
        return self._runtime

    @property
    def chunk_lengths(self) -> list[int]:
        if self._lengths_cache is None:
            self._lengths_cache = self._gather_host(_len)
        return list(self._lengths_cache)

    def __len__(self) -> int:
        return int(sum(self.chunk_lengths))

    @property
    def _partition_key(self) -> tuple:
        """Identifies partitioning for alignment checks."""
        return (tuple(self._devices), tuple(self.chunk_lengths))

    def _aligned_with(self, other: "ChunkedFrame") -> bool:
        return self._partition_key == other._partition_key

    @property
    def _meta(self):
        """An empty single-GPU object with this frame's schema (host copy)."""
        if self._plan is not None:
            # derived from the inputs' schemas; deliberately not cached, so it
            # cannot survive the plan it was derived from
            return self._plan.meta()
        if self._meta_cache is None:
            self._meta_cache = self._runtime.run(
                self._devices[0],
                lambda c: c.head(0).to_pandas(),
                self._chunks[0],
            )
        return self._meta_cache

    # ------------------------------------------------------------------
    # dispatch primitives
    # ------------------------------------------------------------------
    def _run_chunks(self, fn: Callable, *others: "ChunkedFrame") -> list[Any]:
        """Run ``fn(chunk, *other_chunks)`` on each chunk's own device."""
        for other in others:
            if isinstance(other, ChunkedFrame) and not self._aligned_with(other):
                raise ValueError(
                    "operands are not aligned: multi-GPU frames must share the "
                    "same partitioning (same devices and per-chunk row counts). "
                    "Use .repartition_like(other) first."
                )
        jobs = []
        for i, (chunk, device) in enumerate(zip(self._chunks, self._devices)):
            operands = tuple(
                o._chunks[i] if isinstance(o, ChunkedFrame) else o
                for o in others
            )
            jobs.append((device, _apply, (fn, chunk, *operands), {}))
        return self._runtime.run_many(jobs)

    def _gather_host(self, fn: Callable, *others: "ChunkedFrame") -> list[Any]:
        """Run ``fn`` per chunk and bring each (small) result to host."""
        wrapped = functools.partial(_map_to_host, fn)

        def _shim(chunk, *rest):
            return _to_host(fn(chunk, *rest))

        return self._run_chunks(_shim, *others)

    def map_chunks(
        self, fn: Callable, *others, broadcast: Sequence[Any] = (), **kwargs
    ) -> "ChunkedFrame":
        """Apply ``fn`` to every chunk, returning a new multi-GPU frame.

        ``fn`` must be row-wise: it may not depend on rows outside its chunk.

        Positional ``others`` must be chunked frames aligned with this one and
        are passed chunk-by-chunk.  Anything in ``broadcast`` is replicated to
        every device instead and passed whole -- use it for small lookup tables
        that each chunk needs in full.

        Evaluating a compound expression inside ``fn`` keeps its intermediates
        local to one chunk, so peak memory is set by the largest chunk rather
        than by the whole frame.
        """
        if kwargs:
            fn = functools.partial(fn, **kwargs)
        if not broadcast:
            results = self._run_chunks(fn, *others)
            return _wrap_like(results, self._devices, self._runtime)

        replicas = [self._replicate(item) for item in broadcast]
        for other in others:
            if isinstance(other, ChunkedFrame) and not self._aligned_with(other):
                raise ValueError(
                    "positional operands must be aligned with this frame; "
                    "pass unaligned lookup tables via broadcast=[...]"
                )
        jobs = []
        for i, (chunk, device) in enumerate(zip(self._chunks, self._devices)):
            aligned = tuple(
                o._chunks[i] if isinstance(o, ChunkedFrame) else o for o in others
            )
            shared = tuple(replica[device] for replica in replicas)
            jobs.append((device, _apply, (fn, chunk, *aligned, *shared), {}))
        return _wrap_like(
            self._runtime.run_many(jobs), self._devices, self._runtime
        )

    def _replicate(self, obj: Any) -> dict[int, Any]:
        """Place a full copy of ``obj`` on every device this frame uses."""
        targets = list(dict.fromkeys(self._devices))
        if isinstance(obj, ChunkedFrame):
            source = targets[0]
            gathered = obj.compute(source)
            return _transfer.broadcast(
                gathered, source, targets, self._runtime
            )
        return {
            device: self._runtime.run(device, _to_device, obj)
            for device in targets
        }

    # ------------------------------------------------------------------
    # materialization
    # ------------------------------------------------------------------
    def to_pandas(self, **kwargs) -> Any:
        """Collect to a single host (pandas) object.

        Each chunk is copied device-to-host from its own GPU, so this never
        requires the whole frame to fit on one device.
        """
        parts = self._run_chunks(lambda c: c.to_pandas(**kwargs))
        if len(parts) == 1:
            return parts[0]
        return pd.concat(parts, axis=0)

    def compute(self, device: int | None = None) -> Any:
        """Collect onto a single GPU and return an ordinary cuDF object.

        The result must fit in that one device's memory.
        """
        device = self._devices[0] if device is None else device
        return _transfer.gather_concat(
            list(zip(self._chunks, self._devices)),
            device,
            runtime=self._runtime,
        )

    to_cudf = compute

    def to_arrow(self):
        import pyarrow as pa

        parts = self._run_chunks(lambda c: c.to_arrow())
        return pa.concat_tables(parts) if len(parts) > 1 else parts[0]

    # ------------------------------------------------------------------
    # placement
    # ------------------------------------------------------------------
    def rechunk(
        self, devices: Sequence[int] | None = None, nchunks: int | None = None
    ) -> "ChunkedFrame":
        """Redistribute rows over ``devices`` in ``nchunks`` even pieces."""
        rt = self._runtime
        devices = list(devices) if devices is not None else list(rt.devices)
        nchunks = nchunks or len(devices)
        total = len(self)
        bounds = _even_bounds(total, nchunks)
        targets = [devices[i % len(devices)] for i in range(nchunks)]

        # Slice every source chunk into the pieces each target needs.
        src_offsets = list(itertools.accumulate([0] + self.chunk_lengths))
        moves: list[tuple[Any, int, int]] = []
        owner: list[list[int]] = [[] for _ in range(nchunks)]
        for si, (chunk, sdev) in enumerate(zip(self._chunks, self._devices)):
            s0, s1 = src_offsets[si], src_offsets[si + 1]
            cuts = []
            for ti in range(nchunks):
                t0, t1 = bounds[ti], bounds[ti + 1]
                lo, hi = max(s0, t0), min(s1, t1)
                if lo < hi:
                    cuts.append((ti, lo - s0, hi - s0))
            if not cuts:
                continue
            pieces = rt.run(
                sdev,
                lambda c, cs: [c.iloc[a:b] for _t, a, b in cs],
                chunk,
                cuts,
            )
            for (ti, _a, _b), piece in zip(cuts, pieces, strict=True):
                owner[ti].append(len(moves))
                moves.append((piece, sdev, targets[ti]))

        moved = _transfer.move_batch(moves, runtime=rt)
        jobs = []
        for ti in range(nchunks):
            parts = [moved[k] for k in owner[ti]]
            jobs.append((targets[ti], _concat_parts, (parts, self._meta), {}))
        new_chunks = rt.run_many(jobs)
        return _wrap_like(new_chunks, targets, rt)

    def repartition_like(self, other: "ChunkedFrame") -> "ChunkedFrame":
        """Repartition so this frame aligns row-for-row with ``other``."""
        if self._aligned_with(other):
            return self
        if len(self) != len(other):
            raise ValueError(
                f"cannot align frames of different length ({len(self)} vs "
                f"{len(other)})"
            )
        return self._rechunk_to(other.chunk_lengths, other.devices)

    def _rechunk_to(
        self, lengths: Sequence[int], devices: Sequence[int]
    ) -> "ChunkedFrame":
        rt = self._runtime
        bounds = list(itertools.accumulate([0, *lengths]))
        src_offsets = list(itertools.accumulate([0] + self.chunk_lengths))
        moves: list[tuple[Any, int, int]] = []
        owner: list[list[int]] = [[] for _ in lengths]
        for si, (chunk, sdev) in enumerate(zip(self._chunks, self._devices)):
            s0, s1 = src_offsets[si], src_offsets[si + 1]
            cuts = [
                (ti, max(s0, bounds[ti]) - s0, min(s1, bounds[ti + 1]) - s0)
                for ti in range(len(lengths))
                if max(s0, bounds[ti]) < min(s1, bounds[ti + 1])
            ]
            if not cuts:
                continue
            pieces = rt.run(
                sdev, lambda c, cs: [c.iloc[a:b] for _t, a, b in cs], chunk, cuts
            )
            for (ti, _a, _b), piece in zip(cuts, pieces, strict=True):
                owner[ti].append(len(moves))
                moves.append((piece, sdev, devices[ti]))
        moved = _transfer.move_batch(moves, runtime=rt)
        jobs = [
            (devices[ti], _concat_parts, ([moved[k] for k in owner[ti]], self._meta), {})
            for ti in range(len(lengths))
        ]
        return _wrap_like(rt.run_many(jobs), list(devices), rt)

    # ------------------------------------------------------------------
    def memory_usage_per_device(self) -> dict[int, int]:
        """Bytes of device memory this frame occupies, per GPU."""
        per_chunk = self._run_chunks(
            lambda c: int(c.memory_usage(deep=True).sum())
            if hasattr(c.memory_usage(deep=True), "sum")
            else int(c.memory_usage(deep=True))
        )
        out: dict[int, int] = {}
        for device, nbytes in zip(self._devices, per_chunk):
            out[device] = out.get(device, 0) + int(nbytes)
        return out

    @property
    def nbytes(self) -> int:
        return int(sum(self.memory_usage_per_device().values()))


def _concat_parts(parts: list[Any], meta: Any):
    parts = [p for p in parts if len(p) > 0] or parts
    if not parts:
        return cudf.from_pandas(meta)
    if len(parts) == 1:
        return parts[0]
    return cudf.concat(parts)


def _even_bounds(total: int, nchunks: int) -> list[int]:
    base, extra = divmod(total, nchunks)
    bounds = [0]
    for i in range(nchunks):
        bounds.append(bounds[-1] + base + (1 if i < extra else 0))
    return bounds


def drop_empty_chunks(frame: "ChunkedFrame") -> "ChunkedFrame":
    """Drop zero-row chunks, keeping at least one.

    Needed after ``groupby.apply``: cuDF returns the *input* schema when the
    input is empty, rather than the schema the UDF produces, so an empty chunk
    poisons the concatenation with the pre-aggregation columns.
    """
    lengths = frame.chunk_lengths
    keep = [i for i, length in enumerate(lengths) if length > 0]
    if not keep or len(keep) == len(lengths):
        return frame
    return type(frame)(
        [frame._chunks[i] for i in keep],
        [frame._devices[i] for i in keep],
        frame.runtime,
        lengths=[lengths[i] for i in keep],
    )


def _wrap_like(chunks: Sequence[Any], devices: Sequence[int], runtime):
    """Wrap per-chunk results in the right multi-GPU class."""
    sample = next((c for c in chunks if c is not None), None)
    if isinstance(sample, cudf.DataFrame):
        return ChunkedDataFrame(chunks, devices, runtime)
    if isinstance(sample, cudf.Series):
        return ChunkedSeries(chunks, devices, runtime)
    if isinstance(sample, _INDEX_TYPES):
        return ChunkedIndex(chunks, devices, runtime)
    return list(chunks)


# ----------------------------------------------------------------------
# operand substitution: let a chunked operand appear anywhere in args
# ----------------------------------------------------------------------
def _resolve_exprs(value, frame):
    """Replace any deferred Expr with its value on ``frame``."""
    from ._lazy import Expr

    if isinstance(value, Expr):
        return value.evaluate(frame)
    if isinstance(value, tuple):
        return tuple(_resolve_exprs(v, frame) for v in value)
    if isinstance(value, list):
        return [_resolve_exprs(v, frame) for v in value]
    if isinstance(value, dict):
        return {k: _resolve_exprs(v, frame) for k, v in value.items()}
    return value


def _find_chunked(value, out: list) -> None:
    value = unwrap_proxy(value)
    if isinstance(value, ChunkedFrame):
        out.append(value)
    elif isinstance(value, (list, tuple)):
        for v in value:
            _find_chunked(v, out)
    elif isinstance(value, dict):
        for v in value.values():
            _find_chunked(v, out)


def _substitute(value, i: int):
    value = unwrap_proxy(value)
    if isinstance(value, ChunkedFrame):
        return value._chunks[i]
    if isinstance(value, tuple):
        return tuple(_substitute(v, i) for v in value)
    if isinstance(value, list):
        return [_substitute(v, i) for v in value]
    if isinstance(value, dict):
        return {k: _substitute(v, i) for k, v in value.items()}
    return value


# ----------------------------------------------------------------------
# reduction combiners (run on host over tiny per-chunk partials)
# ----------------------------------------------------------------------
def _stack(parts: list) -> pd.DataFrame | pd.Series:
    """Stack per-chunk partials: rows are chunks."""
    if isinstance(parts[0], pd.Series):
        return pd.DataFrame(list(parts))
    return pd.Series(list(parts))


def _combine(parts: list, how: str, skipna: bool = True):
    stacked = _stack(parts)
    if how in ("sum", "count", "prod"):
        return getattr(stacked, how)(skipna=skipna)
    if how in ("min", "max"):
        return getattr(stacked, how)(skipna=skipna)
    if how == "any":
        return stacked.any()
    if how == "all":
        return stacked.all()
    raise ValueError(f"unknown combiner {how!r}")


def _combine_moments(counts, means, m2s):
    """Chan et al. pairwise combination of (count, mean, M2) partials.

    Numerically stable, unlike combining raw sums of squares.
    """
    n = _stack(counts).sum()
    stacked_c = _stack(counts)
    stacked_m = _stack(means)
    stacked_v = _stack(m2s)
    if isinstance(stacked_c, pd.DataFrame):
        total_n = stacked_c.sum()
        mean = (stacked_c * stacked_m).sum() / total_n.replace(0, np.nan)
        delta = stacked_m.sub(mean, axis=1)
        m2 = (stacked_v + stacked_c * delta * delta).sum()
        return total_n, mean, m2
    total_n = stacked_c.sum()
    mean = (stacked_c * stacked_m).sum() / (total_n if total_n else np.nan)
    delta = stacked_m - mean
    m2 = (stacked_v + stacked_c * delta * delta).sum()
    return total_n, mean, m2


#: methods that are purely row-wise: run them unchanged on every chunk
#: Strictly row-wise methods: the answer for a row depends only on that row,
#: so applying them chunk by chunk and concatenating is exactly equivalent.
#: Anything that inspects other rows (interpolate), reorders rows (melt),
#: numbers rows (factorize), compares across rows (duplicated), or can infer a
#: different result *type* per chunk (convert_dtypes) does NOT belong here --
#: it would silently produce per-chunk-local answers.
_MAP_METHODS_COMMON = (
    "abs", "add", "astype", "between", "ceil", "clip", "copy", "cos",
    "div", "divide", "dot", "eq", "exp", "fillna", "floor", "floordiv",
    "ge", "gt", "isin", "isna", "isnull", "le", "log", "lt", "mod", "mul",
    "multiply", "ne", "notna", "notnull", "nans_to_nulls", "pow", "radd",
    "rdiv", "repeat", "rfloordiv", "rmod", "rmul", "round", "rpow", "rsub",
    "rtruediv", "sin", "sqrt", "sub", "subtract", "tan", "truediv", "where",
    "mask", "replace", "rename", "pipe",
)

_MAP_METHODS_DATAFRAME = (
    "assign", "drop", "eval", "query", "select_dtypes", "insert", "pop",
    "rename_axis", "add_prefix", "add_suffix", "applymap",
    "hash_values", "interleave_columns",
)

_MAP_METHODS_SERIES = (
    "map", "str_cat", "explode", "to_frame", "hash_values",
)

#: reductions with an associative host-side combine
_SIMPLE_REDUCTIONS = {
    "sum": "sum",
    "product": "prod",
    "prod": "prod",
    "min": "min",
    "max": "max",
    "count": "sum",
    "any": "any",
    "all": "all",
}


class _ReductionMixin:
    """Map-reduce implementations of cuDF's reduction API."""

    def _reduce(self, name: str, combiner: str, *args, **kwargs):
        skipna = kwargs.get("skipna", True)
        parts = self._run_chunks(
            lambda c: _to_host(getattr(c, name)(*args, **kwargs))
        )
        parts = [p for p in parts if p is not None]
        return _combine(parts, combiner, skipna=skipna)

    def mean(self, *args, **kwargs):
        counts = self._run_chunks(lambda c: _to_host(c.count(*args, **kwargs)))
        sums = self._run_chunks(lambda c: _to_host(c.sum(*args, **kwargs)))
        total = _combine(counts, "sum")
        return _combine(sums, "sum") / total

    def _moments(self, *args, **kwargs):
        def per_chunk(c):
            return (
                _to_host(c.count()),
                _to_host(c.mean()),
                _to_host(c.var(ddof=0)) ,
            )

        triples = self._run_chunks(per_chunk)
        counts = [t[0] for t in triples]
        means = [t[1] for t in triples]
        # M2 = var(ddof=0) * count
        m2s = [
            (t[2] * t[0]) if not isinstance(t[2], pd.Series) else t[2] * t[0]
            for t in triples
        ]
        return _combine_moments(counts, means, m2s)

    def var(self, ddof: int = 1, **kwargs):
        n, _mean, m2 = self._moments(**kwargs)
        denom = n - ddof
        if isinstance(denom, pd.Series):
            denom = denom.where(denom > 0, np.nan)
        elif denom <= 0:
            denom = np.nan
        return m2 / denom

    def std(self, ddof: int = 1, **kwargs):
        return self.var(ddof=ddof, **kwargs) ** 0.5

    def skew(self, **kwargs):
        from ._stats import skew

        return skew(self, **kwargs)

    def kurtosis(self, **kwargs):
        from ._stats import kurtosis

        return kurtosis(self, **kwargs)

    kurt = kurtosis

    def agg(self, func, *args, **kwargs):
        """Frame-level aggregation, dispatched to the distributed reductions."""
        if isinstance(func, str):
            return getattr(self, func)(*args, **kwargs)
        if isinstance(func, (list, tuple)):
            return pd.DataFrame(
                {name: getattr(self, name)() for name in func}
            ).T
        if isinstance(func, dict):
            return pd.Series(
                {
                    column: getattr(self[column], name)()
                    for column, name in func.items()
                }
            )
        raise TypeError(f"unsupported aggregation {func!r}")

    aggregate = agg

    def memory_usage(self, index: bool = True, deep: bool = False):
        """Bytes used per column, summed over every GPU."""
        parts = self._run_chunks(
            lambda c: _to_host(c.memory_usage(index=index, deep=deep))
        )
        return _stack(parts).sum()

    def ffill(self, **kwargs):
        from ._stats import fill_directional

        return fill_directional(self, forward=True, **kwargs)

    def bfill(self, **kwargs):
        from ._stats import fill_directional

        return fill_directional(self, forward=False, **kwargs)

    pad = ffill
    backfill = bfill

    def median(self, *args, **kwargs):
        return self.quantile(0.5, *args, **kwargs)

    def nunique(self, *args, **kwargs):
        """Exact distinct count via per-chunk unique + a small final pass."""
        return self._distinct_reduce("nunique", *args, **kwargs)


class _FallbackMixin:
    """Whole-API coverage: anything not distributed runs on one GPU.

    This keeps every cuDF method *available* while the distributed
    implementations are filled in.  It is a correctness fallback, not a
    scalability one: the frame must fit on the target device, so it warns.
    """

    #: set False to make unimplemented methods raise instead of gathering
    allow_single_gpu_fallback = True

    def _single_gpu_fallback(self, name: str, args: tuple, kwargs: dict):
        if not self.allow_single_gpu_fallback:
            raise NotImplementedError(
                f"{type(self).__name__}.{name} has no multi-GPU implementation "
                "and single-GPU fallback is disabled"
            )
        nbytes = self.nbytes
        warnings.warn(
            f"{type(self).__name__}.{name}() has no distributed implementation "
            f"yet; gathering {nbytes / (1 << 30):.2f} GiB onto GPU "
            f"{self._devices[0]} and running single-GPU. The result must fit "
            "in that device's memory.",
            UserWarning,
            stacklevel=3,
        )
        device = self._devices[0]
        gathered = self.compute(device)
        result = self._runtime.run(device, _call, gathered, name, args, kwargs)
        return _maybe_rechunk(result, device, self._runtime, self.devices)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        # Attribute-style column access (``df.price``). pandas code uses this
        # constantly, and under the accelerated-pandas backend the proxy
        # resolves it against this instance -- so without it, every such
        # access falls back and drags the frame to host memory.
        columns = getattr(self, "_column_names_for_attrs", None)
        if columns is not None and name in columns:
            return self[name]
        # ``__getattr__`` also fires when a *defined* property raises
        # AttributeError internally.  Surfacing that as "no such attribute"
        # hides real bugs, so distinguish the two cases explicitly.
        #
        # Probe the class dicts directly rather than with ``hasattr``: the
        # metaclass synthesizes a dispatcher for any cuDF name on demand, so
        # ``hasattr(type(self), name)`` is true for everything and would make
        # this branch swallow the normal fallback path.
        if any(name in vars(klass) for klass in type(self).__mro__):
            raise AttributeError(
                f"{type(self).__name__}.{name} exists but raised "
                "AttributeError while being evaluated; the original error was "
                "swallowed by attribute lookup. Re-run with "
                f"`type(obj).{name}.fget(obj)` to see it."
            )
        cudf_type = type(self)._cudf_type
        if not hasattr(cudf_type, name):
            raise AttributeError(
                f"{type(self).__name__!r} object has no attribute {name!r}"
            )
        attr = getattr(cudf_type, name)
        if not callable(attr) or isinstance(attr, property):
            return self._single_gpu_fallback_property(name)

        def _fallback(*args, **kwargs):
            return self._single_gpu_fallback(name, args, kwargs)

        _fallback.__name__ = name
        _fallback.__doc__ = getattr(attr, "__doc__", None)
        return _fallback

    def _single_gpu_fallback_property(self, name: str):
        device = self._devices[0]
        gathered = self.compute(device)
        return self._runtime.run(device, _getattr, gathered, name)


def _maybe_rechunk(result, device: int, runtime, devices: Sequence[int]):
    """Re-distribute a single-GPU result if it is a frame, else return as-is."""
    if isinstance(result, cudf.DataFrame):
        out = ChunkedDataFrame([result], [device], runtime)
    elif isinstance(result, cudf.Series):
        out = ChunkedSeries([result], [device], runtime)
    elif isinstance(result, _INDEX_TYPES):
        out = ChunkedIndex([result], [device], runtime)
    else:
        return runtime.run(device, _to_host, result)
    if len(devices) > 1:
        return out.rechunk(devices=list(dict.fromkeys(devices)))
    return out


def _make_map_method(name: str):
    def method(self, *args, **kwargs):
        return self._map_method(name, *args, **kwargs)

    method.__name__ = name
    return method


def _make_reduction(name: str, combiner: str):
    def method(self, *args, **kwargs):
        return self._reduce(name, combiner, *args, **kwargs)

    method.__name__ = name
    return method


class _ChunkedCommon(ChunkedFrame, _ReductionMixin, _FallbackMixin):
    """Behaviour shared by the DataFrame and Series wrappers."""

    def _map_method(self, name: str, *args, **kwargs):
        # Resolve deferred expressions here, on the calling thread. If one
        # reaches a chunk callback instead, cuDF will getattr() it, which
        # materializes the pending join and re-dispatches to every device --
        # from inside a device worker, which deadlocks.
        #
        # The frame has to be executed first: evaluating an expression against
        # a frame that is still pending just yields another expression, since
        # selecting a column from a pending frame is itself deferred.
        if self._plan is not None:
            self._materialize()
        args = _resolve_exprs(args, self)
        kwargs = _resolve_exprs(kwargs, self)
        others: list[ChunkedFrame] = []
        _find_chunked(args, others)
        _find_chunked(kwargs, others)
        for other in others:
            if not self._aligned_with(other):
                other_aligned = other.repartition_like(self)
                args = _swap(args, other, other_aligned)
                kwargs = _swap(kwargs, other, other_aligned)
        jobs = []
        for i, (chunk, device) in enumerate(zip(self._chunks, self._devices)):
            jobs.append(
                (
                    device,
                    _call,
                    (chunk, name, _substitute(args, i), _substitute(kwargs, i)),
                    {},
                )
            )
        results = self._runtime.run_many(jobs)
        return _wrap_like(results, self._devices, self._runtime)

    # -- schema ------------------------------------------------------
    @property
    def dtypes(self):
        return self._meta.dtypes

    @property
    def ndim(self) -> int:
        return self._meta.ndim

    @property
    def index(self):
        return ChunkedIndex(
            self._run_chunks(lambda c: c.index),
            self._devices,
            self._runtime,
            lengths=self._lengths_cache,
        )

    @property
    def values_host(self):
        return self.to_pandas().values

    # -- row slicing -------------------------------------------------
    def head(self, n: int = 5):
        """First ``n`` rows, collected onto a single GPU (still chunked)."""
        return self._edge(n, from_start=True)

    def tail(self, n: int = 5):
        """Last ``n`` rows, collected onto a single GPU (still chunked)."""
        return self._edge(n, from_start=False)

    def _edge(self, n: int, from_start: bool):
        lengths = self.chunk_lengths
        order = range(self.nchunks) if from_start else range(self.nchunks - 1, -1, -1)
        picks: list[tuple[Any, int]] = []
        remaining = n
        for i in order:
            if remaining <= 0:
                break
            take = min(lengths[i], remaining)
            if take == 0:
                continue
            method = "head" if from_start else "tail"
            piece = self._runtime.run(
                self._devices[i], _call, self._chunks[i], method, (take,), {}
            )
            picks.append((piece, self._devices[i]))
            remaining -= take
        if not from_start:
            picks.reverse()
        device = self._devices[0]
        if not picks:
            result = self._runtime.run(
                device, _call, self._chunks[0], "head", (0,), {}
            )
        else:
            result = _transfer.gather_concat(picks, device, self._runtime)
        return _wrap_like([result], [device], self._runtime)

    def _global_iloc(self, start: int, stop: int, step: int = 1):
        """Positional row slice -> a new multi-GPU frame."""
        if step != 1:
            raise NotImplementedError("iloc with a step is not supported yet")
        total = len(self)
        start = max(0, start + total if start < 0 else start)
        stop = min(total, stop + total if stop < 0 else stop)
        bounds = list(itertools.accumulate([0] + self.chunk_lengths))
        chunks, devices = [], []
        for i in range(self.nchunks):
            lo, hi = max(bounds[i], start), min(bounds[i + 1], stop)
            if lo >= hi:
                continue
            a, b = lo - bounds[i], hi - bounds[i]
            chunks.append(
                self._runtime.run(
                    self._devices[i],
                    lambda c, a=a, b=b: c.iloc[a:b],
                    self._chunks[i],
                )
            )
            devices.append(self._devices[i])
        if not chunks:
            empty = self._runtime.run(
                self._devices[0], _call, self._chunks[0], "head", (0,), {}
            )
            chunks, devices = [empty], [self._devices[0]]
        return _wrap_like(chunks, devices, self._runtime)

    # -- index handling ----------------------------------------------
    def reset_index(self, drop: bool = False, **kwargs):
        """Reset to a globally consistent RangeIndex."""
        offsets = list(itertools.accumulate([0] + self.chunk_lengths))
        jobs = []
        for i, (chunk, device) in enumerate(zip(self._chunks, self._devices)):
            jobs.append(
                (device, _reset_index_chunk, (chunk, offsets[i], drop, kwargs), {})
            )
        return _wrap_like(
            self._runtime.run_many(jobs), self._devices, self._runtime
        )

    # -- scans and boundary-crossing ops ------------------------------
    def cumsum(self, **kwargs):
        from ._scan import cumulative

        return cumulative(self, "cumsum", **kwargs)

    def cumprod(self, **kwargs):
        from ._scan import cumulative

        return cumulative(self, "cumprod", **kwargs)

    def cummax(self, **kwargs):
        from ._scan import cumulative

        return cumulative(self, "cummax", **kwargs)

    def cummin(self, **kwargs):
        from ._scan import cumulative

        return cumulative(self, "cummin", **kwargs)

    def shift(self, periods: int = 1, **kwargs):
        from ._scan import shift

        return shift(self, periods, **kwargs)

    def diff(self, periods: int = 1, **kwargs):
        from ._scan import diff

        return diff(self, periods, **kwargs)

    def pct_change(self, periods: int = 1, **kwargs):
        from ._scan import pct_change

        return pct_change(self, periods, **kwargs)

    # -- sampling and top-k -------------------------------------------
    def sample(self, n: int | None = None, frac: float | None = None, **kwargs):
        """Sample rows independently on each GPU.

        ``frac`` samples that fraction of every chunk, so the result stays
        distributed.  ``n`` splits the request proportionally across chunks.
        """
        if frac is None and n is None:
            frac = None
            n = 1
        if frac is not None:
            return self.map_chunks(lambda c: c.sample(frac=frac, **kwargs))
        lengths = self.chunk_lengths
        total = sum(lengths) or 1
        quotas = [min(length, round(n * length / total)) for length in lengths]
        # fix up rounding so the total is exactly n
        deficit = n - sum(quotas)
        for i in range(len(quotas)):
            if deficit == 0:
                break
            step = 1 if deficit > 0 else -1
            if 0 <= quotas[i] + step <= lengths[i]:
                quotas[i] += step
                deficit -= step
        jobs = [
            (device, _call, (chunk, "sample", (), {"n": quotas[i], **kwargs}), {})
            for i, (chunk, device) in enumerate(zip(self._chunks, self._devices))
        ]
        return _wrap_like(
            self._runtime.run_many(jobs), self._devices, self._runtime
        )

    def nlargest(self, n: int, columns=None, keep: str = "first"):
        """Top ``n`` rows: take the top ``n`` on each GPU, then merge."""
        return self._top_k(n, columns, keep, largest=True)

    def nsmallest(self, n: int, columns=None, keep: str = "first"):
        return self._top_k(n, columns, keep, largest=False)

    def _top_k(self, n: int, columns, keep: str, largest: bool):
        name = "nlargest" if largest else "nsmallest"
        args = (n,) if columns is None else (n, columns)
        candidates = self._run_chunks(
            lambda c: _call(c, name, args, {"keep": keep})
        )
        device = self._devices[0]
        pooled = _transfer.gather_concat(
            list(zip(candidates, self._devices)), device, self._runtime
        )
        result = self._runtime.run(device, _call, pooled, name, args, {"keep": keep})
        return _wrap_like([result], [device], self._runtime)

    def mode(self, **kwargs):
        from ._stats import mode

        return mode(self, **kwargs)

    # -- whole-frame reshaping ----------------------------------------
    def duplicated(self, subset=None, keep="first"):
        from ._reshape import duplicated

        return duplicated(self, subset=subset, keep=keep)

    def interpolate(self, method: str = "linear", **kwargs):
        from ._reshape import interpolate

        return interpolate(self, method=method, **kwargs)

    def convert_dtypes(self, **kwargs):
        from ._reshape import convert_dtypes

        return convert_dtypes(self, **kwargs)

    # -- misc --------------------------------------------------------
    @property
    def empty(self) -> bool:
        return len(self) == 0

    def equals(self, other) -> bool:
        if isinstance(other, ChunkedFrame):
            if len(self) != len(other):
                return False
            other = other.to_pandas()
        return bool(self.to_pandas().equals(other))

    def persist(self):
        return self

    def __array__(self, *args, **kwargs):
        return self.to_pandas().__array__(*args, **kwargs)

    def _repr_summary(self) -> str:
        placement: dict[int, int] = {}
        for device in self._devices:
            placement[device] = placement.get(device, 0) + 1
        gib = self.nbytes / (1 << 30)
        where = ", ".join(
            f"GPU{d}x{n}" if n > 1 else f"GPU{d}" for d, n in sorted(placement.items())
        )
        return (
            f"[{self.nchunks} chunks across {len(placement)} GPUs "
            f"({where}), {gib:.3f} GiB device memory]"
        )


def _swap(container, old, new):
    if container is old:
        return new
    if isinstance(container, tuple):
        return tuple(_swap(v, old, new) for v in container)
    if isinstance(container, list):
        return [_swap(v, old, new) for v in container]
    if isinstance(container, dict):
        return {k: _swap(v, old, new) for k, v in container.items()}
    return container


def _reset_index_chunk(chunk, offset: int, drop: bool, kwargs: dict):
    out = chunk.reset_index(drop=drop, **kwargs)
    out.index = cudf.RangeIndex(offset, offset + len(out))
    return out


def _columns_by_position(frame, cols) -> list:
    """Column *labels* for an ``iloc`` column selector.

    ``iloc`` indexes columns by position, so the selector has to be resolved
    against the column order before it can be handed to label-based selection.
    Passing it through unresolved makes ``df.iloc[:, 0]`` mean ``df[0]``, which
    is a lookup for a column literally named ``0``.
    """
    names = list(frame.columns)
    if isinstance(cols, slice):
        return names[cols]
    if isinstance(cols, int):
        return [names[cols]]
    seq = list(cols)
    if seq and all(isinstance(v, (bool, np.bool_)) for v in seq):
        if len(seq) != len(names):
            raise IndexError(
                f"boolean column mask has {len(seq)} entries, "
                f"frame has {len(names)} columns"
            )
        return [n for n, keep in zip(names, seq) if keep]
    return [names[int(v)] for v in seq]


class _ILocIndexer:
    def __init__(self, frame):
        self._frame = frame

    def __getitem__(self, key):
        frame = self._frame
        if isinstance(key, tuple):
            rows, cols = key
        else:
            rows, cols = key, None
        if isinstance(rows, slice):
            start, stop, step = rows.indices(len(frame))
            out = frame._global_iloc(start, stop, step)
        elif isinstance(rows, int):
            return frame._global_iloc(rows, rows + 1).compute().iloc[0]
        else:
            raise NotImplementedError(
                "iloc row selection supports slices and integers"
            )
        if cols is not None:
            out = out[_columns_by_position(out, cols)] if not isinstance(
                cols, int) else out[list(out.columns)[cols]]
        return out


class _LocIndexer:
    """Label-based selection.

    Row selection is limited to a full slice or a boolean mask: anything else
    needs a global index lookup, which a row-partitioned frame cannot answer
    without a shuffle.  Column selection is unrestricted.
    """

    def __init__(self, frame):
        self._frame = frame

    def __getitem__(self, key):
        frame = self._frame
        rows, columns = (key if isinstance(key, tuple) else (key, None))

        if isinstance(rows, slice):
            if rows.start is not None or rows.stop is not None:
                raise NotImplementedError(
                    "loc row slicing by label is not supported on a "
                    "row-partitioned frame; use .iloc with positions"
                )
            result = frame
        elif isinstance(rows, ChunkedSeries):
            result = frame._map_method("__getitem__", rows)
        else:
            raise NotImplementedError(
                "loc row selection supports ':' or a boolean mask; got "
                f"{type(rows).__name__}"
            )

        if columns is None:
            return result
        return result._map_method("__getitem__", columns)

    def __setitem__(self, key, value):
        rows, columns = (key if isinstance(key, tuple) else (key, None))
        if not (isinstance(rows, slice) and rows.start is None and rows.stop is None):
            raise NotImplementedError("loc assignment supports ':' rows only")
        self._frame[columns] = value


class ChunkedDataFrame(_ChunkedCommon):
    """A :class:`cudf.DataFrame` partitioned by rows across several GPUs."""

    #: pandas identifies its own types structurally rather than by isinstance:
    #: ``ABCSeries.__instancecheck__`` reads ``inst._typ`` and compares it to a
    #: string. Without it, ``pd.to_datetime`` and friends raise AttributeError
    #: on a chunked frame, which cudf.pandas turns into a silent CPU fallback --
    #: so the query still answers correctly, just not on the GPU.
    _typ = "dataframe"


    _cudf_type = cudf.DataFrame

    @property
    def loc(self):
        return _LocIndexer(self)

    # -- schema ------------------------------------------------------
    @property
    def columns(self):
        return self._meta.columns

    @property
    def _column_names_for_attrs(self):
        """Column names, for attribute-style access. Never raises."""
        try:
            return self._meta.columns
        except Exception:
            return ()

    @columns.setter
    def columns(self, value) -> None:
        names = list(value)
        self._run_chunks(_set_columns, _Const(names))
        self._meta_cache = None

    @property
    def shape(self) -> tuple[int, int]:
        return (len(self), len(self._meta.columns))

    @property
    def iloc(self):
        return _ILocIndexer(self)

    # -- selection ---------------------------------------------------
    def __getitem__(self, key):
        from ._lazy import Expr, _MISS

        if self._plan is not None:
            answer = self._plan.getitem(self, key)
            if answer is not _MISS:
                return answer
        if isinstance(key, Expr):
            # a portable expression applied to an already-executed frame
            return self._map_method("__getitem__", key.evaluate(self))
        if isinstance(key, ChunkedSeries):
            return self._map_method("__getitem__", key)
        if isinstance(key, slice):
            start, stop, step = key.indices(len(self))
            return self._global_iloc(start, stop, step)
        return self._map_method("__getitem__", key)

    def __setitem__(self, key, value):
        from ._lazy import Expr

        if self._plan is not None:
            self._materialize()
        if isinstance(value, Expr):
            # evaluate against *this* frame so the partitioning lines up
            value = value.evaluate(self)
        if isinstance(value, ChunkedFrame) and not self._aligned_with(value):
            value = value.repartition_like(self)
        self._run_chunks(_setitem, value if isinstance(value, ChunkedFrame) else _Const(value), key)
        self._meta_cache = None

    def __delitem__(self, key):
        self._run_chunks(lambda c: c.__delitem__(key))
        self._meta_cache = None

    def __contains__(self, key) -> bool:
        return key in self._meta.columns

    def __iter__(self):
        return iter(self._meta.columns)

    # -- reductions over columns -------------------------------------
    def nunique(self, *args, **kwargs):
        return pd.Series(
            {col: self[col].nunique(*args, **kwargs) for col in self.columns}
        )

    def corr(self, method: str = "pearson", **kwargs):
        from ._stats import corr

        return corr(self, method=method, **kwargs)

    def cov(self, **kwargs):
        from ._stats import cov

        return cov(self, **kwargs)

    def apply(self, func, axis: int = 1, **kwargs):
        """Row-wise UDF, compiled and run independently on each GPU."""
        if axis != 1:
            raise NotImplementedError(
                "multi-GPU apply supports axis=1 (row-wise) only"
            )
        return self.map_chunks(lambda c: c.apply(func, axis=axis, **kwargs))

    def describe(self, *args, **kwargs):
        numeric = self._meta.select_dtypes("number").columns
        rows = {}
        for col in numeric:
            series = self[col]
            rows[col] = pd.Series(
                {
                    "count": series.count(),
                    "mean": series.mean(),
                    "std": series.std(),
                    "min": series.min(),
                    "max": series.max(),
                }
            )
        return pd.DataFrame(rows)

    # -- shuffle-backed ops ------------------------------------------
    def groupby(self, by, **kwargs):
        from ._ops import ChunkedGroupBy

        return ChunkedGroupBy(self, by, **kwargs)

    def merge(self, right, **kwargs):
        from ._ops import merge

        return merge(self, right, **kwargs)

    join = merge

    def sort_values(self, by, **kwargs):
        from ._ops import sort_values

        return sort_values(self, by, **kwargs)

    def drop_duplicates(self, subset=None, **kwargs):
        from ._ops import drop_duplicates

        return drop_duplicates(self, subset=subset, **kwargs)

    def set_index(self, keys, **kwargs):
        from ._ops import set_index

        return set_index(self, keys, **kwargs)

    def melt(self, id_vars=None, value_vars=None, **kwargs):
        from ._reshape import melt

        return melt(self, id_vars=id_vars, value_vars=value_vars, **kwargs)

    def value_counts(self, subset=None, **kwargs):
        cols = list(self.columns) if subset is None else list(subset)
        return self.groupby(cols).size().sort_values(ascending=False)

    # -- IO ----------------------------------------------------------
    def to_parquet(self, path: str, **kwargs):
        from ._io import to_parquet

        return to_parquet(self, path, **kwargs)

    # -- display -----------------------------------------------------
    def __repr__(self) -> str:
        total = len(self)
        if total <= 10:
            body = repr(self.to_pandas())
        else:
            head = self.head(5).to_pandas()
            tail = self.tail(5).to_pandas()
            body = repr(pd.concat([head, tail]))
            body += f"\n\n[{total} rows x {len(self.columns)} columns]"
        return _banner("ChunkedDataFrame", self) + body


class _Const:
    """Marks a non-chunked value inside ``_run_chunks``."""

    def __init__(self, value):
        self.value = value


def _set_columns(chunk, names):
    chunk.columns = names.value if isinstance(names, _Const) else names
    return chunk


def _setitem(chunk, value, key):
    chunk[key] = value.value if isinstance(value, _Const) else value
    return chunk


class ChunkedSeries(_ChunkedCommon):
    """A :class:`cudf.Series` partitioned by rows across several GPUs."""

    #: pandas identifies its own types structurally rather than by isinstance:
    #: ``ABCSeries.__instancecheck__`` reads ``inst._typ`` and compares it to a
    #: string. Without it, ``pd.to_datetime`` and friends raise AttributeError
    #: on a chunked frame, which cudf.pandas turns into a silent CPU fallback --
    #: so the query still answers correctly, just not on the GPU.
    _typ = "series"


    _cudf_type = cudf.Series

    @property
    def name(self):
        return self._meta.name

    @property
    def dtype(self):
        return self._meta.dtype

    @property
    def shape(self) -> tuple[int]:
        return (len(self),)

    @property
    def iloc(self):
        return _ILocIndexer(self)

    def __getitem__(self, key):
        if isinstance(key, ChunkedSeries):
            return self._map_method("__getitem__", key)
        if isinstance(key, slice):
            start, stop, step = key.indices(len(self))
            return self._global_iloc(start, stop, step)
        return self._map_method("__getitem__", key)

    # -- distinct ----------------------------------------------------
    def unique(self):
        """Exact distinct values, gathered onto one GPU."""
        parts = self._run_chunks(lambda c: c.unique())
        device = self._devices[0]
        gathered = _transfer.gather_concat(
            list(zip(parts, self._devices)), device, self._runtime, ignore_index=True
        )
        result = self._runtime.run(device, lambda s: s.unique(), gathered)
        return _wrap_like([result], [device], self._runtime)

    def _distinct_reduce(self, name: str, *args, **kwargs):
        return int(len(self.unique()))

    def nunique(self, *args, **kwargs) -> int:
        return self._distinct_reduce("nunique", *args, **kwargs)

    def factorize(self, sort: bool = False, **kwargs):
        from ._reshape import factorize

        return factorize(self, sort=sort, **kwargs)

    def apply(self, func, *args, **kwargs):
        """Elementwise UDF, compiled and run independently on each GPU.

        String UDFs work too: cuDF's compiled-kernel cache is device-aware
        (``cudf/utils/device.py``), so each GPU keeps its own PTX with its own
        character-table pointers.
        """
        return self.map_chunks(lambda c: c.apply(func, *args, **kwargs))

    def value_counts(self, **kwargs):
        from ._ops import series_value_counts

        return series_value_counts(self, **kwargs)

    def drop_duplicates(self, **kwargs):
        from ._ops import drop_duplicates

        return drop_duplicates(self, **kwargs)

    def sort_values(self, by=None, **kwargs):
        from ._ops import sort_values

        # `by` is accepted (and ignored when it names this series) so that code
        # written against a DataFrame keeps working if a step yields a Series.
        if by is not None and by not in (self.name, [self.name]):
            raise KeyError(f"cannot sort a Series by {by!r}")
        return sort_values(self, None, **kwargs)

    def groupby(self, by, **kwargs):
        from ._ops import ChunkedGroupBy

        return ChunkedGroupBy(self, by, **kwargs)

    def quantile(self, q=0.5, interpolation: str = "linear", **kwargs):
        from ._ops import quantile

        return quantile(self, q, interpolation=interpolation)

    def idxmax(self, **kwargs):
        return self._extreme_index("max", **kwargs)

    def idxmin(self, **kwargs):
        return self._extreme_index("min", **kwargs)

    def _extreme_index(self, how: str, **kwargs):
        """Index label of the first minimum/maximum.

        cuDF has no ``idxmin``/``idxmax``, so each chunk reports its extreme
        value and the label of its first occurrence; the winner is then chosen
        on the host.  Ties resolve to the earliest chunk, matching pandas.
        """

        def per_chunk(chunk):
            if len(chunk) == 0:
                return None
            value = getattr(chunk, how)(**kwargs)
            if value is None or (isinstance(value, float) and np.isnan(value)):
                return None
            matches = chunk.index[chunk == value]
            if len(matches) == 0:
                return None
            return (_to_host(value), _to_host(matches[:1])[0])

        parts = [
            (i, p)
            for i, p in enumerate(self._run_chunks(per_chunk))
            if p is not None
        ]
        if not parts:
            return None
        better = (lambda a, b: a > b) if how == "max" else (lambda a, b: a < b)
        best_i, best = parts[0]
        for i, part in parts[1:]:
            if better(part[0], best[0]):
                best_i, best = i, part
        return best[1]

    # -- accessors ---------------------------------------------------
    @property
    def str(self):
        return _Accessor(self, "str")

    @property
    def dt(self):
        return _Accessor(self, "dt")

    @property
    def cat(self):
        return _Accessor(self, "cat")

    @property
    def list(self):
        return _Accessor(self, "list")

    @property
    def struct(self):
        return _Accessor(self, "struct")

    def __repr__(self) -> str:
        total = len(self)
        if total <= 10:
            body = repr(self.to_pandas())
        else:
            head = self.head(5).to_pandas()
            tail = self.tail(5).to_pandas()
            body = repr(pd.concat([head, tail]))
            body += f"\n\n[{total} rows]"
        return _banner("ChunkedSeries", self) + body


class _Accessor:
    """Forwards ``.str`` / ``.dt`` / ``.cat`` / ``.list`` methods per chunk.

    Resolves eagerly. An earlier version returned a lazy object that only did
    the work when something touched it, which deadlocked: cuDF's ``as_column``
    calls ``getattr`` on assigned values, so ``df["y"] = df["x"].dt.year``
    resolved the accessor *inside* a device worker thread, which then tried to
    dispatch back to that same single-threaded worker.
    """

    def __init__(self, series: "ChunkedSeries", namespace: str) -> None:
        self._series = series
        self._namespace = namespace

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        series = self._series
        namespace = self._namespace

        # Ask one chunk whether this name is a method or a value. Metadata
        # only -- it never touches the data.
        is_method = series._runtime.run(
            series._devices[0],
            lambda chunk: callable(getattr(getattr(chunk, namespace), name)),
            series._chunks[0],
        )

        if not is_method:
            return _wrap_like(
                series._run_chunks(
                    lambda c: getattr(getattr(c, namespace), name)
                ),
                series._devices,
                series._runtime,
            )

        def method(*args, **kwargs):
            return _wrap_like(
                series._run_chunks(
                    lambda c: getattr(getattr(c, namespace), name)(
                        *args, **kwargs
                    )
                ),
                series._devices,
                series._runtime,
            )

        method.__name__ = name
        return method


class ChunkedIndex(_ChunkedCommon):
    """A :class:`cudf.Index` partitioned across several GPUs."""

    #: pandas identifies its own types structurally rather than by isinstance:
    #: ``ABCSeries.__instancecheck__`` reads ``inst._typ`` and compares it to a
    #: string. Without it, ``pd.to_datetime`` and friends raise AttributeError
    #: on a chunked frame, which cudf.pandas turns into a silent CPU fallback --
    #: so the query still answers correctly, just not on the GPU.
    _typ = "index"


    _cudf_type = cudf.Index

    @property
    def name(self):
        return self._meta.name

    @property
    def dtype(self):
        return self._meta.dtype

    def __repr__(self) -> str:
        return _banner("ChunkedIndex", self) + repr(self.head(5).to_pandas())


# ----------------------------------------------------------------------
# install generated methods
# ----------------------------------------------------------------------
for _name in _MAP_METHODS_COMMON:
    for _cls in (ChunkedDataFrame, ChunkedSeries):
        if hasattr(_cls._cudf_type, _name) and _name not in vars(_cls):
            setattr(_cls, _name, _make_map_method(_name))

for _name in _MAP_METHODS_DATAFRAME:
    if hasattr(cudf.DataFrame, _name) and _name not in vars(ChunkedDataFrame):
        setattr(ChunkedDataFrame, _name, _make_map_method(_name))

for _name in _MAP_METHODS_SERIES:
    if hasattr(cudf.Series, _name) and _name not in vars(ChunkedSeries):
        setattr(ChunkedSeries, _name, _make_map_method(_name))

# dropna/isna style row-wise filters keep partitioning but change lengths
for _name in ("dropna", "drop_na", "nans_to_nulls"):
    for _cls in (ChunkedDataFrame, ChunkedSeries):
        if hasattr(_cls._cudf_type, _name):
            setattr(_cls, _name, _make_map_method(_name))

for _name, _combiner in _SIMPLE_REDUCTIONS.items():
    for _cls in (ChunkedDataFrame, ChunkedSeries):
        if hasattr(_cls._cudf_type, _name):
            setattr(_cls, _name, _make_reduction(_name, _combiner))

# arithmetic / comparison dunders
_BINOPS = (
    "add", "sub", "mul", "truediv", "floordiv", "mod", "pow", "and", "or",
    "xor", "lt", "le", "gt", "ge", "eq", "ne",
)
for _op in _BINOPS:
    for _prefix in ("__{}__", "__r{}__"):
        _dunder = _prefix.format(_op)
        for _cls in (ChunkedDataFrame, ChunkedSeries, ChunkedIndex):
            if hasattr(_cls._cudf_type, _dunder):
                setattr(_cls, _dunder, _make_map_method(_dunder))

for _dunder in ("__neg__", "__abs__", "__invert__", "__pos__"):
    for _cls in (ChunkedDataFrame, ChunkedSeries):
        if hasattr(_cls._cudf_type, _dunder):
            setattr(_cls, _dunder, _make_map_method(_dunder))

del _name, _cls, _op, _prefix, _dunder, _combiner
