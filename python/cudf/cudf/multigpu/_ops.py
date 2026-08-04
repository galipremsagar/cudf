# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Distributed implementations of the key-based cuDF operations.

Every operation here follows the same shape: get the rows that need to meet
onto the same GPU, then let ordinary single-GPU cuDF do the actual work.

* group-by  -- pre-aggregate locally, shuffle the (much smaller) partials by
  key, aggregate again.  Falls back to shuffling raw rows for aggregations
  that are not decomposable.
* join      -- co-partition both sides on the join keys, then join locally.
  Broadcasts instead when one side is small enough to replicate.
* sort      -- sample to find range splitters, range-partition, sort locally.
  Chunks then appear in globally sorted order.
* distinct  -- shuffle by the subset, then drop duplicates locally.
"""

from __future__ import annotations

import itertools
import os
import sys
from typing import Any, Hashable, Sequence

import numpy as np
import pandas as pd

import cudf

from . import _transfer
from ._frame import (
    ChunkedDataFrame,
    ChunkedFrame,
    ChunkedSeries,
    _concat_parts,
    _wrap_like,
    drop_empty_chunks,
    unwrap_proxy,
)
from ._shuffle import (
    assign_targets,
    default_nparts,
    hash_shuffle,
    map_shuffle,
)

__all__ = [
    "ChunkedGroupBy",
    "merge",
    "sort_values",
    "drop_duplicates",
    "set_index",
    "quantile",
    "series_value_counts",
]

#: bytes below which the right side of a join is always replicated
BROADCAST_THRESHOLD = 1 << 30

#: Replicating the right side costs ``right * ndevices``; shuffling instead
#: costs roughly a copy of *both* sides plus the serialization and destination
#: buffers on top. So broadcast whenever replication is the cheaper of the two,
#: not just when the right side is small in absolute terms -- otherwise a
#: mid-sized dimension table drags a huge fact table through a full shuffle.
BROADCAST_COST_RATIO = 1.0

#: set CUDF_MULTIGPU_DEBUG=1 to print each join's sizes and chosen strategy
DEBUG = bool(os.environ.get("CUDF_MULTIGPU_DEBUG"))

#: aggregation -> (local partial aggregation, combine of the partials)
_DECOMPOSABLE = {
    "sum": ("sum", "sum"),
    "min": ("min", "min"),
    "max": ("max", "max"),
    "count": ("count", "sum"),
    "size": ("size", "sum"),
    "prod": ("prod", "prod"),
    "product": ("prod", "prod"),
    "any": ("any", "any"),
    "all": ("all", "all"),
}


# ----------------------------------------------------------------------
# group-by
# ----------------------------------------------------------------------
def _normalize_agg_spec(spec, columns: Sequence[Hashable], keys: Sequence[Hashable]):
    """Return ``[(out_name, in_col, agg), ...]`` plus a MultiIndex flag."""
    value_cols = [c for c in columns if c not in keys]
    plan: list[tuple[Any, Any, str]] = []
    multi = False
    if isinstance(spec, str):
        plan = [(c, c, spec) for c in value_cols]
    elif isinstance(spec, (list, tuple)):
        multi = True
        plan = [((c, a), c, a) for c in value_cols for a in spec]
    elif isinstance(spec, dict):
        for col, aggs in spec.items():
            if isinstance(aggs, str):
                plan.append((col, col, aggs))
            else:
                multi = True
                plan.extend(((col, a), col, a) for a in aggs)
    else:
        raise TypeError(f"unsupported aggregation spec: {spec!r}")
    return plan, multi


def _named_aggregations(kwargs: dict) -> list[tuple]:
    """Parse pandas' named-aggregation kwargs into ``[(out, column, agg)]``.

    Accepts both ``pd.NamedAgg(column=..., aggfunc=...)`` and the bare
    ``(column, aggfunc)`` tuple form.
    """
    plan: list[tuple] = []
    for name, value in kwargs.items():
        column = getattr(value, "column", None)
        aggfunc = getattr(value, "aggfunc", None)
        if column is None and isinstance(value, tuple) and len(value) == 2:
            column, aggfunc = value
        if column is None or aggfunc is None:
            continue
        plan.append((name, column, aggfunc))
    return plan


def _local_named_agg(chunk, keys, plan, as_index, sort, dropna):
    grouped = chunk.groupby(keys, as_index=False, sort=sort, dropna=dropna)
    spec: dict = {}
    for _out, column, agg in plan:
        spec.setdefault(column, [])
        if agg not in spec[column]:
            spec[column].append(agg)
    aggregated = grouped.agg(spec)
    aggregated.columns = [
        c if not isinstance(c, tuple) else (c[0] if c[1] == "" else f"{c[0]}__{c[1]}")
        for c in aggregated.columns.to_flat_index()
    ]
    out = aggregated[list(keys)].copy()
    for name, column, agg in plan:
        out[name] = aggregated[f"{column}__{agg}"]
    return out.set_index(list(keys)) if as_index else out


def _partial_name(col: Hashable, agg: str) -> str:
    return f"__mg_{col}__{agg}"


class ChunkedGroupBy:
    """Multi-GPU group-by.  Produced by ``ChunkedDataFrame.groupby``."""

    def __init__(
        self,
        frame: ChunkedFrame,
        by,
        as_index: bool = True,
        sort: bool = False,
        dropna: bool = True,
        split_out: int | None = None,
        **kwargs,
    ) -> None:
        self._frame = frame
        self._by = [by] if isinstance(by, (str, int)) or not isinstance(by, (list, tuple)) else list(by)
        self._as_index = as_index
        self._sort = sort
        self._dropna = dropna
        self._split_out = split_out
        self._kwargs = kwargs
        #: set by __getitem__ when a single column was selected, in which case
        #: aggregations return a Series rather than a one-column DataFrame
        self._single_column = None

    # -- helpers -----------------------------------------------------
    @property
    def _keys(self) -> list:
        return list(self._by)

    def _frame_as_dataframe(self) -> ChunkedDataFrame:
        frame = self._frame
        if isinstance(frame, ChunkedSeries):
            return frame.to_frame()
        return frame

    def _nparts(self) -> int:
        if self._split_out is not None:
            return self._split_out
        return default_nparts(self._frame, list(self._frame.runtime.devices))

    # -- entry points ------------------------------------------------
    def agg(self, spec=None, **kwargs):
        frame = self._frame_as_dataframe()
        keys = self._keys
        named = _named_aggregations(kwargs)
        if spec is None and named:
            plan, multi = named, False
            kwargs = {k: v for k, v in kwargs.items() if k not in dict(
                (name, None) for name, _c, _a in named
            )}
        elif spec is None:
            raise TypeError(
                "agg() needs either an aggregation spec or named aggregations"
            )
        else:
            plan, multi = _normalize_agg_spec(spec, list(frame.columns), keys)
        aggs = {a for _o, _c, a in plan}
        if aggs and aggs <= (set(_DECOMPOSABLE) | {"mean"}):
            result = self._tree_agg(frame, plan, multi)
        elif spec is not None:
            result = self._shuffle_agg(spec, **kwargs)
        else:
            result = self._shuffle_agg_plan(plan)
        return self._maybe_squeeze(result, spec)

    def _shuffle_agg_plan(self, plan):
        """Shuffle-then-aggregate for a named-aggregation plan."""
        frame = self._frame_as_dataframe()
        shuffled = hash_shuffle(frame, self._keys, nparts=self._nparts())
        return shuffled.map_chunks(
            _local_named_agg,
            keys=self._keys,
            plan=plan,
            as_index=self._as_index,
            sort=self._sort,
            dropna=self._dropna,
        )

    def _maybe_squeeze(self, result, spec):
        """``gb["v"].sum()`` is a Series -- but only when ``as_index=True``.

        With ``as_index=False`` pandas keeps the grouping key as a column and
        returns a DataFrame, which callers then merge on.
        """
        column = self._single_column
        if column is None or not self._as_index:
            return result
        if isinstance(spec, (list, tuple)):
            return result
        if isinstance(spec, dict) and not isinstance(spec.get(column), str):
            return result
        if isinstance(result, ChunkedDataFrame) and column in result.columns:
            return result[column]
        return result

    aggregate = agg

    def _tree_agg(self, frame: ChunkedDataFrame, plan, multi: bool):
        """Pre-aggregate on each GPU, shuffle partials, aggregate again.

        The shuffle only carries one row per (key, chunk) instead of every
        input row, which is the whole point when the group count is far
        smaller than the row count.
        """
        keys = self._keys
        # 1. which raw partials each output needs
        needed: dict[tuple, str] = {}
        for _out, col, agg in plan:
            for base in (("sum", "count") if agg == "mean" else (agg,)):
                needed[(col, base)] = _DECOMPOSABLE[base][0]

        local_spec: dict[Any, list[str]] = {}
        for (col, base) in needed:
            local_spec.setdefault(col, []).append(_DECOMPOSABLE[base][0])
        for col in local_spec:
            local_spec[col] = sorted(set(local_spec[col]))

        partials = frame.map_chunks(
            _local_partial_agg, keys=keys, spec=local_spec, dropna=self._dropna
        )

        # 2. move partials so each key lands on exactly one GPU
        shuffled = hash_shuffle(partials, keys, nparts=self._nparts())

        # 3. combine and derive
        return shuffled.map_chunks(
            _combine_partial_agg,
            keys=keys,
            plan=plan,
            multi=multi,
            as_index=self._as_index,
            sort=self._sort,
            dropna=self._dropna,
        )

    def _shuffle_agg(self, spec, **kwargs):
        """Move whole rows so every key is complete on one GPU, then aggregate."""
        frame = self._frame_as_dataframe()
        shuffled = hash_shuffle(frame, self._keys, nparts=self._nparts())
        return shuffled.map_chunks(
            _local_full_agg,
            keys=self._keys,
            spec=spec,
            as_index=self._as_index,
            sort=self._sort,
            dropna=self._dropna,
            kwargs=kwargs,
        )

    def size(self):
        frame = self._frame_as_dataframe()
        counts = frame.map_chunks(_local_size, keys=self._keys, dropna=self._dropna)
        shuffled = hash_shuffle(counts, self._keys, nparts=self._nparts())
        return shuffled.map_chunks(
            _combine_size, keys=self._keys, as_index=self._as_index, sort=self._sort
        )

    def count(self):
        return self.agg("count")

    def __getitem__(self, key):
        sub = self._frame_as_dataframe()
        cols = self._keys + ([key] if isinstance(key, (str, int)) else list(key))
        seen: list = []
        for c in cols:
            if c not in seen:
                seen.append(c)
        new = ChunkedGroupBy(
            sub[seen],
            self._by,
            as_index=self._as_index,
            sort=self._sort,
            dropna=self._dropna,
            split_out=self._split_out,
        )
        new._single_column = key if isinstance(key, (str, int)) else None
        return new

    def _agg_columns(self):
        frame = self._frame_as_dataframe()
        return [c for c in frame.columns if c not in self._keys]

    def apply(self, func, *args, **kwargs):
        """Run a per-group UDF after moving each group onto a single GPU."""
        frame = self._frame_as_dataframe()
        shuffled = hash_shuffle(frame, self._keys, nparts=self._nparts())
        applied = shuffled.map_chunks(
            _local_apply,
            keys=self._keys,
            func=func,
            args=args,
            kwargs=kwargs,
            dropna=self._dropna,
            as_index=self._as_index,
            sort=self._sort,
        )
        return drop_empty_chunks(applied)

    def __repr__(self) -> str:
        return f"<ChunkedGroupBy by={self._by} over {self._frame.nchunks} chunks>"


for _agg in ("sum", "min", "max", "mean", "prod", "any", "all", "std", "var",
             "median", "nunique", "first", "last"):

    def _make(agg=_agg):
        def method(self, *args, **kwargs):
            if self._single_column is not None:
                return self.agg({self._single_column: agg})
            return self.agg(agg)

        method.__name__ = agg
        return method

    setattr(ChunkedGroupBy, _agg, _make())
del _agg


# -- per-chunk group-by helpers (run on the chunk's device) ----------
def _local_partial_agg(chunk, keys, spec, dropna):
    # Empty chunks are aggregated too rather than special-cased: cuDF returns
    # the correct *schema* for an empty group-by, and every chunk of a
    # partitioned frame must agree on schema or the final concat silently
    # unions columns and fills them with nulls.
    grouped = chunk.groupby(keys, as_index=False, dropna=dropna, sort=False)
    out = grouped.agg(spec)
    out.columns = [
        c if not isinstance(c, tuple) else (c[0] if c[1] == "" else _partial_name(*c))
        for c in out.columns.to_flat_index()
    ]
    return out


def _combine_partial_agg(chunk, keys, plan, multi, as_index, sort, dropna):
    combine_spec: dict[str, str] = {}
    for _out, col, agg in plan:
        for base in (("sum", "count") if agg == "mean" else (agg,)):
            name = _partial_name(col, _DECOMPOSABLE[base][0])
            combine_spec[name] = _DECOMPOSABLE[base][1]
    combined = chunk.groupby(keys, as_index=False, dropna=dropna, sort=sort).agg(
        combine_spec
    )

    out = combined[keys].copy()
    for out_name, col, agg in plan:
        if agg == "mean":
            total = combined[_partial_name(col, "sum")]
            n = combined[_partial_name(col, "count")]
            out[out_name] = total / n
        else:
            out[out_name] = combined[_partial_name(col, _DECOMPOSABLE[agg][0])]
    if multi:
        out.columns = cudf.MultiIndex.from_tuples(
            [(c, "") if not isinstance(c, tuple) else c for c in out.columns]
        )
    if as_index:
        out = out.set_index(keys)
    return out


def _local_full_agg(chunk, keys, spec, as_index, sort, dropna, kwargs):
    return chunk.groupby(
        keys, as_index=as_index, sort=sort, dropna=dropna, **kwargs
    ).agg(spec)


def _local_size(chunk, keys, dropna):
    out = chunk.groupby(keys, as_index=False, dropna=dropna, sort=False).size()
    out = out.rename(columns={"size": "__mg_size", 0: "__mg_size"})
    return out


def _combine_size(chunk, keys, as_index, sort):
    out = chunk.groupby(keys, as_index=False, sort=sort).agg({"__mg_size": "sum"})
    result = out.set_index(keys)["__mg_size"] if as_index else out
    if as_index:
        result.name = None
        return result
    return result.rename(columns={"__mg_size": "size"})


def _local_apply(chunk, keys, func, args, kwargs, dropna, as_index, sort):
    # as_index must be threaded through: with as_index=False pandas returns the
    # group key as a column and the result as a DataFrame, and callers go on to
    # assign .columns or sort by the key.
    return chunk.groupby(
        keys, dropna=dropna, as_index=as_index, sort=sort
    ).apply(func, *args, **kwargs)


# ----------------------------------------------------------------------
# join
# ----------------------------------------------------------------------
#: Plan joins instead of running them, so that a filter written after a join
#: can be pushed underneath it (see _lazy.py). Set False to force the old
#: execute-immediately behaviour.
LAZY_JOINS = True


def normalize_merge_kwargs(kwargs: dict) -> dict:
    """Canonical merge arguments, so a plan can be compared and replayed."""
    plan = {
        "on": kwargs.pop("on", None),
        "left_on": kwargs.pop("left_on", None),
        "right_on": kwargs.pop("right_on", None),
        "how": kwargs.pop("how", "inner"),
        "broadcast": kwargs.pop("broadcast", None),
        "nparts": kwargs.pop("nparts", None),
    }
    plan.update(kwargs)
    return plan


def merge(left: ChunkedDataFrame, right, **kwargs):
    """Plan a join. Nothing is computed until the result is used.

    Deferring is what makes predicate pushdown possible: a filter applied to
    the joined frame can be rewritten into a filter on one of the inputs, so
    the join runs on less data.
    """
    left = unwrap_proxy(left)
    right = unwrap_proxy(right)
    plan = normalize_merge_kwargs(dict(kwargs))
    if not LAZY_JOINS or not isinstance(right, ChunkedFrame):
        return execute_merge(left, right, plan)

    from ._lazy import JoinPlan

    return ChunkedDataFrame(plan=JoinPlan(left, right, plan))


def execute_merge(left, right, plan: dict) -> ChunkedDataFrame:
    """Actually run a planned join.

    Small right-hand sides are replicated to every GPU, which keeps ``left``'s
    partitioning intact.  Otherwise both sides are hash-partitioned on the join
    keys so that matching rows meet on the same GPU.
    """
    plan = dict(plan)
    on = plan.pop("on", None)
    left_on = plan.pop("left_on", None)
    right_on = plan.pop("right_on", None)
    how = plan.pop("how", "inner")
    broadcast = plan.pop("broadcast", None)
    nparts = plan.pop("nparts", None)
    kwargs = plan

    # inputs may themselves be planned joins; run them first
    left = unwrap_proxy(left)
    right = unwrap_proxy(right)
    if getattr(left, "_plan", None) is not None:
        left._materialize()
    if getattr(right, "_plan", None) is not None:
        right._materialize()
    if on is None and left_on is None and right_on is None:
        on = [c for c in left.columns if c in _columns_of(right)]
        if not on:
            raise ValueError("no common columns to merge on")
    left_keys = list(_as_list(left_on if left_on is not None else on))
    right_keys = list(_as_list(right_on if right_on is not None else on))

    if not isinstance(right, ChunkedFrame):
        return _broadcast_merge(
            left, right, left_keys, right_keys, how, kwargs, already_local=True
        )

    if broadcast is None:
        left_bytes, right_bytes = left.nbytes, right.nbytes
        replicate_cost = right_bytes * runtime_of(left).n_devices
        broadcast = how in ("inner", "left") and (
            right_bytes <= BROADCAST_THRESHOLD
            or replicate_cost <= left_bytes * BROADCAST_COST_RATIO
        )
        if DEBUG:
            print(f"[multigpu] merge how={how} left={left_bytes / (1 << 30):.2f}G "
                  f"right={right_bytes / (1 << 30):.2f}G "
                  f"replicate={replicate_cost / (1 << 30):.2f}G -> "
                  f"{'broadcast' if broadcast else 'shuffle'}",
                  file=sys.stderr, flush=True)
    if broadcast:
        if how not in ("inner", "left"):
            raise ValueError(
                f"broadcast join cannot produce correct results for how={how!r}; "
                "pass broadcast=False"
            )
        gathered = right.compute(left._devices[0])
        return _broadcast_merge(
            left, gathered, left_keys, right_keys, how, kwargs,
            source_device=left._devices[0],
        )

    runtime = left.runtime
    devices = list(runtime.devices)
    if nparts is None:
        # both sides must use the same count or matching keys land in
        # different partitions; size it from whichever side is bigger
        nparts = max(default_nparts(left, devices),
                     default_nparts(right, devices))
    left_s = hash_shuffle(left, left_keys, nparts=nparts, devices=devices)
    right_s = hash_shuffle(right, right_keys, nparts=nparts, devices=devices)

    targets = assign_targets(nparts, devices)
    jobs = [
        (
            targets[i],
            _local_merge,
            (left_s._chunks[i], right_s._chunks[i], on, left_on, right_on, how, kwargs),
            {},
        )
        for i in range(nparts)
    ]
    return _wrap_like(runtime.run_many(jobs), targets, runtime)


def runtime_of(frame):
    return frame.runtime


def _broadcast_merge(
    left, right_local, left_keys, right_keys, how, kwargs, source_device=None,
    already_local: bool = False,
):
    runtime = left.runtime
    if already_local:
        # A host/pandas or single-GPU right side: upload it to each device.
        replicas = {}
        for device in dict.fromkeys(left._devices):
            replicas[device] = runtime.run(device, _materialize, right_local)
    else:
        replicas = _transfer.broadcast(
            right_local, source_device, dict.fromkeys(left._devices), runtime
        )
    jobs = [
        (
            device,
            _local_merge,
            (chunk, replicas[device], None, left_keys, right_keys, how, kwargs),
            {},
        )
        for chunk, device in zip(left._chunks, left._devices, strict=True)
    ]
    return _wrap_like(runtime.run_many(jobs), left._devices, runtime)


def _materialize(obj):
    if isinstance(obj, cudf.DataFrame):
        return obj
    return cudf.DataFrame(obj)


def _local_merge(lhs, rhs, on, left_on, right_on, how, kwargs):
    if on is not None:
        return lhs.merge(rhs, on=on, how=how, **kwargs)
    return lhs.merge(rhs, left_on=left_on, right_on=right_on, how=how, **kwargs)


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _columns_of(obj):
    if isinstance(obj, ChunkedFrame):
        return list(obj.columns)
    if isinstance(obj, (cudf.DataFrame, pd.DataFrame)):
        return list(obj.columns)
    return []


# ----------------------------------------------------------------------
# sort
# ----------------------------------------------------------------------
def sort_values(
    frame: ChunkedFrame,
    by=None,
    ascending: bool = True,
    na_position: str = "last",
    ignore_index: bool = False,
    nparts: int | None = None,
    sample_per_chunk: int = 2000,
    **kwargs,
) -> ChunkedFrame:
    """Globally sort by range-partitioning on sampled splitters.

    Chunk *i* holds a contiguous, sorted slice of the key space, so the
    concatenation of the chunks in order is fully sorted.
    """
    runtime = frame.runtime
    is_series = isinstance(frame, ChunkedSeries)
    nparts = nparts or max(frame.nchunks, runtime.n_devices)
    if nparts == 1 or len(frame) == 0:
        out = frame.map_chunks(
            lambda c: _local_sort(c, by, ascending, na_position, is_series)
        )
        return out.reset_index(drop=True) if ignore_index else out

    keys = _as_list(by)
    directions = (
        list(ascending) if isinstance(ascending, (list, tuple)) else None
    )
    leading_ascending = directions[0] if directions else bool(ascending)

    # Range-partition on the leading key only when the sort directions differ
    # between keys. Bucketing by a multi-column lexicographic comparison is
    # only order-preserving when every key sorts the same way. Using just the
    # first key stays correct because rows sharing a first-key value always
    # land in the same bucket, so the local sort settles the rest.
    if directions is not None and len(set(directions)) > 1:
        keys = keys[:1]

    # 1. sample the key space from every chunk (host-side, tiny)
    samples = frame._run_chunks(
        lambda c: _sample_keys(c, keys, sample_per_chunk, is_series)
    )
    pooled = pd.concat([s for s in samples if len(s)], axis=0)
    if len(pooled) == 0:
        return frame.map_chunks(
            lambda c: _local_sort(c, by, ascending, na_position, is_series)
        )
    pooled = pooled.sort_values(list(pooled.columns)).reset_index(drop=True)
    quantiles = [i / nparts for i in range(1, nparts)]
    positions = sorted({min(len(pooled) - 1, int(q * len(pooled))) for q in quantiles})
    splitters = pooled.iloc[positions].reset_index(drop=True)
    n_buckets = len(splitters) + 1

    # 2. assign every row a bucket, then shuffle so buckets are contiguous
    part_ids = frame.map_chunks(
        lambda c: _bucket_of(
            c, keys, splitters, is_series, leading_ascending, n_buckets,
            na_position,
        )
    )
    shuffled = map_shuffle(frame, part_ids, n_buckets)

    # 3. sort within each bucket
    out = shuffled.map_chunks(
        lambda c: _local_sort(c, by, ascending, na_position, is_series)
    )
    if ignore_index:
        out = out.reset_index(drop=True)
    return out


def _keys_frame(chunk, keys, is_series):
    if is_series:
        return chunk.to_frame(name="__mg_key")
    return chunk[keys] if keys else chunk


def _sample_keys(chunk, keys, n, is_series):
    frame = _keys_frame(chunk, keys, is_series)
    if len(frame) == 0:
        return frame.head(0).to_pandas()
    if len(frame) > n:
        step = max(1, len(frame) // n)
        frame = frame.iloc[::step]
    return frame.to_pandas()


def _bucket_of(chunk, keys, splitters, is_series, ascending, n_buckets,
               na_position="last"):
    """Which bucket each row sorts into.

    Buckets are concatenated in order, so a row's bucket fixes where it lands
    globally. ``na_position`` therefore has to be applied *here* as well as in
    the per-bucket sort: sorting nulls to the front of whichever bucket
    ``searchsorted`` happened to put them in still leaves them in the middle of
    the frame. Null keys are sent to the first or last bucket outright.

    This showed up as TPC-DS q15 losing a row -- the null group sorted last
    instead of first, so ``head(100)`` cut it off and the answer was quietly
    short one group rather than visibly wrong.
    """
    frame = _keys_frame(chunk, keys, is_series)
    if len(frame) == 0:
        return cudf.Series([], dtype="int32")
    device_splitters = cudf.from_pandas(splitters)
    device_splitters.columns = frame.columns
    ids = device_splitters.searchsorted(frame, side="right")
    ids = cudf.Series(ids).astype("int32")
    if not ascending:
        ids = (n_buckets - 1) - ids

    null_key = None
    for column in frame.columns:
        is_null = frame[column].isna()
        null_key = is_null if null_key is None else (null_key | is_null)
    if null_key is not None and bool(null_key.any()):
        target = 0 if na_position == "first" else n_buckets - 1
        ids = ids.where(~null_key.reset_index(drop=True), target)
    return ids


def _local_sort(chunk, by, ascending, na_position, is_series):
    if len(chunk) == 0:
        return chunk
    if is_series or by is None:
        return chunk.sort_values(ascending=ascending, na_position=na_position)
    return chunk.sort_values(by=by, ascending=ascending, na_position=na_position)


# ----------------------------------------------------------------------
# distinct / index
# ----------------------------------------------------------------------
def drop_duplicates(frame, subset=None, keep="first", nparts=None, **kwargs):
    """Shuffle on the identifying columns, then de-duplicate locally."""
    runtime = frame.runtime
    nparts = nparts or max(frame.nchunks, runtime.n_devices)
    is_series = isinstance(frame, ChunkedSeries)
    as_frame = frame.to_frame(name="__mg_value") if is_series else frame
    keys = list(as_frame.columns) if subset is None else _as_list(subset)
    shuffled = hash_shuffle(as_frame, keys, nparts=nparts)
    out = shuffled.map_chunks(
        lambda c: c.drop_duplicates(subset=keys, keep=keep, **kwargs)
    )
    if is_series:
        return out["__mg_value"].rename(frame.name)
    return out


def set_index(frame: ChunkedDataFrame, keys, nparts: int | None = None, **kwargs):
    """Repartition by the new index so each key value lives on one GPU."""
    runtime = frame.runtime
    nparts = nparts or max(frame.nchunks, runtime.n_devices)
    key_list = _as_list(keys)
    shuffled = hash_shuffle(frame, key_list, nparts=nparts)
    return shuffled.map_chunks(lambda c: c.set_index(keys, **kwargs))


def quantile(series: ChunkedSeries, q, interpolation: str = "linear"):
    """Exact quantile via a global sort and positional lookup."""
    scalar = np.isscalar(q)
    qs = [q] if scalar else list(q)
    ordered = series.dropna().sort_values()
    n = len(ordered)
    if n == 0:
        return float("nan") if scalar else pd.Series([float("nan")] * len(qs), index=qs)

    results = []
    for value in qs:
        position = value * (n - 1)
        low, high = int(np.floor(position)), int(np.ceil(position))
        window = ordered._global_iloc(low, high + 1).to_pandas().to_numpy()
        if low == high or interpolation == "lower":
            results.append(window[0])
        elif interpolation == "higher":
            results.append(window[-1])
        elif interpolation == "nearest":
            results.append(window[0] if position - low < 0.5 else window[-1])
        elif interpolation == "midpoint":
            results.append((window[0] + window[-1]) / 2)
        else:
            frac = position - low
            results.append(window[0] + (window[-1] - window[0]) * frac)
    return results[0] if scalar else pd.Series(results, index=qs)


def series_value_counts(
    series: ChunkedSeries,
    sort: bool = True,
    ascending: bool = False,
    dropna: bool = True,
    **kwargs,
):
    """Distributed value counts: local counts, shuffle, combine."""
    name = series.name if series.name is not None else "__mg_value"
    frame = series.rename(name).to_frame()
    grouped = ChunkedGroupBy(frame, [name], as_index=True, dropna=dropna)
    counts = grouped.size()
    if sort:
        counts = counts.sort_values(ascending=ascending)
    return counts
