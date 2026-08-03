# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Operations that look wrong -- but not obviously wrong -- if mapped per chunk.

``duplicated`` compares a row against every *earlier* row, ``factorize``
numbers values across the whole frame, ``melt`` reorders rows, ``interpolate``
reads across chunk boundaries, and ``convert_dtypes`` can infer a different
type per chunk.  Applying any of them chunk by chunk returns a chunk-local
answer that looks plausible and is silently wrong, so each one is implemented
here against the whole frame.
"""

from __future__ import annotations

import itertools
from typing import Any, Sequence

import numpy as np
import pandas as pd

import cudf

from ._frame import (
    ChunkedDataFrame,
    ChunkedFrame,
    ChunkedSeries,
    _to_host,
    _wrap_like,
)

__all__ = ["duplicated", "factorize", "melt", "interpolate", "convert_dtypes"]

_POSITION = "__mg_position"
_FLAG = "__mg_duplicated"


# ----------------------------------------------------------------------
# global row positions
# ----------------------------------------------------------------------
def _with_positions(frame: ChunkedFrame, columns: Sequence | None = None):
    """A frame of ``columns`` plus each row's global position."""
    offsets = list(itertools.accumulate([0] + frame.chunk_lengths))
    jobs = []
    for i, (chunk, device) in enumerate(zip(frame._chunks, frame._devices)):
        jobs.append((device, _attach_position, (chunk, offsets[i], columns), {}))
    return _wrap_like(frame.runtime.run_many(jobs), frame._devices, frame.runtime)


def _attach_position(chunk, offset: int, columns):
    frame = chunk.to_frame() if isinstance(chunk, cudf.Series) else chunk
    frame = frame if columns is None else frame[list(columns)]
    out = frame.reset_index(drop=True)
    out[_POSITION] = cudf.Series(
        np.arange(offset, offset + len(out), dtype="int64")
    )
    return out


def _restore_order(values: ChunkedFrame, template: ChunkedFrame, column: str):
    """Put ``values`` back in ``template``'s row order and partitioning."""
    ordered = values.sort_values(_POSITION)
    ordered = ordered._rechunk_to(template.chunk_lengths, template.devices)
    return ordered.map_chunks(_take_column, template, column=column)


def _take_column(chunk, original, column: str):
    series = chunk[column].reset_index(drop=True)
    series.index = original.index
    return series


# ----------------------------------------------------------------------
# duplicated
# ----------------------------------------------------------------------
def duplicated(frame: ChunkedFrame, subset=None, keep: str | bool = "first"):
    """Boolean mask of rows duplicating an earlier (or later) row.

    Rows sharing a key are gathered onto one GPU, compared by their *global*
    position, and the answer is then put back in the original row order.
    """
    if keep not in ("first", "last", False):
        raise ValueError(f"keep must be 'first', 'last' or False, got {keep!r}")

    is_series = isinstance(frame, ChunkedSeries)
    if is_series:
        keys = ["__mg_value"]
        positioned = _with_positions(frame.rename("__mg_value").to_frame())
    else:
        keys = list(frame.columns) if subset is None else list(subset)
        positioned = _with_positions(frame, keys)

    from ._shuffle import hash_shuffle

    shuffled = hash_shuffle(positioned, keys)
    flagged = shuffled.map_chunks(_flag_duplicates, keys=keys, keep=keep)
    return _restore_order(flagged, frame, _FLAG)


def _flag_duplicates(chunk, keys, keep):
    if len(chunk) == 0:
        out = chunk.head(0)
        out[_FLAG] = cudf.Series([], dtype="bool")
        return out[[_POSITION, _FLAG]]
    if keep is False:
        counts = chunk.groupby(keys, as_index=False, dropna=False).agg(
            {_POSITION: "count"}
        )
        counts = counts.rename(columns={_POSITION: "__mg_n"})
        merged = chunk.merge(counts, on=keys)
        merged[_FLAG] = merged["__mg_n"] > 1
    else:
        how = "min" if keep == "first" else "max"
        anchors = chunk.groupby(keys, as_index=False, dropna=False).agg(
            {_POSITION: how}
        )
        anchors = anchors.rename(columns={_POSITION: "__mg_anchor"})
        merged = chunk.merge(anchors, on=keys)
        merged[_FLAG] = merged[_POSITION] != merged["__mg_anchor"]
    return merged[[_POSITION, _FLAG]]


# ----------------------------------------------------------------------
# factorize
# ----------------------------------------------------------------------
def factorize(series: ChunkedSeries, sort: bool = False, **kwargs):
    """``(codes, uniques)`` numbered over the whole frame.

    Codes must be global: numbering each chunk independently would give the
    same value different codes in different chunks.
    """
    uniques = series.unique()
    host_uniques = _to_host(uniques.compute())
    if sort:
        host_uniques = host_uniques.sort_values()
    host_uniques = host_uniques.reset_index(drop=True)

    lookup = pd.DataFrame(
        {
            "__mg_value": host_uniques,
            "__mg_code": np.arange(len(host_uniques), dtype="int64"),
        }
    )
    codes = series.map_chunks(
        _encode, broadcast=[lookup], name=series.name
    )
    return codes, host_uniques


def _encode(chunk, lookup, name):
    frame = chunk.rename("__mg_value").to_frame().reset_index(drop=True)
    frame["__mg_order"] = cudf.Series(np.arange(len(frame), dtype="int64"))
    merged = frame.merge(lookup, on="__mg_value", how="left")
    merged = merged.sort_values("__mg_order")
    codes = merged["__mg_code"].fillna(-1).astype("int64")
    codes.index = chunk.index
    return codes.rename(name)


# ----------------------------------------------------------------------
# melt
# ----------------------------------------------------------------------
def melt(
    frame: ChunkedDataFrame,
    id_vars=None,
    value_vars=None,
    var_name: str | None = None,
    value_name: str = "value",
    **kwargs,
):
    """Unpivot, preserving pandas' row order.

    pandas emits every row of the first value column, then every row of the
    second, and so on.  Melting each chunk independently and concatenating
    interleaves those blocks, so the columns are unpivoted one at a time and
    the per-column results concatenated in order instead.
    """
    from ._creation import concat

    columns = list(frame.columns)
    id_vars = [] if id_vars is None else list(_as_list(id_vars))
    if value_vars is None:
        value_vars = [c for c in columns if c not in id_vars]
    else:
        value_vars = list(_as_list(value_vars))
    var_name = var_name or "variable"

    pieces = [
        frame.map_chunks(
            _melt_one,
            id_vars=id_vars,
            value_var=value,
            var_name=var_name,
            value_name=value_name,
        )
        for value in value_vars
    ]
    if not pieces:
        raise ValueError("melt needs at least one value column")
    return concat(pieces)


def _melt_one(chunk, id_vars, value_var, var_name, value_name):
    out = chunk[list(id_vars)].reset_index(drop=True)
    out[var_name] = cudf.Series([value_var] * len(chunk))
    out[value_name] = chunk[value_var].reset_index(drop=True)
    return out


def _as_list(value):
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


# ----------------------------------------------------------------------
# interpolate
# ----------------------------------------------------------------------
def interpolate(frame: ChunkedFrame, method: str = "linear", **kwargs):
    """Linear interpolation of nulls, including runs spanning chunk boundaries.

    Built from the distributed directional fills rather than cuDF's local
    ``interpolate``: each null takes the last valid value before it and the
    first valid value after it -- which may live on other GPUs -- and is
    placed between them by global row position.
    """
    if method != "linear":
        raise NotImplementedError(
            f"multi-GPU interpolate supports method='linear', not {method!r}"
        )
    if isinstance(frame, ChunkedDataFrame):
        columns = list(frame.columns)
        result = frame
        for column in columns:
            filled = interpolate(frame[column], method=method, **kwargs)
            result = result.map_chunks(
                _replace_column, filled, column=column
            )
        return result

    positioned = _with_positions(frame.rename("__mg_value").to_frame())
    valid = positioned.map_chunks(_mark_valid)

    previous_value = valid["__mg_value"].ffill()
    previous_pos = valid["__mg_valid_pos"].ffill()
    next_value = valid["__mg_value"].bfill()
    next_pos = valid["__mg_valid_pos"].bfill()

    return valid.map_chunks(
        _interpolate_between,
        previous_value,
        previous_pos,
        next_value,
        next_pos,
        name=frame.name,
    )


def _mark_valid(chunk):
    out = chunk.copy(deep=False)
    out["__mg_valid_pos"] = out[_POSITION].where(out["__mg_value"].notna())
    return out


def _interpolate_between(chunk, prev_v, prev_p, next_v, next_p, name):
    position = chunk[_POSITION].astype("float64")
    prev_p = prev_p.astype("float64")
    next_p = next_p.astype("float64")
    span = next_p - prev_p
    weight = ((position - prev_p) / span).fillna(0.0)
    interpolated = prev_v + (next_v - prev_v) * weight
    # Leading nulls have no earlier anchor and stay null (pandas' default);
    # trailing nulls take the last valid value.
    interpolated = interpolated.fillna(prev_v)
    result = chunk["__mg_value"].fillna(interpolated)
    result.index = chunk.index
    return result.rename(name)


def _replace_column(chunk, values, column):
    out = chunk.copy(deep=False)
    out[column] = values
    return out


# ----------------------------------------------------------------------
# convert_dtypes
# ----------------------------------------------------------------------
def convert_dtypes(frame: ChunkedFrame, **kwargs):
    """Infer better dtypes, using one decision for the whole frame.

    Inference is per chunk, so one chunk can conclude ``int64`` while another
    concludes ``float64`` for the same column.  The proposals are reconciled on
    the host and a single dtype applied everywhere, because chunks of one frame
    must agree on schema.
    """
    proposals = frame._run_chunks(
        lambda c: _to_host(c.convert_dtypes(**kwargs).head(0)).dtypes
    )
    if isinstance(frame, ChunkedSeries):
        unified = _common_dtype([p.iloc[0] for p in proposals])
        return frame.astype(unified)

    columns = list(proposals[0].index)
    unified = {
        column: _common_dtype([p[column] for p in proposals])
        for column in columns
    }
    return frame.map_chunks(lambda c: c.astype(unified))


def _common_dtype(dtypes: Sequence) -> Any:
    """A dtype every chunk can be cast to."""
    distinct = list(dict.fromkeys(str(d) for d in dtypes))
    if len(distinct) == 1:
        return dtypes[0]
    try:
        return np.result_type(*[np.dtype(str(d)) for d in dtypes])
    except TypeError:
        # Non-numpy (extension) dtypes that disagree: keep them as objects
        # rather than silently picking one chunk's guess.
        return np.dtype("object")
