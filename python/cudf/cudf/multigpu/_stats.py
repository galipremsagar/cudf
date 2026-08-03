# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Statistics that reduce to a handful of per-chunk sums.

``skew``, ``kurtosis``, ``cov`` and ``corr`` each decompose into deviation
sums, so every GPU contributes a few numbers and the assembly happens on the
host.  Nothing large moves.

All of them are two-pass -- means first, then deviations about those means.
The one-pass form built from raw power sums is algebraically identical and
numerically useless here: it subtracts nearly equal large quantities, which
cost three significant digits of ``corr`` on real data.

``ffill``/``bfill`` also live here; they are a scan over chunk boundaries
carrying one row of state.
"""

from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd

import cudf

__all__ = ["skew", "kurtosis", "cov", "corr", "fill_directional", "mode"]


# ----------------------------------------------------------------------
# shape statistics
# ----------------------------------------------------------------------
def _count_and_sum(chunk):
    frame = chunk.to_frame() if isinstance(chunk, cudf.Series) else chunk
    return frame.count().to_pandas(), frame.sum().to_pandas()


def _deviation_sums(chunk, mean: pd.Series, order: int):
    """Sums of ``(x - mean)**k`` for k = 2..order, given the *global* mean."""
    frame = chunk.to_frame() if isinstance(chunk, cudf.Series) else chunk
    centered = frame - cudf.Series(mean.astype("float64"))
    squared = centered * centered
    sums = [squared.sum().to_pandas()]
    power = squared
    for _ in range(order - 2):
        power = power * centered
        sums.append(power.sum().to_pandas())
    return sums


def _central_moments(frame, order: int):
    """Central moments m2..m_order about the global mean.

    Deliberately two-pass: the mean is computed first, then the deviations are
    accumulated directly.  Deriving these from raw power sums instead loses
    most of the significant digits whenever the mean is large relative to the
    spread, which showed up as a 4th-decimal error in ``corr``.
    """
    parts = frame._run_chunks(_count_and_sum)
    n = pd.DataFrame([p[0] for p in parts]).sum()
    mean = pd.DataFrame([p[1] for p in parts]).sum() / n

    deviations = frame._run_chunks(
        lambda c: _deviation_sums(c, mean, order)
    )
    moments = {1: mean}
    for k in range(2, order + 1):
        total = pd.DataFrame([d[k - 2] for d in deviations]).sum()
        moments[k] = total / n
    return n, moments


def skew(frame, **kwargs):
    """Bias-corrected (Fisher-Pearson) skewness, matching pandas."""
    n, m = _central_moments(frame, 3)
    g1 = m[3] / m[2] ** 1.5
    corrected = g1 * np.sqrt(n * (n - 1)) / (n - 2)
    return _unwrap(frame, corrected.where(n > 2, np.nan))


def kurtosis(frame, **kwargs):
    """Bias-corrected excess kurtosis, matching pandas."""
    n, m = _central_moments(frame, 4)
    g2 = m[4] / m[2] ** 2 - 3
    corrected = ((n + 1) * g2 + 6) * (n - 1) / ((n - 2) * (n - 3))
    return _unwrap(frame, corrected.where(n > 3, np.nan))


def _unwrap(frame, result: pd.Series):
    """Series input -> scalar out; DataFrame input -> Series out."""
    from ._frame import ChunkedSeries

    if isinstance(frame, ChunkedSeries):
        return float(result.iloc[0])
    return result


# ----------------------------------------------------------------------
# pairwise statistics
# ----------------------------------------------------------------------
def _pair_sums(chunk, columns: Sequence[str]):
    """Per-chunk pairwise sums restricted to rows where both columns are valid.

    pandas computes covariance over pairwise-complete observations, so the
    means must be taken over exactly the rows that contribute to the cross
    term -- not over each column's own non-null rows.
    """
    frame = chunk[list(columns)]
    stats: dict[tuple, tuple[float, float, float]] = {}
    for i, a in enumerate(columns):
        for b in columns[i:]:
            product = frame[a] * frame[b]
            valid = product.notna()
            stats[(a, b)] = (
                float(valid.sum()),
                float(frame[a].where(valid).sum()),
                float(frame[b].where(valid).sum()),
            )
    return stats


def _pair_deviation_sums(chunk, columns: Sequence[str], means: dict):
    """Deviation sums over pairwise-complete rows.

    Returns ``(sum_ab, sum_aa, sum_bb)`` per pair.  The two variance terms are
    restricted to the same rows as the cross term, because that is what pandas
    correlates -- using each column's own variance gives a subtly different
    answer whenever the columns have different null patterns.
    """
    frame = chunk[list(columns)]
    out: dict[tuple, tuple[float, float, float]] = {}
    for i, a in enumerate(columns):
        for b in columns[i:]:
            mean_a, mean_b = means[(a, b)]
            valid = (frame[a] * frame[b]).notna()
            dev_a = (frame[a] - mean_a).where(valid)
            dev_b = (frame[b] - mean_b).where(valid)
            out[(a, b)] = (
                float((dev_a * dev_b).sum()),
                float((dev_a * dev_a).sum()),
                float((dev_b * dev_b).sum()),
            )
    return out


def _numeric_columns(frame) -> list:
    return [
        c for c, dtype in frame._meta.dtypes.items()
        if pd.api.types.is_numeric_dtype(dtype)
    ]


def _pairwise_moments(frame, columns):
    """``(counts, {pair: [sum_ab, sum_aa, sum_bb]})`` over complete pairs."""
    # Pass 1: pairwise-complete counts and means.
    parts = frame._run_chunks(lambda c: _pair_sums(c, columns))
    pooled: dict[tuple, list[float]] = {}
    for part in parts:
        for key, values in part.items():
            acc = pooled.setdefault(key, [0.0, 0.0, 0.0])
            for i, value in enumerate(values):
                acc[i] += value
    means = {
        key: (
            (sum_a / count, sum_b / count) if count else (0.0, 0.0)
        )
        for key, (count, sum_a, sum_b) in pooled.items()
    }

    # Pass 2: accumulate deviations about those means (stable).
    deviations = frame._run_chunks(
        lambda c: _pair_deviation_sums(c, columns, means)
    )
    cross: dict[tuple, list[float]] = {}
    for part in deviations:
        for key, values in part.items():
            acc = cross.setdefault(key, [0.0, 0.0, 0.0])
            for i, value in enumerate(values):
                acc[i] += value

    counts = {key: values[0] for key, values in pooled.items()}
    return counts, cross


def cov(frame, min_periods: int | None = None, **kwargs):
    """Pairwise sample covariance (ddof=1), matching pandas."""
    columns = _numeric_columns(frame)
    counts, cross = _pairwise_moments(frame, columns)
    floor = 2 if min_periods is None else max(2, min_periods)
    out = pd.DataFrame(index=columns, columns=columns, dtype="float64")
    for i, a in enumerate(columns):
        for b in columns[i:]:
            count = counts[(a, b)]
            value = cross[(a, b)][0] / (count - 1) if count >= floor else np.nan
            out.loc[a, b] = value
            out.loc[b, a] = value
    return out


def corr(frame, method: str = "pearson", min_periods: int | None = None, **kwargs):
    """Pairwise Pearson correlation over pairwise-complete observations."""
    if method != "pearson":
        raise NotImplementedError(
            f"multi-GPU corr supports method='pearson', not {method!r}"
        )
    columns = _numeric_columns(frame)
    counts, cross = _pairwise_moments(frame, columns)
    floor = 2 if min_periods is None else max(2, min_periods)
    out = pd.DataFrame(index=columns, columns=columns, dtype="float64")
    for i, a in enumerate(columns):
        for b in columns[i:]:
            sum_ab, sum_aa, sum_bb = cross[(a, b)]
            denominator = np.sqrt(sum_aa * sum_bb)
            if counts[(a, b)] < floor or denominator == 0:
                value = np.nan
            else:
                value = sum_ab / denominator
            out.loc[a, b] = value
            out.loc[b, a] = value
    return out


# ----------------------------------------------------------------------
# directional fills (a scan over chunk boundaries)
# ----------------------------------------------------------------------
def fill_directional(frame, forward: bool, **kwargs):
    """``ffill``/``bfill`` across chunk boundaries.

    After filling within a chunk, the only nulls left are a leading (or
    trailing) run.  Those are filled from the last valid value seen in the
    neighbouring chunks, which is a single row of host-side state per chunk.
    """
    from ._frame import _to_host, _wrap_like

    method = "ffill" if forward else "bfill"
    local = frame.map_chunks(lambda c: getattr(c, method)(**kwargs))
    edges = local._run_chunks(
        lambda c: _to_host(c.tail(1) if forward else c.head(1))
    )

    order = range(frame.nchunks) if forward else range(frame.nchunks - 1, -1, -1)
    carries: dict[int, dict] = {}
    running: dict = {}
    for i in order:
        carries[i] = dict(running)
        edge = edges[i]
        if len(edge) == 0:
            continue
        row = edge.iloc[0] if isinstance(edge, pd.DataFrame) else edge
        if isinstance(row, pd.Series) and not isinstance(edge, pd.DataFrame):
            values = {frame._meta.name: row.iloc[0]}
        else:
            values = dict(row)
        for key, value in values.items():
            if not pd.isna(value):
                running[key] = value

    jobs = [
        (device, _fill_from_carry, (chunk, carries[i]), {})
        for i, (chunk, device) in enumerate(
            zip(local._chunks, local._devices, strict=True)
        )
    ]
    return _wrap_like(frame.runtime.run_many(jobs), local._devices, frame.runtime)


def _fill_from_carry(chunk, carry: dict):
    if not carry or len(chunk) == 0:
        return chunk
    if isinstance(chunk, cudf.Series):
        value = next(iter(carry.values()), None)
        return chunk if value is None else chunk.fillna(value)
    usable = {k: v for k, v in carry.items() if k in chunk.columns}
    return chunk.fillna(usable) if usable else chunk


# ----------------------------------------------------------------------
def mode(frame, dropna: bool = True, **kwargs):
    """Most frequent value(s), from a distributed value-count."""
    counts = frame.value_counts(dropna=dropna)
    host = counts.to_pandas()
    if len(host) == 0:
        return pd.Series([], dtype=frame._meta.dtype)
    top = host.max()
    return pd.Series(sorted(host[host == top].index), name=None)
