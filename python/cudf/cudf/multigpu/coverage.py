# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Report which cuDF APIs run distributed and which fall back to one GPU.

    python -m cudf.multigpu.coverage            # summary
    python -m cudf.multigpu.coverage --list     # every method, categorized

A name is "distributed" when it resolves on the chunked class itself -- either
an explicit implementation or a generated per-chunk mapping.  Anything else is
served by ``_FallbackMixin.__getattr__``, which gathers the frame onto a single
GPU, runs stock cuDF, and warns.  Those still *work*; they just stop being
multi-GPU, so the frame has to fit on one device.
"""

from __future__ import annotations

import argparse
import inspect

import cudf

from cudf.multigpu._frame import (
    ChunkedDataFrame,
    ChunkedIndex,
    ChunkedSeries,
    _FallbackMixin,
)

#: names implemented as true distributed algorithms (not per-chunk mapping)
DISTRIBUTED_ALGORITHMS = {
    # shuffle-backed
    "groupby", "merge", "join", "sort_values", "drop_duplicates", "set_index",
    "value_counts", "unique", "nunique", "duplicated", "factorize",
    # reductions
    "sum", "min", "max", "count", "mean", "std", "var", "prod", "product",
    "any", "all", "idxmin", "idxmax", "quantile", "median", "describe",
    "skew", "kurtosis", "kurt", "corr", "cov", "agg", "aggregate",
    "memory_usage", "mode",
    # scans and boundary-crossing
    "cumsum", "cumprod", "cummax", "cummin", "shift", "diff", "pct_change",
    "ffill", "bfill", "pad", "backfill", "interpolate",
    # whole-frame reshaping
    "melt", "convert_dtypes",
    # ordered / positional
    "head", "tail", "reset_index", "nlargest", "nsmallest", "sample",
    # materialization
    "to_pandas", "to_parquet", "to_arrow",
}


def _public_names(cls) -> list[str]:
    return sorted(
        name
        for name in dir(cls)
        if not name.startswith("_") and not name.isupper()
    )


def classify(chunked_cls, cudf_cls) -> dict[str, list[str]]:
    """Split the cuDF API surface by how the chunked class serves it."""
    fallback_names = set(dir(_FallbackMixin))
    algorithms: list[str] = []
    mapped: list[str] = []
    fallback: list[str] = []

    for name in _public_names(cudf_cls):
        if name in fallback_names:
            continue
        # Defined anywhere on the chunked class's own MRO (excluding the
        # fallback mixin) means it has a real multi-GPU path.
        owner = None
        for klass in chunked_cls.__mro__:
            if klass is _FallbackMixin:
                continue
            if name in vars(klass):
                owner = klass
                break
        if owner is None:
            fallback.append(name)
        elif name in DISTRIBUTED_ALGORITHMS:
            algorithms.append(name)
        else:
            mapped.append(name)
    return {"algorithm": algorithms, "mapped": mapped, "fallback": fallback}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="list every method")
    args = parser.parse_args()

    pairs = [
        ("DataFrame", ChunkedDataFrame, cudf.DataFrame),
        ("Series", ChunkedSeries, cudf.Series),
        ("Index", ChunkedIndex, cudf.Index),
    ]
    print(f"{'type':<12}{'distributed':>13}{'per-chunk':>12}{'fallback':>11}"
          f"{'total':>8}{'covered':>10}")
    print("-" * 66)
    totals = [0, 0, 0]
    for label, chunked, base in pairs:
        buckets = classify(chunked, base)
        counts = [len(buckets[k]) for k in ("algorithm", "mapped", "fallback")]
        totals = [t + c for t, c in zip(totals, counts)]
        total = sum(counts)
        covered = (counts[0] + counts[1]) / total * 100 if total else 0
        print(f"{label:<12}{counts[0]:>13}{counts[1]:>12}{counts[2]:>11}"
              f"{total:>8}{covered:>9.0f}%")
    grand = sum(totals)
    print("-" * 66)
    print(f"{'all':<12}{totals[0]:>13}{totals[1]:>12}{totals[2]:>11}{grand:>8}"
          f"{(totals[0] + totals[1]) / grand * 100:>9.0f}%")
    print("\ndistributed = a real multi-GPU algorithm (shuffle/reduce/scan)")
    print("per-chunk   = applied independently to each chunk, no movement")
    print("fallback    = gathers onto one GPU and warns; correct but not scalable")

    if args.list:
        for label, chunked, base in pairs:
            buckets = classify(chunked, base)
            print(f"\n=== {label} ===")
            for kind in ("algorithm", "mapped", "fallback"):
                names = buckets[kind]
                print(f"  {kind} ({len(names)}):")
                for i in range(0, len(names), 6):
                    print("    " + "  ".join(f"{n:<22}" for n in names[i : i + 6]))


if __name__ == "__main__":
    main()
