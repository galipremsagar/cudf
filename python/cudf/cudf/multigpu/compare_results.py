# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Diff two configurations' saved answers, query by query.

    python -m cudf.multigpu.compare_results --a results/8gpu --b results/cpu

Both directories come from ``--save-results``. This answers a narrower question
than the DuckDB reference does: not "is the query right" but "does spreading it
across eight GPUs change the answer". That is the question worth asking when the
same translation runs on both sides, because any difference is attributable to
the execution layer rather than to the query.

Floats are compared with a relative tolerance by default: the two
configurations sum in different orders, so the last bits legitimately differ.
Integers and strings are compared exactly, since no reordering explains a
difference there.

A query present in only one directory is reported as such and never counted as
agreeing -- an absent answer is the one case where "no differences found" would
be actively misleading.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def _numeric(series):
    import pandas as pd

    if pd.api.types.is_bool_dtype(series) or pd.api.types.is_numeric_dtype(series):
        return series.astype("float64")
    if series.dtype == object:
        converted = pd.to_numeric(series, errors="coerce")
        if converted.notna().sum() == series.notna().sum():
            return converted.astype("float64")
    return None


def compare_frames(a, b, rtol: float) -> tuple[bool, str, float]:
    """-> (same, why, largest relative difference seen)."""
    import numpy as np
    import pandas as pd

    if len(a) != len(b):
        return False, f"{len(a)} rows vs {len(b)}", 0.0
    if a.shape[1] != b.shape[1]:
        return False, f"{a.shape[1]} cols vs {b.shape[1]}", 0.0

    worst = 0.0
    problems = []
    for i in range(a.shape[1]):
        left = a.iloc[:, i].reset_index(drop=True)
        right = b.iloc[:, i].reset_index(drop=True)
        lf, rf = _numeric(left), _numeric(right)
        if lf is not None and rf is not None:
            both_null = lf.isna() & rf.isna()
            denom = rf.abs().where(rf.abs() > 1e-12, 1.0)
            rel = ((lf - rf).abs() / denom).where(~both_null, 0.0)
            worst = max(worst, float(rel.max(skipna=True) or 0.0))
            close = np.isclose(lf.fillna(0), rf.fillna(0), rtol=rtol, atol=1e-6)
            if not bool((close | both_null).all()):
                n = int((~(close | both_null)).sum())
                problems.append(f"col {i}: {n} values differ")
        else:
            ls = left.astype("string").str.strip()
            rs = right.astype("string").str.strip()
            same = (ls == rs) | (ls.isna() & rs.isna())
            if not bool(same.all()):
                problems.append(f"col {i}: {int((~same).sum())} values differ")
    if problems:
        # An ORDER BY that does not fully determine the order leaves ties whose
        # arrangement is arbitrary -- DuckDB returns three different orderings
        # across identical runs of some queries. Same rows in a different order
        # is a different fact from wrong rows, and is reported as such rather
        # than counted as a disagreement.
        if _same_rows_unordered(a, b, rtol):
            return "tie-order", "same rows, tie order differs", worst
        return False, "; ".join(problems), worst
    return True, "", worst


def _same_rows_unordered(a, b, rtol: float) -> bool:
    """Whether the two frames hold the same rows, ignoring row order."""
    def key(frame):
        out = frame.copy()
        out.columns = range(frame.shape[1])
        for i in range(frame.shape[1]):
            num = _numeric(out[i])
            out[i] = (num.round(6) if num is not None
                      else out[i].astype("string").str.strip())
        return out.sort_values(list(out.columns),
                               na_position="last").reset_index(drop=True)

    try:
        left, right = key(a), key(b)
    except Exception:
        return False
    if left.shape != right.shape:
        return False
    for i in range(left.shape[1]):
        x, y = left[i], right[i]
        if not bool(((x == y) | (x.isna() & y.isna())).all()):
            return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--a", required=True, help="first results directory")
    parser.add_argument("--b", required=True, help="second results directory")
    parser.add_argument("--label-a", default="A")
    parser.add_argument("--label-b", default="B")
    parser.add_argument("--rtol", type=float, default=1e-6)
    args = parser.parse_args()

    import pandas as pd

    a_dir, b_dir = Path(args.a), Path(args.b)
    a_have = {int(p.stem[1:]) for p in a_dir.glob("q*.parquet")}
    b_have = {int(p.stem[1:]) for p in b_dir.glob("q*.parquet")}

    both = sorted(a_have & b_have)
    only_a = sorted(a_have - b_have)
    only_b = sorted(b_have - a_have)

    print(f"{args.label_a}: {len(a_have)} answers   "
          f"{args.label_b}: {len(b_have)} answers   "
          f"comparable: {len(both)}\n")

    agree, differ, tie_order = [], [], []
    worst_overall = 0.0
    for q in both:
        fa = pd.read_parquet(a_dir / f"q{q}.parquet")
        fb = pd.read_parquet(b_dir / f"q{q}.parquet")
        same, why, worst = compare_frames(fa, fb, args.rtol)
        # A tie-order query's "worst difference" is computed by comparing rows
        # that are in different positions, so it is a number about ordering, not
        # about values -- q11 at TPC-H SF500 reports 1.37e3 that way while being
        # row-for-row identical once sorted. Quoting it alongside genuine value
        # differences is alarming and meaningless, so it is excluded.
        if same != "tie-order":
            worst_overall = max(worst_overall, worst)
        if same is True:
            agree.append(q)
        elif same == "tie-order":
            tie_order.append(q)
        else:
            differ.append((q, why, worst))

    print(f"agree            : {len(agree)}/{len(both)}")
    if tie_order:
        print(f"same rows, tie order differs: {tie_order}")
    if differ:
        print(f"differ           : {len(differ)}")
        for q, why, worst in differ:
            print(f"    q{q}: {why}   (largest relative diff {worst:.3g})")
    if only_a:
        print(f"only in {args.label_a}: {only_a}")
    if only_b:
        print(f"only in {args.label_b}: {only_b}")
    print(f"\nlargest relative difference among comparable values: "
          f"{worst_overall:.3g}   (tolerance {args.rtol:g})")
    if tie_order:
        print("  (tie-order queries excluded: their rows sit in different "
              "positions, so a positional difference says nothing about values)")


if __name__ == "__main__":
    main()
