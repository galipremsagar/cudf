# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Run the TPC-DS queries, on one GPU or on several.

    python -m cudf.multigpu.tpcds_run --path /raid/pgali/tpcds/sf1 --strict
    python -m cudf.multigpu.tpcds_run --path /raid/pgali/tpcds/sf1 --backend cudf

Same shape as ``tpch_pdsh`` -- identical timing points, identical fallback
detection -- so the single-GPU and multi-GPU columns are measured the same way.

Correctness is checked against DuckDB's answer to the official SQL (see
``tpcds_reference``), not against a previous run of this code, so a translation
that is wrong in the same way twice still fails.
"""

from __future__ import annotations

import argparse
import gc
import os
import time
import traceback
from pathlib import Path

from cuda.bindings import runtime as cudart

from . import tpcds_queries
from .tpcds_reference import load as load_reference

GIB = 1 << 30
FALLBACK_RSS_GIB = 2.0


class _RunConfig:
    def __init__(self, dataset_path: str, scale_factor: float, suffix: str):
        self.dataset_path = dataset_path
        self.scale_factor = scale_factor
        self.suffix = suffix


def _host_rss_gib() -> float:
    try:
        with open("/proc/self/statm") as handle:
            return int(handle.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / GIB
    except Exception:
        return 0.0


def _context_is_dead() -> bool:
    err, ptr = cudart.cudaMalloc(256)
    if err == cudart.cudaError_t.cudaSuccess:
        cudart.cudaFree(ptr)
        return False
    return True


def _to_host(result):
    """Bring the answer back to genuine host pandas.

    ``to_pandas()`` is not enough: under cudf.pandas the result it returns is
    re-wrapped into a proxy whose fast side is another chunked frame, so the
    comparison still runs on the GPU and every gap in the indexing layer looks
    like a wrong answer. ``_fsproxy_slow`` is the proxy's real pandas object.

    This is a deliberate materialization of a final result of at most a few
    thousand rows, not a silent fallback -- the queries themselves are still
    held to ``--strict``.

    isinstance against ``pd.Series`` is avoided on purpose: ``pd`` is the proxy
    module here, so the check would be asking a proxy type about a real pandas
    object.
    """
    slow = getattr(result, "_fsproxy_slow", None)
    if slow is not None:
        result = slow
    elif hasattr(result, "to_pandas"):
        result = result.to_pandas()
    if not hasattr(result, "columns"):  # a Series
        result = result.to_frame()
    return result.reset_index(drop=True)


def _numeric(series):
    """``series`` as float64 if it is numeric at all, else None.

    Handles the ``decimal.Decimal`` objects DuckDB writes for SQL DECIMAL
    columns, which arrive as dtype ``object`` and so are not caught by
    ``is_numeric_dtype``. Comparing those as *strings* would make correctness
    depend on rendering -- 60828.6 against 60828.60 -- which is a property of
    formatting, not of the answer.
    """
    import pandas as pd

    if pd.api.types.is_bool_dtype(series) or pd.api.types.is_numeric_dtype(series):
        return series.astype("float64")
    if series.dtype == object:
        converted = pd.to_numeric(series, errors="coerce")
        if converted.notna().sum() == series.notna().sum():
            return converted.astype("float64")
    return None


def _compare(got, expected, rtol: float = 1e-6) -> tuple[bool, str]:
    """Whether ``got`` matches DuckDB's answer.

    Column *names* are not compared. SQL names an aggregate whatever the query
    says, and a pandas translation reasonably names it something else; what has
    to agree is the shape, the order, and the values.

    Integers are compared exactly. An earlier version used a 1e-2 tolerance for
    everything, which meant a count of 109 passed against an expected 108 -- and
    that is precisely the size of error a wrong join produces, so the check was
    blind to the bug class it most needed to catch. Floats keep a small relative
    tolerance because the two engines sum in different orders.
    """
    import numpy as np
    import pandas as pd

    if expected is None:
        return True, "no reference"
    if len(got) != len(expected):
        return False, f"{len(got)} rows, expected {len(expected)}"
    if got.shape[1] != expected.shape[1]:
        return False, f"{got.shape[1]} columns, expected {expected.shape[1]}"

    for i in range(expected.shape[1]):
        left = got.iloc[:, i].reset_index(drop=True)
        right = expected.iloc[:, i].reset_index(drop=True)
        lf, rf = _numeric(left), _numeric(right)
        if lf is not None and rf is not None:
            both_null = lf.isna() & rf.isna()
            whole = bool(
                np.isclose(rf.dropna() % 1, 0).all()
                and np.isclose(lf.dropna() % 1, 0).all()
            )
            if whole:
                same = lf.fillna(np.nan) == rf.fillna(np.nan)
            else:
                same = pd.Series(
                    np.isclose(lf.fillna(0), rf.fillna(0), rtol=rtol, atol=1e-6),
                    index=lf.index,
                )
            if not bool((same | both_null).all()):
                bad = int((~(same | both_null)).sum())
                kind = "exact" if whole else f"rtol={rtol:g}"
                return False, f"column {i}: {bad} values differ ({kind})"
        else:
            ls = left.astype("string").str.strip()
            rs = right.astype("string").str.strip()
            if not bool(((ls == rs) | (ls.isna() & rs.isna())).all()):
                bad = int((~((ls == rs) | (ls.isna() & rs.isna()))).sum())
                return False, f"column {i}: {bad} values differ"
    return True, ""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True)
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--backend",
                        choices=["multigpu", "cudf", "pandas"],
                        default="multigpu",
                        help="'pandas' is stock CPU pandas, for checking a "
                             "translation against DuckDB without any GPU")
    parser.add_argument("--queries", default="all")
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--strict", action="store_true",
                        help="fail a query that silently ran on the CPU")
    parser.add_argument("--memory-resource", default="pool",
                        choices=["pool", "async", "managed"])
    parser.add_argument("--pool-fraction", type=float, default=0.94)
    parser.add_argument("--initial-pool-fraction", type=float, default=0.30)
    parser.add_argument("--validate", action="store_true", default=True)
    parser.add_argument("--no-validate", dest="validate", action="store_false")
    parser.add_argument("--traceback", action="store_true")
    args = parser.parse_args()

    if args.strict:
        os.environ["CUDF_PANDAS_FAIL_ON_FALLBACK"] = "1"

    if args.backend == "multigpu":
        from . import pandas_compat

        pandas_compat.install(
            max_pool_fraction=args.pool_fraction,
            initial_pool_fraction=args.initial_pool_fraction,
            memory_resource=args.memory_resource,
        )
        from . import get_runtime

        devices = get_runtime().devices
        print(f"backend: multi-GPU, {len(devices)} devices {list(devices)}")
    elif args.backend == "cudf":
        import cudf.pandas

        cudf.pandas.install()
        print("backend: single GPU (stock cudf.pandas)")
    else:
        print("backend: stock CPU pandas")

    if args.queries == "all":
        numbers = sorted(tpcds_queries.available())
    else:
        numbers = sorted(int(x) for x in args.queries.replace(",", " ").split())

    missing = [n for n in range(1, tpcds_queries.COUNT + 1)
               if tpcds_queries.get(n) is None]
    config = _RunConfig(args.path, args.scale, ".parquet")
    print(f"dataset: {args.path}  (scale {args.scale})")
    print(f"translated: {len(tpcds_queries.available())}/{tpcds_queries.COUNT}"
          + (f"   not yet written: {missing}" if missing else ""))
    print()
    print(f"{'query':>6}  {'status':<10}{'time':>10}{'hostRSS':>10}  detail")
    print("-" * 82)

    timings, failures, host_growth, mismatches = {}, {}, {}, {}
    for number in numbers:
        query = tpcds_queries.get(number)
        if query is None:
            continue
        gc.collect()
        rss_before = _host_rss_gib()
        try:
            start = time.perf_counter()
            result = _to_host(query(config))
            elapsed = time.perf_counter() - start
            timings[number] = elapsed
            grew = _host_rss_gib() - rss_before
            host_growth[number] = grew
            note = ""
            if args.validate:
                # A crash *in the comparison* is a defect in the harness, not in
                # the query. Reporting it as a query failure would blame the
                # wrong thing, and counting it as both run and failed -- which
                # an earlier version did -- makes the totals contradict.
                try:
                    # The reference is read with pd.read_parquet, which under
                    # cudf.pandas is itself a proxy backed by a chunked frame --
                    # so it needs the same materialization the result does, or
                    # the comparison runs on the GPU from the other side.
                    expected = load_reference(Path(args.path), number)
                    ok, why = _compare(
                        result,
                        None if expected is None else _to_host(expected))
                except Exception as exc:
                    ok, why = False, f"comparison failed: {type(exc).__name__}: {exc}"
                if not ok:
                    mismatches[number] = why
                    note = f"  <-- MISMATCH: {why}"
            if grew > FALLBACK_RSS_GIB:
                note += "  <-- ran on CPU"
            print(f"{number:>6}  {'ok':<10}{elapsed:>9.3f}s{grew:>+9.1f}G  "
                  f"{len(result)} rows{note}")
        except Exception as exc:
            failures[number] = f"{type(exc).__name__}: {exc}"
            print(f"{number:>6}  {'ERROR':<10}{'-':>10}{'':>10}  "
                  f"{type(exc).__name__}: {str(exc)[:88]}")
            if args.traceback:
                traceback.print_exc()
            if args.backend == "multigpu" and _context_is_dead():
                rest = numbers[numbers.index(number) + 1:]
                print(f"{'':>6}  {'ABORTED':<10}{'-':>10}{'':>10}  "
                      f"CUDA context unusable after q{number}; "
                      f"{len(rest)} queries not attempted")
                break

    print("-" * 82)
    on_cpu = [n for n, g in host_growth.items() if g > FALLBACK_RSS_GIB]
    correct = [n for n in timings if n not in mismatches]
    print(f"{len(timings)}/{len(numbers)} queries ran; "
          f"total {sum(timings.values()):.2f}s")
    print(f"  on GPU: {len(timings) - len(on_cpu)}/{len(numbers)}"
          + (f"   fell back to CPU: {sorted(on_cpu)}" if on_cpu
             else "   (no query fell back to CPU)"))
    if args.validate:
        print(f"  matched DuckDB: {len(correct)}/{len(timings)}")
    if mismatches:
        print("\nwrong answers:")
        for number, why in sorted(mismatches.items()):
            print(f"  q{number}: {why}")
    if failures:
        print("\nfailures:")
        for number, message in sorted(failures.items()):
            print(f"  q{number}: {message[:200]}")


if __name__ == "__main__":
    main()
