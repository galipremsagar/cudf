# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Run cuDF's official PDS-H (TPC-H) queries on the multi-GPU backend.

Uses the query definitions that ship with cuDF
(``cudf.pandas._benchmarks.pdsh``) unmodified, so this measures the same
workload as ``python -m cudf.pandas._benchmarks.pdsh``, just with every table
spread across all GPUs instead of living on one.

    # data must be converted first (decimal -> float64, date -> timestamp):
    #   python -m cudf.multigpu.tpch_convert --src .../sf100 --dst .../sf100c

    python -m cudf.multigpu.tpch_pdsh --path .../sf100c
    python -m cudf.multigpu.tpch_pdsh --path .../sf1c --validate

A query that raises is a missing multi-GPU API, and the traceback names it.
``--strict`` additionally turns any silent fall-back-to-pandas into an error,
which matters here: a fallback copies the whole frame to host memory, so on a
dataset that only fits in aggregate GPU memory it is fatal rather than slow.
"""

from __future__ import annotations

import argparse
import contextlib
import gc
import os
import signal
import time
import traceback
from typing import Any

GIB = 1 << 30


class _RunConfig:
    """The three attributes the PDS-H query bodies actually read."""

    def __init__(self, dataset_path: str, scale_factor: float, suffix: str):
        self.dataset_path = dataset_path
        self.scale_factor = scale_factor
        self.suffix = suffix


def _report_placement(label: str = "tables") -> None:
    import cudf.multigpu as mgpu

    runtime = mgpu.get_runtime()
    info = runtime.memory_info()
    used = {d: (total - free) for d, (free, total) in info.items()}
    total_used = sum(used.values()) / GIB
    print(
        f"  {label}: {total_used:.2f} GiB resident across "
        f"{len(used)} GPUs -> "
        + ", ".join(f"GPU{d}={v / GIB:.1f}G" for d, v in sorted(used.items()))
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True)
    parser.add_argument("--queries", default="all")
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--suffix", default=".parquet")
    parser.add_argument("--npartitions", type=int, default=None)
    parser.add_argument("--devices", default=None)
    parser.add_argument("--pool-fraction", type=float, default=0.90)
    parser.add_argument("--initial-pool-fraction", type=float, default=0.05)
    parser.add_argument("--memory-resource", default="pool",
                        choices=["pool", "async"],
                        help="'async' returns memory to the driver between "
                             "queries; needed at large scale factors")
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--strict", action="store_true",
                        help="turn any pandas fallback into an error")
    parser.add_argument("--validate", action="store_true",
                        help="compare each result against single-GPU cudf.pandas")
    parser.add_argument("--traceback", action="store_true")
    parser.add_argument("--timeout", type=int, default=0,
                        help="per-query timeout in seconds (0 = none)")
    parser.add_argument("--print-results", action="store_true")
    args = parser.parse_args()

    if args.strict:
        os.environ["CUDF_PANDAS_FAIL_ON_FALLBACK"] = "1"

    # Must precede any import of pandas or the benchmark module.
    import cudf.multigpu.pandas_compat as pandas_compat

    devices = [int(x) for x in args.devices.split(",")] if args.devices else None
    pandas_compat.install(
        devices=devices,
        npartitions=args.npartitions,
        max_pool_fraction=args.pool_fraction,
        initial_pool_fraction=args.initial_pool_fraction,
        memory_resource=args.memory_resource,
    )

    import cudf.multigpu as mgpu

    runtime = mgpu.get_runtime()
    print(f"multi-GPU backend: {runtime.n_devices} devices {list(runtime.devices)}")
    print(f"dataset: {args.path}  (scale {args.scale})")

    from cudf.pandas._benchmarks.pdsh import PDSHQueries

    numbers = (
        list(range(1, 23))
        if args.queries == "all"
        else _parse_queries(args.queries)
    )
    config = _RunConfig(args.path, args.scale, args.suffix)

    results: dict[int, Any] = {}
    timings: dict[int, float] = {}
    failures: dict[int, str] = {}

    print(f"\n{'query':>6}  {'status':<10}{'time':>10}  detail")
    print("-" * 78)
    for number in numbers:
        # Chunked frames form reference cycles (indexers and accessors hold the
        # frame), so without an explicit collection the previous query's device
        # memory is still held when the next one starts. At 300 GB that is the
        # difference between finishing and running out.
        gc.collect()
        query = getattr(PDSHQueries, f"q{number}")
        best = None
        try:
            with _deadline(args.timeout):
              for _ in range(args.iterations):
                start = time.perf_counter()
                result = query(config)
                # force materialization before stopping the clock
                host = _to_host(result)
                elapsed = time.perf_counter() - start
                best = elapsed if best is None else min(best, elapsed)
            results[number] = host
            timings[number] = best
            print(f"{number:>6}  {'ok':<10}{best:>9.3f}s  {len(host)} rows")
            if args.print_results:
                print(host.head(5).to_string())
        except Exception as exc:
            failures[number] = f"{type(exc).__name__}: {exc}"
            print(f"{number:>6}  {'ERROR':<10}{'-':>10}  "
                  f"{type(exc).__name__}: {str(exc)[:100]}")
            if args.traceback:
                traceback.print_exc()

    print("-" * 78)
    ok = len(timings)
    print(f"{ok}/{len(numbers)} queries ran; "
          f"total {sum(timings.values()):.2f}s")
    _report_placement("device memory")

    if failures:
        print("\nfailures (each one is a multi-GPU API to implement):")
        for number, message in sorted(failures.items()):
            print(f"  q{number}: {message[:220]}")

    if args.validate and results:
        _validate(args, numbers, results)


class _Timeout(Exception):
    pass


@contextlib.contextmanager
def _deadline(seconds: int):
    """Bound a query's wall time so one hang does not block the rest."""
    if not seconds:
        yield
        return

    def _fire(signum, frame):
        raise _Timeout(f"exceeded {seconds}s")

    previous = signal.signal(signal.SIGALRM, _fire)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def _parse_queries(text: str) -> list[int]:
    numbers: list[int] = []
    for piece in text.split(","):
        piece = piece.strip()
        if "-" in piece:
            low, high = piece.split("-")
            numbers.extend(range(int(low), int(high) + 1))
        else:
            numbers.append(int(piece))
    return numbers


def _to_host(result):
    import pandas as pd

    if hasattr(result, "to_pandas"):
        result = result.to_pandas()
    if isinstance(result, pd.Series):
        result = result.to_frame()
    return result.reset_index(drop=True)


def _validate(args, numbers, results) -> None:
    """Re-run on stock single-GPU cudf.pandas in a subprocess and compare."""
    import json
    import pickle
    import subprocess
    import sys
    import tempfile

    print("\nvalidating against single-GPU cudf.pandas ...")
    with tempfile.TemporaryDirectory() as tmp:
        target = os.path.join(tmp, "reference.pkl")
        script = _REFERENCE_SCRIPT.format(
            path=repr(args.path),
            scale=args.scale,
            suffix=repr(args.suffix),
            numbers=repr(sorted(results)),
            target=repr(target),
        )
        proc = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True
        )
        if proc.returncode != 0:
            print("  reference run failed:")
            print(proc.stderr[-2000:])
            return
        with open(target, "rb") as handle:
            reference = pickle.load(handle)

    same = differ = 0
    for number in sorted(results):
        expected = reference.get(number)
        if expected is None:
            print(f"  q{number}: reference unavailable")
            continue
        ok, reason = _compare(results[number], expected)
        if ok:
            same += 1
        else:
            differ += 1
            print(f"  q{number}: MISMATCH ({reason})")
    print(f"  {same} match, {differ} differ")


_REFERENCE_SCRIPT = """
import pickle
import cudf.pandas
cudf.pandas.install()
from cudf.pandas._benchmarks.pdsh import PDSHQueries

class C:
    def __init__(self, p, s, x):
        self.dataset_path, self.scale_factor, self.suffix = p, s, x

config = C({path}, {scale}, {suffix})
out = {{}}
for n in {numbers}:
    try:
        r = getattr(PDSHQueries, f"q{{n}}")(config)
        if hasattr(r, "to_pandas"):
            r = r.to_pandas()
        try:
            r = r.to_frame()
        except AttributeError:
            pass
        out[n] = r.reset_index(drop=True)
    except Exception:
        pass
with open({target}, "wb") as f:
    pickle.dump(out, f)
"""


def _compare(got, expected, tol: float = 1e-4) -> tuple[bool, str]:
    import numpy as np
    import pandas as pd

    if got.shape != expected.shape:
        return False, f"shape {got.shape} != {expected.shape}"
    if list(got.columns) != list(expected.columns):
        return False, "column names differ"
    for column in expected.columns:
        left, right = got[column], expected[column]
        if pd.api.types.is_numeric_dtype(right):
            if not np.allclose(
                left.to_numpy(dtype="float64"),
                right.to_numpy(dtype="float64"),
                rtol=tol,
                atol=tol,
                equal_nan=True,
            ):
                return False, f"column {column!r} differs"
        elif not left.astype(str).equals(right.astype(str)):
            return False, f"column {column!r} differs"
    return True, ""


if __name__ == "__main__":
    main()
