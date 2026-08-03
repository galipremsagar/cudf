# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Demonstrate a DataFrame larger than any single GPU.

    python -m cudf.multigpu.demo --gib 300

Builds a synthetic frame directly on every GPU (never assembling it anywhere),
proves it cannot fit on one device, then runs real cuDF operations over it.
"""

from __future__ import annotations

import argparse
import time
from contextlib import contextmanager

import numpy as np

import cudf
import cudf.multigpu as mgpu

GIB = 1 << 30
#: int32 key + int32 key2 + N float64 value columns
VALUE_COLUMNS = 4
BYTES_PER_ROW = 4 + 4 + 8 * VALUE_COLUMNS


@contextmanager
def timed(label: str):
    start = time.perf_counter()
    yield
    print(f"    {label:<44s} {time.perf_counter() - start:8.2f}s")


def make_chunk(index: int, nchunks: int, rows: int, groups: int):
    """Generate one chunk *on the GPU that will own it*.

    Uses CuPy, whose allocator cuDF routes through RMM, so the data is born in
    this device's pool and is never staged through host memory or another GPU.
    """
    import cupy as cp

    rng = cp.random.default_rng(1234 + index)
    data = {
        "key": rng.integers(0, groups, rows, dtype=cp.int32),
        "key2": rng.integers(0, 8, rows, dtype=cp.int32),
    }
    for c in range(VALUE_COLUMNS):
        data[f"v{c}"] = rng.random(rows, dtype=cp.float64)
    return cudf.DataFrame(data)


def report_placement(frame, label: str) -> None:
    usage = frame.memory_usage_per_device()
    total = sum(usage.values())
    print(f"    {label}: {total / GIB:8.1f} GiB over {len(usage)} GPUs")
    print(
        "      "
        + "  ".join(f"GPU{d}={v / GIB:.1f}G" for d, v in sorted(usage.items()))
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gib", type=float, default=200.0,
                        help="approximate total size of the frame in GiB")
    parser.add_argument("--chunks-per-device", type=int, default=1)
    parser.add_argument("--groups", type=int, default=1_000_000,
                        help="number of distinct group-by keys")
    parser.add_argument("--devices", type=str, default=None,
                        help="comma separated device ordinals")
    parser.add_argument("--pool-fraction", type=float, default=0.90,
                        help="max RMM pool size as a fraction of each GPU")
    parser.add_argument("--initial-pool-fraction", type=float, default=0.05,
                        help="pre-reserved RMM pool size per GPU; raising this "
                             "avoids fragmentation from repeated pool growth")
    args = parser.parse_args()

    devices = (
        [int(x) for x in args.devices.split(",")] if args.devices else None
    )
    runtime = mgpu.init(
        devices=devices,
        max_pool_fraction=args.pool_fraction,
        initial_pool_fraction=args.initial_pool_fraction,
    )
    ndev = runtime.n_devices
    per_device = {d: t for d, (_f, t) in runtime.memory_info().items()}
    one_gpu = min(per_device.values()) / GIB

    print("=" * 72)
    print("cudf.multigpu -- one DataFrame across many GPUs")
    print("=" * 72)
    print(f"  devices              : {list(runtime.devices)}")
    print(f"  memory per GPU       : {one_gpu:.1f} GiB")
    print(f"  aggregate GPU memory : {sum(per_device.values()) / GIB:.1f} GiB")

    nchunks = ndev * args.chunks_per_device
    total_rows = int(args.gib * GIB / BYTES_PER_ROW)
    rows_per_chunk = total_rows // nchunks
    print(f"  target frame size    : {args.gib:.1f} GiB "
          f"({total_rows / 1e9:.2f} B rows, {nchunks} chunks)")
    print(f"  -> {args.gib / one_gpu:.2f}x a single GPU's total memory\n")

    print("[1] Building the frame directly on each GPU")
    with timed("build"):
        df = mgpu.build(
            nchunks, make_chunk, rows=rows_per_chunk, groups=args.groups
        )
    report_placement(df, "resident")

    print("\n[2] Can this fit on one GPU?")
    print(f"    frame is {df.nbytes / GIB:.1f} GiB; one GPU has {one_gpu:.1f} GiB "
          f"-> {'NO' if df.nbytes / GIB > one_gpu else 'yes'}")
    if df.nbytes / GIB > one_gpu:
        try:
            df.compute(device=runtime.devices[0])
            print("    unexpected: gathering onto one GPU succeeded")
        except Exception as exc:
            print(f"    gathering onto GPU {runtime.devices[0]} fails as expected:")
            print(f"      {type(exc).__name__}: {str(exc)[:110]}")

    print("\n[3] Elementwise + filter (embarrassingly parallel, no movement)")
    # Written the natural way, each sub-expression materializes across every
    # GPU before the next one runs: `a * b + c` costs two full-width
    # intermediates. At 500 GiB that is 200+ GiB of temporaries.
    with timed("count((v0 + v1) > 0.5)   [1 intermediate]"):
        kept = int(((df["v0"] + df["v1"]) > 0.5).sum())
    print(f"    rows passing filter: {kept / 1e9:.3f} B of {len(df) / 1e9:.3f} B")

    # map_chunks evaluates the whole expression inside one chunk at a time, so
    # the intermediates are freed per chunk instead of per frame. Same answer,
    # a fraction of the peak memory -- this is the idiom for expressions that
    # would otherwise not fit.
    with timed("count((v0 * v1 + v2) > 0.25)  [fused per chunk]"):
        fused = int(
            df.map_chunks(
                lambda c: (c["v0"] * c["v1"] + c["v2"]) > 0.25
            ).sum()
        )
    print(f"    rows passing fused filter: {fused / 1e9:.3f} B")

    print("\n[4] Reductions (per-GPU partials, combined on host)")
    with timed("v0.sum() / mean / std"):
        total = df["v0"].sum()
        mean = df["v0"].mean()
        std = df["v0"].std()
    print(f"    sum={total:.6e}  mean={mean:.8f}  std={std:.8f}")
    print("    (uniform[0,1) => mean ~0.5, std ~0.288675)")

    print("\n[5] group-by (local pre-aggregate, shuffle partials, combine)")
    with timed(f"groupby('key').agg(sum,mean) over {args.groups:,} groups"):
        grouped = df.groupby("key").agg({"v0": "sum", "v1": "mean"})
        n_groups = len(grouped)
    print(f"    distinct groups: {n_groups:,}")
    report_placement(grouped, "result")

    print("\n[6] Join against a dimension table (broadcast)")
    import pandas as pd

    dim = mgpu.from_pandas(
        pd.DataFrame({"key2": np.arange(8, dtype="int32"),
                      "weight": np.arange(8, dtype="float64") / 8}),
        npartitions=1,
    )
    # The dimension table is tiny, so it is replicated to every GPU and each
    # chunk joins locally -- no shuffle of the 500 GiB side. The join output is
    # consumed inside map_chunks so the widened rows never all exist at once.
    with timed("broadcast merge on key2, then weighted sum"):
        products = df.map_chunks(
            lambda c, d: c[["key2", "v0"]].merge(d, on="key2").eval(
                "v0 * weight"
            ),
            broadcast=[dim],
        )
        weighted = float(products.sum())
        joined_rows = len(products)
    print(f"    joined rows: {joined_rows / 1e9:.3f} B, weighted sum = {weighted:.6e}")
    del products

    print("\n[7] Final memory picture")
    info = runtime.memory_info()
    for d, (free, tot) in sorted(info.items()):
        used = (tot - free) / GIB
        bar = "#" * int(30 * used / (tot / GIB))
        print(f"    GPU{d}  {used:6.1f}/{tot / GIB:.0f} GiB  |{bar:<30s}|")
    print("\ndone.")


if __name__ == "__main__":
    main()
