# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Collect every benchmark run into one machine-readable file.

    python -m cudf.multigpu.export_results --out /raid/pgali/benchmark_results

Writes ``<out>.json`` (full detail, per query) and ``<out>.csv`` (one row per
query, for anything that would rather not parse nested JSON).

The runs themselves live in ~20 separate logs whose format is meant for a human
watching a terminal. Anything that wants to plot or re-analyse them should read
this instead, so the log format stays free to change and the caveats travel with
the numbers rather than living in someone's memory.

Two fields deserve care from any consumer:

``on_cpu``
    True means cudf.pandas silently computed that query on the host. The answer
    is still right and the time is still real, so it belongs in a total -- but a
    configuration's "on GPU" count is the honest measure of whether the GPU was
    used at all, and at TPC-DS SF100 on one GPU that is 32 of 99.

``validated``
    Whether the answer was checked against DuckDB's answer to the official SQL.
    False above TPC-DS SF300 because DuckDB cannot compute the references on
    this machine. An unvalidated run measures speed, not correctness.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from pathlib import Path

TPCH = Path("/raid/pgali/tpch")
TPCDS = Path("/raid/pgali/tpcds")

#: (benchmark, scale, config, allocator, validated, path)
RUNS = [
    # ---- TPC-H, 22 queries -------------------------------------------
    ("tpch", 1, "1gpu", "default", True, TPCH / "1gpu_sf1.log"),
    ("tpch", 100, "1gpu", "default", True, TPCH / "1gpu_sf100.log"),
    ("tpch", 300, "1gpu", "default", True, TPCH / "1gpu_sf300.log"),
    ("tpch", 1, "8gpu", "pool", True, TPCH / "final_sf1_pool.log"),
    ("tpch", 100, "8gpu", "pool", True, TPCH / "final_sf100_pool.log"),
    ("tpch", 300, "8gpu", "pool", True, TPCH / "final_sf300_pool.log"),
    ("tpch", 500, "8gpu", "pool", True, TPCH / "final_sf500_pool.log"),
    ("tpch", 1000, "8gpu", "pool", True, TPCH / "final_sf1000_pool.log"),
    ("tpch", 500, "8gpu", "managed", True, TPCH / "final_sf500_managed.log"),
    ("tpch", 1000, "8gpu", "managed", True, TPCH / "final_sf1000_managed.log"),
    # ---- TPC-DS, 99 queries ------------------------------------------
    ("tpcds", 1, "cpu", "none", True, TPCDS / "final_sf1_pandas.log"),
    ("tpcds", 10, "cpu", "none", True, TPCDS / "final_sf10_pandas.log"),
    ("tpcds", 1, "1gpu", "default", True, TPCDS / "final_sf1_cudf.log"),
    ("tpcds", 10, "1gpu", "default", True, TPCDS / "final_sf10_cudf.log"),
    ("tpcds", 100, "1gpu", "default", True,
     TPCDS / "final_sf100_cudf_MERGED.json"),
    ("tpcds", 1, "8gpu", "pool", True, TPCDS / "final_sf1_multigpu.log"),
    ("tpcds", 10, "8gpu", "pool", True, TPCDS / "final_sf10_multigpu.log"),
    ("tpcds", 100, "8gpu", "pool", True, TPCDS / "final_sf100_multigpu.log"),
    ("tpcds", 300, "8gpu", "pool", True, TPCDS / "final_sf300_multigpu.log"),
    ("tpcds", 500, "8gpu", "pool", False, TPCDS / "final_sf500_multigpu.log"),
    ("tpcds", 1000, "8gpu", "pool", False,
     TPCDS / "final_sf1000_multigpu.log"),
    # only the 3 queries the pool could not do; not a full-suite run
    ("tpcds", 1000, "8gpu", "managed", False,
     TPCDS / "sf1000_managed_retry.log"),
]

#: on-disk Parquet size, GB
DATASET_GB = {
    ("tpch", 1): 0.3, ("tpch", 100): 26, ("tpch", 300): 78,
    ("tpch", 500): 174, ("tpch", 1000): 347,
    ("tpcds", 1): 0.266, ("tpcds", 10): 2.6, ("tpcds", 100): 25,
    ("tpcds", 300): 74, ("tpcds", 500): 116, ("tpcds", 1000): 243,
}

QUERY_COUNT = {"tpch": 22, "tpcds": 99}

CAVEATS = [
    "Single-GPU totals include queries cudf.pandas silently ran on the CPU. "
    "The answers are right and the times are real, but 'on_gpu' is the honest "
    "measure of whether the GPU did the work.",
    "Comparisons between configurations should use only the queries BOTH "
    "completed; otherwise a configuration is credited for time it saved by "
    "failing.",
    "TPC-DS above SF300 has no DuckDB reference (DuckDB exhausted 1.7 TiB of "
    "spill space on one SF300 query), so validated=false there.",
    "TPC-DS q23 at SF500 fails on libcudf's 2^31-1 rows-per-column limit, not "
    "on memory. No allocator fixes it.",
    "TPC-DS q2 at SF1000 fell back to the CPU without --strict noticing: "
    "CUDF_PANDAS_FAIL_ON_FALLBACK only guards the function-call path, not "
    "attribute access. The host-RSS heuristic is what caught it.",
    "TPC-DS q18/q65/q71/q73 report 'tie order differs': their ORDER BY does "
    "not fully determine row order, and DuckDB itself returns different "
    "orderings across identical runs. Counted as matching.",
    "A run with partial_run=true covers only a subset of the suite -- the "
    "TPC-DS SF1000 managed entry is a retry of the 3 queries the pool could "
    "not complete, so its total is not comparable to a full-suite total.",
    "The single-GPU TPC-DS SF100 run was assembled from three passes; the "
    "first used a 900s per-query cap that turned 9 healthy queries into "
    "'timeouts'. Only the uncapped numbers are exported here.",
]


def _parse_log(path: Path) -> dict:
    """-> {query: {status, seconds, host_gb, on_cpu, note|detail}}"""
    rows: dict[int, dict] = {}
    for line in path.read_text(errors="replace").splitlines():
        m = re.match(r"^ *(\d+) +ok +([\d.]+)s +([+-][\d.]+)G +(\d+) rows(.*)",
                     line)
        if m:
            tail = m.group(5)
            rows[int(m.group(1))] = {
                "status": "ok",
                "seconds": float(m.group(2)),
                "host_rss_delta_gb": float(m.group(3)),
                "result_rows": int(m.group(4)),
                "on_cpu": "ran on CPU" in tail,
                "tie_order_differs": "tie order differs" in tail,
            }
            continue
        m = re.match(r"^ *(\d+) +ERROR +\S+ +(.*)", line)
        if m:
            rows[int(m.group(1))] = {
                "status": "error",
                "detail": m.group(2).strip()[:200],
            }
    return rows


def _parse_json(path: Path) -> dict:
    data = json.loads(path.read_text())
    out = {}
    for k, v in data["per_query"].items():
        if v["status"] == "ok":
            out[int(k)] = {
                "status": "ok", "seconds": v["seconds"],
                "host_rss_delta_gb": v.get("host_gb"),
                "on_cpu": v["on_cpu"], "tie_order_differs": False,
            }
        else:
            out[int(k)] = {"status": "error", "detail": v.get("detail", "")}
    return out


def collect() -> dict:
    runs = []
    for benchmark, scale, config, allocator, validated, path in RUNS:
        if not os.path.exists(path):
            continue
        queries = (_parse_json(path) if str(path).endswith(".json")
                   else _parse_log(path))
        if not queries:
            continue
        ok = [q for q, v in queries.items() if v["status"] == "ok"]
        if config == "cpu":
            # Everything ran on the CPU by definition. The host-RSS heuristic
            # detects a *fallback*, which is not a meaningful question when
            # there was never a GPU involved -- left alone it reported "99 on
            # GPU" for a stock-pandas run.
            for q in ok:
                queries[q]["on_cpu"] = True
        on_cpu = [q for q in ok if queries[q]["on_cpu"]]
        runs.append({
            "benchmark": benchmark,
            "scale_factor": scale,
            "dataset_gb": DATASET_GB.get((benchmark, scale)),
            "config": config,
            "allocator": allocator,
            "validated_against_duckdb": validated,
            "queries_in_suite": QUERY_COUNT[benchmark],
            "queries_attempted": len(queries),
            "queries_ok": len(ok),
            "queries_on_gpu": len(ok) - len(on_cpu),
            "queries_on_cpu": len(on_cpu),
            "queries_errored": len(queries) - len(ok),
            "total_seconds": round(sum(queries[q]["seconds"] for q in ok), 2),
            "partial_run": len(queries) < QUERY_COUNT[benchmark],
            "source_log": str(path),
            "per_query": {str(q): queries[q] for q in sorted(queries)},
        })
    return {
        "machine": {
            "gpus": 8, "gpu_model": "RTX PRO 6000",
            "gpu_memory_gib_each": 97, "gpu_memory_gib_total": 776,
            "host_ram_gb": 2267,
        },
        "config_meanings": {
            "cpu": "stock pandas, no GPU",
            "1gpu": "stock cudf.pandas on a single GPU",
            "8gpu": "cudf.multigpu across all 8 GPUs",
        },
        "caveats": CAVEATS,
        "runs": runs,
    }


def write_csv(data: dict, path: Path) -> None:
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "benchmark", "scale_factor", "dataset_gb", "config", "allocator",
            "validated", "query", "status", "seconds", "on_cpu",
            "host_rss_delta_gb", "result_rows", "detail",
        ])
        for run in data["runs"]:
            for q, v in run["per_query"].items():
                writer.writerow([
                    run["benchmark"], run["scale_factor"], run["dataset_gb"],
                    run["config"], run["allocator"],
                    run["validated_against_duckdb"], q, v["status"],
                    v.get("seconds", ""), v.get("on_cpu", ""),
                    v.get("host_rss_delta_gb", ""), v.get("result_rows", ""),
                    v.get("detail", ""),
                ])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="benchmark_results")
    args = parser.parse_args()

    data = collect()
    Path(f"{args.out}.json").write_text(json.dumps(data, indent=1))
    write_csv(data, Path(f"{args.out}.csv"))

    print(f"{len(data['runs'])} runs exported\n")
    print(f"{'benchmark':<8}{'SF':>6}{'config':>10}{'alloc':>9}"
          f"{'ok':>7}{'onGPU':>7}{'onCPU':>7}{'err':>5}{'total s':>10}  valid")
    for r in data["runs"]:
        print(f"{r['benchmark']:<8}{r['scale_factor']:>6}{r['config']:>10}"
              f"{r['allocator']:>9}"
              f"{r['queries_ok']:>4}/{r['queries_in_suite']:<2}"
              f"{r['queries_on_gpu']:>7}{r['queries_on_cpu']:>7}"
              f"{r['queries_errored']:>5}{r['total_seconds']:>10.1f}"
              f"  {'yes' if r['validated_against_duckdb'] else 'NO'}"
              f"{'   PARTIAL' if r['partial_run'] else ''}")
    print(f"\nwrote {args.out}.json and {args.out}.csv")


if __name__ == "__main__":
    main()
