# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Generate TPC-DS data as Parquet, and dump the 99 official queries.

    python -m cudf.multigpu.tpcds_gen --scale 1 --out /raid/pgali/tpcds/sf1

tpchgen-rs, used for PDS-H, is TPC-H only. DuckDB bundles ``dsdgen`` and the
official query set, so it is the generator here: data comes out of ``dsdgen``
into DuckDB tables and is written straight to Parquet, one file per table.

The queries are dumped alongside the data purely as reference SQL. They are not
executable against this project -- the multi-GPU layer sits under the *pandas*
API, so a query only exercises it once it exists as pandas code.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

#: TPC-DS is 24 tables; the big ones are the sales/returns facts.
FACTS = (
    "store_sales", "store_returns", "catalog_sales", "catalog_returns",
    "web_sales", "web_returns", "inventory",
)


def generate(scale: float, out: Path, memory_limit: str = "64GB",
             threads: int | None = None) -> None:
    import duckdb

    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=str(out / "_gen.duckdb"))
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET temp_directory='{out / '_spill'}'")
    if threads:
        # leave cores for anything else running; dsdgen will happily take all
        con.execute(f"SET threads={threads}")
    con.execute("INSTALL tpcds; LOAD tpcds;")

    start = time.perf_counter()
    print(f"dsdgen at scale {scale} ...", flush=True)
    con.execute(f"CALL dsdgen(sf={scale})")
    print(f"  generated in {time.perf_counter() - start:.1f}s", flush=True)

    tables = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' ORDER BY table_name").fetchall()]

    for name in tables:
        path = out / f"{name}.parquet"
        t0 = time.perf_counter()
        con.execute(
            f"COPY (SELECT * FROM {name}) TO '{path}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        rows = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        size = path.stat().st_size / (1 << 30)
        print(f"  {name:<24} {rows:>15,} rows {size:>8.2f} GiB "
              f"{time.perf_counter() - t0:>8.1f}s", flush=True)

    queries = out / "queries"
    queries.mkdir(exist_ok=True)
    for number, sql in con.execute(
            "SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr"
    ).fetchall():
        (queries / f"q{number}.sql").write_text(sql)
    print(f"  wrote 99 reference queries to {queries}", flush=True)

    con.close()
    # the DuckDB file is a build artifact several times the size of the Parquet
    for leftover in (out / "_gen.duckdb", out / "_gen.duckdb.wal"):
        if leftover.exists():
            leftover.unlink()
    print(f"done -> {out}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", type=float, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--memory-limit", default="64GB")
    parser.add_argument("--threads", type=int, default=None)
    args = parser.parse_args()
    generate(args.scale, args.out, args.memory_limit, args.threads)


if __name__ == "__main__":
    main()
