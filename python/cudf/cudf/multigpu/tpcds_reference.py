# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Reference answers for TPC-DS, produced by DuckDB from the official SQL.

    python -m cudf.multigpu.tpcds_reference --path /raid/pgali/tpcds/sf1

A pandas translation of a TPC-DS query is only worth having if it computes what
the query says. DuckDB ships the official statements and reads the same Parquet,
so it can answer each one directly -- which makes every translation checkable
against the thing it was translated from, rather than against a reading of the
SQL by whoever wrote the pandas.

Answers are written to ``<path>/reference/qN.parquet``. Queries whose SQL DuckDB
itself cannot run are recorded as skipped rather than silently omitted, so a
missing reference is never mistaken for a passing translation.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

__all__ = ["build", "load"]


def build(path: Path, memory_limit: str = "64GB") -> dict:
    import duckdb

    out = path / "reference"
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET temp_directory='{path / '_spill'}'")
    con.execute("INSTALL tpcds; LOAD tpcds;")

    # Views over the Parquet, so the official SQL runs unmodified.
    for parquet in sorted(path.glob("*.parquet")):
        con.execute(
            f"CREATE OR REPLACE VIEW {parquet.stem} AS "
            f"SELECT * FROM read_parquet('{parquet}')"
        )

    status: dict[str, str] = {}
    for number, sql in con.execute(
        "SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr"
    ).fetchall():
        target = out / f"q{number}.parquet"
        start = time.perf_counter()
        try:
            con.execute(
                f"COPY ({sql.rstrip().rstrip(';')}) TO '{target}' "
                "(FORMAT PARQUET)"
            )
            rows = con.execute(
                f"SELECT count(*) FROM read_parquet('{target}')"
            ).fetchone()[0]
            status[str(number)] = "ok"
            print(f"  q{number:<3} {rows:>8,} rows "
                  f"{time.perf_counter() - start:>7.2f}s", flush=True)
        except Exception as exc:  # noqa: BLE001 - reported, not swallowed
            # A failed COPY leaves a truncated file behind. Left in place it
            # looks like a reference that exists, and every later comparison
            # against it dies reading it -- which reads as the *query* being
            # broken. q85 at SF300 did exactly this after DuckDB exhausted its
            # spill space.
            target.unlink(missing_ok=True)
            status[str(number)] = f"{type(exc).__name__}: {exc}"
            print(f"  q{number:<3} SKIPPED  {type(exc).__name__}: "
                  f"{str(exc)[:90]}", flush=True)

    (out / "status.json").write_text(json.dumps(status, indent=2))
    ok = sum(1 for v in status.values() if v == "ok")
    print(f"\n{ok}/{len(status)} reference answers written to {out}")
    return status


def load(path: Path, number: int):
    """The reference answer for query ``number``, or None if there isn't one."""
    import pandas as pd

    target = Path(path) / "reference" / f"q{number}.parquet"
    if not target.exists() or target.stat().st_size == 0:
        return None
    try:
        return pd.read_parquet(target)
    except Exception:
        # An unreadable reference is a missing reference. Reporting it as a
        # comparison failure blames the query for the harness's problem.
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--memory-limit", default="64GB")
    args = parser.parse_args()
    build(args.path, args.memory_limit)


if __name__ == "__main__":
    main()
