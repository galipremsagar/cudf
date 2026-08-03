# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Convert tpchgen-cli parquet output into the dtypes the benchmarks expect.

``tpchgen-cli`` emits DECIMAL columns and DATE32 columns.  pandas cannot do
arithmetic on either (it materializes them as ``object`` arrays of
``decimal.Decimal`` and ``datetime.date``), so the PDS-H benchmarks require
them cast to ``float64`` and ``timestamp[ms]`` first.  This mirrors the
conversion described in
``docs/cudf/source/cudf_pandas/benchmarks.md``.

    python -m cudf.multigpu.tpch_convert --src /data/tpch/sf100 \
                                         --dst /data/tpch/sf100c

Streams row group by row group, so converting a 60 GiB table does not need
60 GiB of host memory.
"""

from __future__ import annotations

import argparse
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq


def convert_field(field: pa.Field) -> pa.Field:
    dtype = field.type
    if pa.types.is_decimal(dtype):
        return field.with_type(pa.float64())
    if pa.types.is_date(dtype):
        return field.with_type(pa.timestamp("ms"))
    return field


def convert_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema([convert_field(f) for f in schema])


def convert_file(source: str, target: str, batch_rows: int = 1 << 20) -> tuple[int, float]:
    start = time.perf_counter()
    reader = pq.ParquetFile(source)
    schema = convert_schema(reader.schema_arrow)
    rows = 0
    os.makedirs(os.path.dirname(target), exist_ok=True)
    with pq.ParquetWriter(target, schema, compression="snappy") as writer:
        for batch in reader.iter_batches(batch_size=batch_rows):
            table = pa.Table.from_batches([batch]).cast(schema)
            writer.write_table(table)
            rows += table.num_rows
    return rows, time.perf_counter() - start


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", required=True)
    parser.add_argument("--dst", required=True)
    parser.add_argument("--batch-rows", type=int, default=1 << 20)
    args = parser.parse_args()

    os.makedirs(args.dst, exist_ok=True)
    names = sorted(f for f in os.listdir(args.src) if f.endswith(".parquet"))
    for name in names:
        source = os.path.join(args.src, name)
        target = os.path.join(args.dst, name)
        rows, elapsed = convert_file(source, target, args.batch_rows)
        size = os.path.getsize(target) / (1 << 30)
        print(f"  {name:<22} {rows:>13,} rows  {size:6.2f} GiB  {elapsed:7.1f}s",
              flush=True)
    print("done ->", args.dst)


if __name__ == "__main__":
    main()
