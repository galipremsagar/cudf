# Copyright (c) 2020-2021, NVIDIA CORPORATION.

import sys

import pandas as pd

import cudf
from cudf._fuzz_testing.main import pythonfuzz
from cudf._fuzz_testing.parquet import ParquetReader
from cudf._fuzz_testing.utils import compare_dataframe, run_test


@pythonfuzz(data_handle=ParquetReader, crash_reports_dir="./parquet_crashes/")
def parquet_reader_test_with_bug(parquet_buffer):
    pdf = pd.read_parquet(parquet_buffer, use_nullable_dtypes=True)
    gdf = cudf.read_parquet(parquet_buffer)

    # Let's Introduce a BUG and compare!
    pdf = pdf.drop(0)

    compare_dataframe(gdf, pdf)


@pythonfuzz(
    data_handle=ParquetReader, regression=True, dir=["./parquet_crashes/"]
)
def parquet_reader_test(parquet_buffer):
    pdf = pd.read_parquet(parquet_buffer, use_nullable_dtypes=True)
    gdf = cudf.read_parquet(parquet_buffer)

    compare_dataframe(gdf, pdf)


if __name__ == "__main__":
    run_test(globals(), sys.argv)
