# Copyright (c) 2020-2021, NVIDIA CORPORATION.

import sys

import pandas as pd

import cudf
from cudf._fuzz_testing.main import pythonfuzz
from cudf._fuzz_testing.parquet import ParquetReader
from cudf._fuzz_testing.utils import (
    ALL_POSSIBLE_VALUES,
    compare_dataframe,
    run_test,
)


@pythonfuzz(
    data_handle=ParquetReader,
    params={
        "columns": ALL_POSSIBLE_VALUES,
        "use_pandas_metadata": [True, False],
        "skiprows": ALL_POSSIBLE_VALUES,
        "num_rows": ALL_POSSIBLE_VALUES,
    },
    crash_reports_dir="./parquet_crashes_with_params/",
)
def parquet_reader_columns_with_bug(
    parquet_buffer, columns, use_pandas_metadata, skiprows, num_rows
):
    pdf = pd.read_parquet(
        parquet_buffer,
        columns=columns,
        use_pandas_metadata=use_pandas_metadata,
    )

    # Again, Lets introduce a BUG!
    # pdf = pdf.iloc[skiprows:]
    if num_rows is not None:
        pdf = pdf.head(num_rows)

    gdf = cudf.read_parquet(
        parquet_buffer,
        columns=columns,
        use_pandas_metadata=use_pandas_metadata,
        skiprows=skiprows,
        num_rows=num_rows,
    )

    compare_dataframe(gdf, pdf)


@pythonfuzz(
    data_handle=ParquetReader,
    params={
        "columns": ALL_POSSIBLE_VALUES,
        "use_pandas_metadata": [True, False],
        "skiprows": ALL_POSSIBLE_VALUES,
        "num_rows": ALL_POSSIBLE_VALUES,
    },
    regression=True,
    dir=["./parquet_crashes_with_params/"],
)
def parquet_reader_columns(
    parquet_buffer, columns, use_pandas_metadata, skiprows, num_rows
):
    pdf = pd.read_parquet(
        parquet_buffer,
        columns=columns,
        use_pandas_metadata=use_pandas_metadata,
    )

    pdf = pdf.iloc[skiprows:]
    if num_rows is not None:
        pdf = pdf.head(num_rows)

    gdf = cudf.read_parquet(
        parquet_buffer,
        columns=columns,
        use_pandas_metadata=use_pandas_metadata,
        skiprows=skiprows,
        num_rows=num_rows,
    )

    compare_dataframe(gdf, pdf)


if __name__ == "__main__":
    run_test(globals(), sys.argv)
