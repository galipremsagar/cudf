import pandas as pd

import cudf
from cudf.testing.main import pythonfuzz
from cudf.testing.parquet import ParquetWriter
from cudf.tests.utils import assert_eq


@pythonfuzz(data_handle=ParquetWriter)
def parquet_writer_test(gdf):
    pd_file_name = "cpu_pdf.parquet"
    gd_file_name = "gpu_pdf.parquet"

    pdf = gdf.to_pandas()

    pdf.to_parquet(pd_file_name)
    gdf.to_parquet(gd_file_name)

    actual = cudf.read_parquet(gd_file_name)
    expected = pd.read_parquet(pd_file_name)
    assert_eq(actual, expected)

    actual = cudf.read_parquet(pd_file_name)
    expected = pd.read_parquet(gd_file_name)
    assert_eq(actual, expected)


if __name__ == "__main__":
    parquet_writer_test()
