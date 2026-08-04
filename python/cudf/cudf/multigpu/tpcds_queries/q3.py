# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 3. November store sales by year and brand for one manufacturer."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=["ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price"],
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}",
        columns=["i_item_sk", "i_brand_id", "i_brand", "i_manufact_id"],
    )

    dates = date_dim[date_dim["d_moy"] == 11][["d_date_sk", "d_year"]]
    items = item[item["i_manufact_id"] == 128][["i_item_sk", "i_brand_id", "i_brand"]]

    joined = store_sales.merge(
        dates, left_on="ss_sold_date_sk", right_on="d_date_sk", how="inner"
    ).merge(items, left_on="ss_item_sk", right_on="i_item_sk", how="inner")

    grouped = (
        joined.groupby(["d_year", "i_brand", "i_brand_id"], dropna=False)[
            "ss_ext_sales_price"
        ]
        .sum()
        .reset_index()
        .rename(columns={"ss_ext_sales_price": "sum_agg"})
    )

    grouped = grouped.sort_values(
        ["d_year", "sum_agg", "i_brand_id"],
        ascending=[True, False, True],
        na_position="first",
        kind="stable",
    )
    result = grouped[["d_year", "i_brand_id", "i_brand", "sum_agg"]].head(100)
    return result.reset_index(drop=True)
