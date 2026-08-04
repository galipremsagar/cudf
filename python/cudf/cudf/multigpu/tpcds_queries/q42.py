# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 42.Store sales by item category for November 2000, for items handled by manager 1."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    date_dim = pd.read_parquet(
        f"{run_config.dataset_path}/date_dim{run_config.suffix}",
        columns=["d_date_sk", "d_year", "d_moy"],
    )
    store_sales = pd.read_parquet(
        f"{run_config.dataset_path}/store_sales{run_config.suffix}",
        columns=["ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price"],
    )
    item = pd.read_parquet(
        f"{run_config.dataset_path}/item{run_config.suffix}",
        columns=["i_item_sk", "i_category_id", "i_category", "i_manager_id"],
    )

    date_dim = date_dim[(date_dim["d_moy"] == 11) & (date_dim["d_year"] == 2000)]
    item = item[item["i_manager_id"] == 1]

    merged = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    ).merge(item, left_on="ss_item_sk", right_on="i_item_sk")

    grouped = (
        merged.groupby(["d_year", "i_category_id", "i_category"], dropna=False)[
            "ss_ext_sales_price"
        ]
        .sum()
        .reset_index()
    )

    result = (
        grouped.sort_values(
            ["ss_ext_sales_price", "d_year", "i_category_id", "i_category"],
            ascending=[False, True, True, True],
        )
        .head(100)
        .reset_index(drop=True)
    )
    return result[["d_year", "i_category_id", "i_category", "ss_ext_sales_price"]]
