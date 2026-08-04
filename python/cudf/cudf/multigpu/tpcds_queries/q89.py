# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 89. Monthly store sales in selected classes that stray from their brand's yearly average."""

from __future__ import annotations

import pandas as pd

_GROUP = [
    "i_category",
    "i_class",
    "i_brand",
    "s_store_name",
    "s_company_name",
    "d_moy",
]
_PARTITION = ["i_category", "i_brand", "s_store_name", "s_company_name"]


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    item = pd.read_parquet(
        f"{base}/item{suffix}",
        columns=["i_item_sk", "i_category", "i_class", "i_brand"],
    )
    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=["ss_item_sk", "ss_sold_date_sk", "ss_store_sk", "ss_sales_price"],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    store = pd.read_parquet(
        f"{base}/store{suffix}",
        columns=["s_store_sk", "s_store_name", "s_company_name"],
    )

    item = item[
        (
            item["i_category"].isin(["Books", "Electronics", "Sports"])
            & item["i_class"].isin(["computers", "stereo", "football"])
        )
        | (
            item["i_category"].isin(["Men", "Jewelry", "Women"])
            & item["i_class"].isin(["shirts", "birdal", "dresses"])
        )
    ]
    dates = date_dim[date_dim["d_year"] == 1999][["d_date_sk", "d_moy"]]

    frame = store_sales.merge(item, left_on="ss_item_sk", right_on="i_item_sk")
    frame = frame.merge(dates, left_on="ss_sold_date_sk", right_on="d_date_sk")
    frame = frame.merge(store, left_on="ss_store_sk", right_on="s_store_sk")

    grouped = frame.groupby(_GROUP, as_index=False, dropna=False)[
        "ss_sales_price"
    ].sum(min_count=1)
    grouped = grouped.rename(columns={"ss_sales_price": "sum_sales"})
    grouped["sales"] = grouped["sum_sales"].astype("float64")
    grouped["avg_monthly_sales"] = grouped.groupby(_PARTITION, dropna=False)[
        "sales"
    ].transform("mean")

    deviation = (grouped["sales"] - grouped["avg_monthly_sales"]).abs() / grouped[
        "avg_monthly_sales"
    ]
    grouped = grouped[(grouped["avg_monthly_sales"] != 0) & (deviation > 0.1)]

    grouped = grouped.assign(
        delta=grouped["sales"] - grouped["avg_monthly_sales"]
    )
    grouped = grouped.sort_values(
        [
            "delta",
            "s_store_name",
            "i_category",
            "i_class",
            "i_brand",
            "s_company_name",
            "d_moy",
            "sales",
            "avg_monthly_sales",
        ],
        na_position="last",
    )

    result = grouped[
        [
            "i_category",
            "i_class",
            "i_brand",
            "s_store_name",
            "s_company_name",
            "d_moy",
            "sum_sales",
            "avg_monthly_sales",
        ]
    ]
    return result.head(100).reset_index(drop=True)
