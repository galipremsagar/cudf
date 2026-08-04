# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 63.Which manager/month store sales deviate more than 10 percent
from that manager's average monthly sales, for selected brands."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    item = pd.read_parquet(
        f"{path}/item{suffix}",
        columns=[
            "i_item_sk",
            "i_manager_id",
            "i_category",
            "i_class",
            "i_brand",
        ],
    )
    group_a = (
        item["i_category"].isin(["Books", "Children", "Electronics"])
        & item["i_class"].isin(["personal", "portable", "reference", "self-help"])
        & item["i_brand"].isin(
            [
                "scholaramalgamalg #14",
                "scholaramalgamalg #7",
                "exportiunivamalg #9",
                "scholaramalgamalg #9",
            ]
        )
    )
    group_b = (
        item["i_category"].isin(["Women", "Music", "Men"])
        & item["i_class"].isin(
            ["accessories", "classical", "fragrances", "pants"]
        )
        & item["i_brand"].isin(
            [
                "amalgimporto #1",
                "edu packscholar #1",
                "exportiimporto #1",
                "importoamalg #1",
            ]
        )
    )
    item = item[group_a | group_b][["i_item_sk", "i_manager_id"]]

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_item_sk",
            "ss_sold_date_sk",
            "ss_store_sk",
            "ss_sales_price",
        ],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}",
        columns=["d_date_sk", "d_month_seq", "d_moy"],
    )
    date_dim = date_dim[date_dim["d_month_seq"].isin(list(range(1200, 1212)))][
        ["d_date_sk", "d_moy"]
    ]

    store = pd.read_parquet(f"{path}/store{suffix}", columns=["s_store_sk"])

    df = (
        store_sales.merge(item, left_on="ss_item_sk", right_on="i_item_sk")
        .merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
        .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    )

    grouped = (
        df.groupby(["i_manager_id", "d_moy"], dropna=False)
        .agg(sum_sales=("ss_sales_price", "sum"), n=("ss_sales_price", "count"))
        .reset_index()
    )
    # SUM over an all-NULL group is NULL: it takes no part in the average and
    # the CASE below is then NULL, which is not > 0.1.
    grouped = grouped[grouped["n"] > 0]
    grouped["_sum_sales"] = grouped["sum_sales"].astype("float64")
    grouped["avg_monthly_sales"] = grouped.groupby("i_manager_id", dropna=False)[
        "_sum_sales"
    ].transform("mean")

    positive = grouped["avg_monthly_sales"] > 0
    deviation = (
        grouped["_sum_sales"] - grouped["avg_monthly_sales"]
    ).abs() / grouped["avg_monthly_sales"]
    grouped = grouped[positive & (deviation > 0.1)]

    grouped = grouped.sort_values(
        ["i_manager_id", "avg_monthly_sales", "_sum_sales"]
    ).head(100)
    result = grouped[["i_manager_id", "sum_sales", "avg_monthly_sales"]]
    return result.reset_index(drop=True)
