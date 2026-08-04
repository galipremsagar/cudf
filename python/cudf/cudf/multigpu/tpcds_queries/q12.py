# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 12. Web sales revenue per item and its share of its class over a one-month window."""

from __future__ import annotations

import pandas as pd

_CATEGORIES = ["Sports", "Books", "Home"]


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    web_sales = pd.read_parquet(
        f"{base}/web_sales{suffix}",
        columns=["ws_item_sk", "ws_sold_date_sk", "ws_ext_sales_price"],
    )
    item = pd.read_parquet(
        f"{base}/item{suffix}",
        columns=[
            "i_item_sk",
            "i_item_id",
            "i_item_desc",
            "i_category",
            "i_class",
            "i_current_price",
        ],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_date"]
    )

    d_date = pd.to_datetime(date_dim["d_date"])
    date_dim = date_dim[
        (d_date >= pd.Timestamp("1999-02-22"))
        & (d_date <= pd.Timestamp("1999-03-24"))
    ][["d_date_sk"]]

    item = item[item["i_category"].isin(_CATEGORIES)]

    joined = web_sales.merge(
        item, left_on="ws_item_sk", right_on="i_item_sk"
    ).merge(date_dim, left_on="ws_sold_date_sk", right_on="d_date_sk")

    keys = ["i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price"]
    grouped = (
        joined.groupby(keys, dropna=False)["ws_ext_sales_price"]
        .sum()
        .reset_index()
        .rename(columns={"ws_ext_sales_price": "itemrevenue"})
    )

    revenue = grouped["itemrevenue"].astype("float64")
    class_total = revenue.groupby(grouped["i_class"], dropna=False).transform("sum")
    grouped["revenueratio"] = revenue * 100.0000 / class_total

    grouped = grouped.sort_values(
        ["i_category", "i_class", "i_item_id", "i_item_desc", "revenueratio"],
        na_position="last",
        kind="stable",
    )
    return grouped.head(100).reset_index(drop=True)
