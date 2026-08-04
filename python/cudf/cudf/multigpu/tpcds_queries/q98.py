# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 98.Each item's share of its class's store revenue over one month."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=["ss_item_sk", "ss_sold_date_sk", "ss_ext_sales_price"],
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}",
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
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_date"]
    )

    item = item[item["i_category"].isin(["Sports", "Books", "Home"])]
    d_date = pd.to_datetime(date_dim["d_date"])
    date_dim = date_dim[
        (d_date >= pd.Timestamp("1999-02-22")) & (d_date <= pd.Timestamp("1999-03-24"))
    ]

    df = store_sales.merge(item, left_on="ss_item_sk", right_on="i_item_sk")
    df = df.merge(
        date_dim[["d_date_sk"]], left_on="ss_sold_date_sk", right_on="d_date_sk"
    )

    keys = ["i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price"]
    grouped = df.groupby(keys, dropna=False, as_index=False)["ss_ext_sales_price"].sum()
    grouped.columns = keys + ["itemrevenue"]

    revenue = grouped["itemrevenue"].astype("float64")
    grouped = grouped.assign(_revenue=revenue)
    class_total = grouped.groupby("i_class", dropna=False)["_revenue"].transform("sum")
    grouped = grouped.assign(
        revenueratio=grouped["_revenue"] * 100.0000 / class_total
    )

    grouped = grouped.sort_values(
        ["i_category", "i_class", "i_item_id", "i_item_desc", "revenueratio"],
        na_position="first",
    )
    return grouped[keys + ["itemrevenue", "revenueratio"]].reset_index(drop=True)
