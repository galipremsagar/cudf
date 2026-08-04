# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 20. Catalog sales revenue per item and its share of its class over a one-month window."""

from __future__ import annotations

import pandas as pd

_CATEGORIES = ["Sports", "Books", "Home"]


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    catalog_sales = pd.read_parquet(
        f"{base}/catalog_sales{suffix}",
        columns=["cs_item_sk", "cs_sold_date_sk", "cs_ext_sales_price"],
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
    # ``i_current_price`` is a DECIMAL, and a decimal cannot be a group key on
    # the GPU; the money column cannot be summed as a decimal either.
    item = item.assign(i_current_price=item["i_current_price"].astype("float64"))
    catalog_sales = catalog_sales.assign(
        cs_ext_sales_price=catalog_sales["cs_ext_sales_price"].astype("float64")
    )

    joined = catalog_sales.merge(
        item, left_on="cs_item_sk", right_on="i_item_sk"
    ).merge(date_dim, left_on="cs_sold_date_sk", right_on="d_date_sk")

    keys = ["i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price"]
    grouped = (
        joined.groupby(keys, dropna=False)["cs_ext_sales_price"]
        .sum()
        .reset_index()
        .rename(columns={"cs_ext_sales_price": "itemrevenue"})
    )

    # The window SUM(...) OVER (PARTITION BY i_class) written as a second
    # aggregation joined back on, which is what the multi-GPU layer supports.
    class_total = (
        grouped.groupby("i_class", as_index=False, dropna=False)["itemrevenue"]
        .sum()
        .rename(columns={"itemrevenue": "class_total"})
    )
    grouped = grouped.merge(class_total, on="i_class", how="left")
    grouped["revenueratio"] = (
        grouped["itemrevenue"] * 100.0000 / grouped["class_total"]
    )

    grouped = grouped.sort_values(
        ["i_category", "i_class", "i_item_id", "i_item_desc", "revenueratio"],
        na_position="first",
    )
    return grouped[keys + ["itemrevenue", "revenueratio"]].head(100).reset_index(
        drop=True
    )
