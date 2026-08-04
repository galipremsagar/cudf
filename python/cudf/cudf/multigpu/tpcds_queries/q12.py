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

    # i_current_price is a grouping key and ws_ext_sales_price is summed; both
    # are DECIMAL in the schema, which libcudf can neither group nor sum on the
    # GPU, so both become float64 first.
    joined = joined.assign(
        i_current_price=joined["i_current_price"].astype("float64"),
        ws_ext_sales_price=joined["ws_ext_sales_price"].astype("float64"),
    )

    keys = ["i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price"]
    grouped = (
        joined.groupby(keys, dropna=False)["ws_ext_sales_price"]
        .sum()
        .reset_index()
        .rename(columns={"ws_ext_sales_price": "itemrevenue"})
    )

    # ``sum(sum(...)) OVER (PARTITION BY i_class)`` -- the per-class total of
    # the already-grouped revenue, joined back rather than computed with a
    # groupby transform.
    class_total = (
        grouped.groupby("i_class", dropna=False)["itemrevenue"]
        .sum()
        .reset_index()
        .rename(columns={"itemrevenue": "_class_total"})
    )
    grouped = grouped.merge(class_total, on="i_class", how="left")
    grouped = grouped.assign(
        revenueratio=grouped["itemrevenue"] * 100.0000 / grouped["_class_total"]
    )

    grouped = grouped.sort_values(
        ["i_category", "i_class", "i_item_id", "i_item_desc", "revenueratio"],
        na_position="last",
        kind="stable",
    )
    return grouped[keys + ["itemrevenue", "revenueratio"]].head(100).reset_index(
        drop=True
    )
