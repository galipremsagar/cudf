# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 40.Catalog sales net of returns for cheap items, before and after a given date, by warehouse state."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_date"]
    )
    date_dim["d_date"] = pd.to_datetime(date_dim["d_date"])
    date_dim = date_dim[
        (date_dim["d_date"] >= pd.Timestamp("2000-02-10"))
        & (date_dim["d_date"] <= pd.Timestamp("2000-04-10"))
    ]

    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_item_id", "i_current_price"]
    )
    item["i_current_price"] = item["i_current_price"].astype("float64")
    item = item[
        (item["i_current_price"] >= 0.99) & (item["i_current_price"] <= 1.49)
    ][["i_item_sk", "i_item_id"]]

    warehouse = pd.read_parquet(
        f"{path}/warehouse{suffix}", columns=["w_warehouse_sk", "w_state"]
    )

    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=[
            "cs_order_number",
            "cs_item_sk",
            "cs_warehouse_sk",
            "cs_sold_date_sk",
            "cs_sales_price",
        ],
    )

    catalog_returns = pd.read_parquet(
        f"{path}/catalog_returns{suffix}",
        columns=["cr_order_number", "cr_item_sk", "cr_refunded_cash"],
    )

    joined = catalog_sales.merge(
        catalog_returns,
        left_on=["cs_order_number", "cs_item_sk"],
        right_on=["cr_order_number", "cr_item_sk"],
        how="left",
    )
    joined = joined.merge(warehouse, left_on="cs_warehouse_sk", right_on="w_warehouse_sk")
    joined = joined.merge(item, left_on="cs_item_sk", right_on="i_item_sk")
    joined = joined.merge(date_dim, left_on="cs_sold_date_sk", right_on="d_date_sk")

    # The money columns are decimals, which no GPU groupby can sum, and the
    # date flag becomes a column of its own so that every operand of the CASE
    # comes from one and the same frame.
    split = pd.Timestamp("2000-03-11")
    joined = joined.assign(
        cs_sales_price=joined["cs_sales_price"].astype("float64"),
        cr_refunded_cash=joined["cr_refunded_cash"].astype("float64"),
        is_before=joined["d_date"] < split,
    )
    net = joined["cs_sales_price"] - joined["cr_refunded_cash"].fillna(0)
    is_before = joined["is_before"]
    joined = joined.assign(
        sales_before=net.where(is_before, 0.0),
        sales_after=net.where(~is_before, 0.0),
    )

    result = (
        joined.groupby(["w_state", "i_item_id"], dropna=False)
        .agg(
            sales_before=("sales_before", "sum"),
            sales_after=("sales_after", "sum"),
        )
        .reset_index()
    )
    result = result.sort_values(["w_state", "i_item_id"]).head(100)
    return result.reset_index(drop=True)
