# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 6. States with at least ten customers buying above-average priced items."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}",
        columns=["d_date_sk", "d_month_seq", "d_year", "d_moy"],
    )
    target_month = date_dim[(date_dim["d_year"] == 2001) & (date_dim["d_moy"] == 1)][
        "d_month_seq"
    ].unique()
    dates = date_dim[date_dim["d_month_seq"].isin(target_month)][["d_date_sk"]]

    item = pd.read_parquet(
        f"{path}/item{suffix}",
        columns=["i_item_sk", "i_current_price", "i_category"],
    )
    item = item.assign(i_current_price=item["i_current_price"].astype("float64"))
    category_avg = (
        item.groupby("i_category")["i_current_price"]
        .mean()
        .reset_index()
        .rename(columns={"i_current_price": "avg_price"})
    )
    item = item.merge(category_avg, on="i_category", how="inner")
    items = item[item["i_current_price"] > 1.2 * item["avg_price"]][["i_item_sk"]]

    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}", columns=["ca_address_sk", "ca_state"]
    )
    customer = pd.read_parquet(
        f"{path}/customer{suffix}", columns=["c_customer_sk", "c_current_addr_sk"]
    )
    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=["ss_customer_sk", "ss_sold_date_sk", "ss_item_sk"],
    )

    joined = (
        store_sales.merge(
            dates, left_on="ss_sold_date_sk", right_on="d_date_sk", how="inner"
        )
        .merge(items, left_on="ss_item_sk", right_on="i_item_sk", how="inner")
        .merge(
            customer, left_on="ss_customer_sk", right_on="c_customer_sk", how="inner"
        )
        .merge(
            customer_address,
            left_on="c_current_addr_sk",
            right_on="ca_address_sk",
            how="inner",
        )
    )

    grouped = (
        joined.groupby("ca_state", dropna=False)
        .size()
        .reset_index(name="cnt")
        .rename(columns={"ca_state": "state"})
    )
    grouped = grouped[grouped["cnt"] >= 10]
    grouped = grouped.sort_values(
        ["cnt", "state"], na_position="first", kind="stable"
    )
    return grouped[["state", "cnt"]].head(100).reset_index(drop=True)
