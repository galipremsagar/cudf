# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 36.Gross margin by category and class in Tennessee stores, rolled up and ranked."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    date_dim = date_dim[date_dim["d_year"] == 2001][["d_date_sk"]]

    store = pd.read_parquet(f"{path}/store{suffix}", columns=["s_store_sk", "s_state"])
    store = store[store["s_state"] == "TN"][["s_store_sk"]]

    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_category", "i_class"]
    )

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_item_sk",
            "ss_store_sk",
            "ss_net_profit",
            "ss_ext_sales_price",
        ],
    )
    store_sales["ss_net_profit"] = store_sales["ss_net_profit"].astype("float64")
    store_sales["ss_ext_sales_price"] = store_sales["ss_ext_sales_price"].astype(
        "float64"
    )

    joined = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    )
    joined = joined.merge(item, left_on="ss_item_sk", right_on="i_item_sk")
    joined = joined.merge(store, left_on="ss_store_sk", right_on="s_store_sk")

    results = (
        joined.groupby(["i_category", "i_class"], dropna=False)
        .agg(
            ss_net_profit=("ss_net_profit", "sum"),
            ss_ext_sales_price=("ss_ext_sales_price", "sum"),
        )
        .reset_index()
    )

    level0 = results[["i_category", "i_class"]].copy()
    level0["gross_margin"] = (results["ss_net_profit"] * 1.0000) / results[
        "ss_ext_sales_price"
    ]
    level0["lochierarchy"] = 0
    level0["rank_within_parent"] = level0.groupby("i_category", dropna=False)[
        "gross_margin"
    ].rank(method="min")

    by_category = (
        results.groupby("i_category", dropna=False)
        .agg(
            ss_net_profit=("ss_net_profit", "sum"),
            ss_ext_sales_price=("ss_ext_sales_price", "sum"),
        )
        .reset_index()
    )
    level1 = by_category[["i_category"]].copy()
    level1["gross_margin"] = (by_category["ss_net_profit"] * 1.0000) / by_category[
        "ss_ext_sales_price"
    ]
    level1["lochierarchy"] = 1
    level1["rank_within_parent"] = level1["gross_margin"].rank(method="min")

    level2 = pd.DataFrame(
        {
            "gross_margin": [
                (results["ss_net_profit"].sum() * 1.0000)
                / results["ss_ext_sales_price"].sum()
            ],
            "lochierarchy": [2],
            "rank_within_parent": [1],
        }
    )

    rollup = pd.concat([level0, level1, level2], ignore_index=True)
    rollup["sort_key"] = rollup["i_category"].where(rollup["lochierarchy"] == 0)
    rollup = rollup.sort_values(
        ["lochierarchy", "sort_key", "rank_within_parent"],
        ascending=[False, True, True],
        na_position="first",
    ).head(100)

    result = rollup[
        ["gross_margin", "i_category", "i_class", "lochierarchy", "rank_within_parent"]
    ]
    return result.reset_index(drop=True)
