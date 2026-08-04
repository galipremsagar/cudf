# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 44.Best and worst performing items at store 4, paired by rank of average net profit."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    store_sales = pd.read_parquet(
        f"{run_config.dataset_path}/store_sales{run_config.suffix}",
        columns=["ss_store_sk", "ss_addr_sk", "ss_item_sk", "ss_net_profit"],
    )
    item = pd.read_parquet(
        f"{run_config.dataset_path}/item{run_config.suffix}",
        columns=["i_item_sk", "i_product_name"],
    )

    store_sales = store_sales[store_sales["ss_store_sk"] == 4]
    store_sales = store_sales.assign(
        ss_net_profit=store_sales["ss_net_profit"].astype("float64")
    )

    threshold = 0.9 * store_sales.loc[
        store_sales["ss_addr_sk"].isna(), "ss_net_profit"
    ].mean()

    ranked = (
        store_sales.groupby("ss_item_sk", dropna=False)["ss_net_profit"]
        .mean()
        .reset_index()
        .rename(columns={"ss_item_sk": "item_sk", "ss_net_profit": "rank_col"})
    )
    ranked = ranked[ranked["rank_col"] > threshold]

    ascending = ranked.assign(
        rnk=ranked["rank_col"].rank(method="min", ascending=True)
    )
    ascending = ascending[ascending["rnk"] < 11][["item_sk", "rnk"]]

    descending = ranked.assign(
        rnk=ranked["rank_col"].rank(method="min", ascending=False)
    )
    descending = descending[descending["rnk"] < 11][["item_sk", "rnk"]]

    ascending = ascending.merge(
        item, left_on="item_sk", right_on="i_item_sk"
    ).rename(columns={"i_product_name": "best_performing"})
    descending = descending.merge(
        item, left_on="item_sk", right_on="i_item_sk"
    ).rename(columns={"i_product_name": "worst_performing"})

    joined = ascending[["rnk", "best_performing"]].merge(
        descending[["rnk", "worst_performing"]], on="rnk"
    )

    result = (
        joined.sort_values("rnk")
        .head(100)
        .reset_index(drop=True)
    )
    return result[["rnk", "best_performing", "worst_performing"]]
