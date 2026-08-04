# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 86. Web sales rolled up over category and class, ranked inside each parent level."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    web_sales = pd.read_parquet(
        f"{base}/web_sales{suffix}",
        columns=["ws_sold_date_sk", "ws_item_sk", "ws_net_paid"],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_month_seq"]
    )
    item = pd.read_parquet(
        f"{base}/item{suffix}", columns=["i_item_sk", "i_category", "i_class"]
    )

    dates = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1200 + 11)
    ][["d_date_sk"]]

    frame = web_sales.merge(dates, left_on="ws_sold_date_sk", right_on="d_date_sk")
    frame = frame.merge(item, left_on="ws_item_sk", right_on="i_item_sk")

    # rollup(i_category, i_class): the two grouping levels plus the grand total
    level0 = frame.groupby(["i_category", "i_class"], as_index=False, dropna=False)[
        "ws_net_paid"
    ].sum(min_count=1)
    level0["lochierarchy"] = 0

    level1 = frame.groupby("i_category", as_index=False, dropna=False)[
        "ws_net_paid"
    ].sum(min_count=1)
    level1["i_class"] = level1["i_category"].where(level1["i_category"].isna())
    level1["lochierarchy"] = 1
    level1 = level1[["i_category", "i_class", "ws_net_paid", "lochierarchy"]]

    level2 = level1.head(1).copy()
    level2["i_category"] = level2["i_category"].where(level2["i_category"].isna())
    level2["ws_net_paid"] = frame["ws_net_paid"].sum(min_count=1)
    level2["lochierarchy"] = 2

    rolled = pd.concat([level0, level1, level2], ignore_index=True)
    rolled = rolled.rename(columns={"ws_net_paid": "total_sum"})
    rolled["sort_sum"] = rolled["total_sum"].astype("float64")

    # case when grouping(i_class) = 0 then i_category end
    rolled["parent"] = rolled["i_category"].where(rolled["lochierarchy"] == 0)
    rolled["rank_within_parent"] = (
        rolled.groupby(["lochierarchy", "parent"], dropna=False)["sort_sum"]
        .rank(method="min", ascending=False)
        .astype("int64")
    )

    rolled = rolled.sort_values(
        ["lochierarchy", "parent", "rank_within_parent"],
        ascending=[False, True, True],
        na_position="first",
    )
    result = rolled[
        ["total_sum", "i_category", "i_class", "lochierarchy", "rank_within_parent"]
    ]
    return result.head(100).reset_index(drop=True)
