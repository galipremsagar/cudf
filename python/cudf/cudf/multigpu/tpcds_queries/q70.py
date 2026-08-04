# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 70.Store net profit rolled up by state and county over a year,
ranked within each level of the hierarchy."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=["ss_sold_date_sk", "ss_store_sk", "ss_net_profit"],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_month_seq"]
    )
    date_dim = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1211)
    ][["d_date_sk"]]
    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_state", "s_county"]
    )

    df = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    ).merge(store, left_on="ss_store_sk", right_on="s_store_sk")

    # The IN subquery: states ranked within their own partition, so every state
    # that sold anything has ranking 1 and qualifies.
    by_state = (
        df.groupby("s_state", dropna=False)["ss_net_profit"].sum().reset_index()
    )
    by_state["_profit"] = by_state["ss_net_profit"].astype("float64")
    by_state["ranking"] = by_state.groupby("s_state", dropna=False)["_profit"].rank(
        method="min", ascending=False
    )
    top_states = by_state[by_state["ranking"] <= 5]["s_state"]
    df = df[df["s_state"].isin(top_states)]

    detail = (
        df.groupby(["s_state", "s_county"], dropna=False)["ss_net_profit"]
        .sum()
        .reset_index()
        .rename(columns={"ss_net_profit": "total_sum"})
    )
    detail["lochierarchy"] = 0
    detail["_total"] = detail["total_sum"].astype("float64")
    detail["rank_within_parent"] = detail.groupby("s_state", dropna=False)[
        "_total"
    ].rank(method="min", ascending=False)

    states = (
        df.groupby("s_state", dropna=False)["ss_net_profit"]
        .sum()
        .reset_index()
        .rename(columns={"ss_net_profit": "total_sum"})
    )
    states["s_county"] = None
    states["lochierarchy"] = 1
    states["_total"] = states["total_sum"].astype("float64")
    states["rank_within_parent"] = states["_total"].rank(
        method="min", ascending=False
    )

    grand = pd.DataFrame(
        {
            "total_sum": [df["ss_net_profit"].sum()],
            "s_state": [None],
            "s_county": [None],
            "lochierarchy": [2],
            "rank_within_parent": [1.0],
        }
    )

    columns = [
        "total_sum",
        "s_state",
        "s_county",
        "lochierarchy",
        "rank_within_parent",
    ]
    rollup = pd.concat(
        [detail[columns], states[columns], grand[columns]], ignore_index=True
    )
    rollup["_order"] = rollup["s_state"].where(rollup["lochierarchy"] == 0)
    rollup = rollup.sort_values(
        ["lochierarchy", "_order", "rank_within_parent"],
        ascending=[False, True, True],
        na_position="last",
    ).head(100)
    return rollup[columns].reset_index(drop=True)
