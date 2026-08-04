# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 62.How long web orders take to ship, bucketed by days, per
warehouse, shipping mode and web site."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    web_sales = pd.read_parquet(
        f"{path}/web_sales{suffix}",
        columns=[
            "ws_ship_date_sk",
            "ws_sold_date_sk",
            "ws_warehouse_sk",
            "ws_ship_mode_sk",
            "ws_web_site_sk",
        ],
    )

    warehouse = pd.read_parquet(
        f"{path}/warehouse{suffix}",
        columns=["w_warehouse_sk", "w_warehouse_name"],
    )
    warehouse["w_substr"] = warehouse["w_warehouse_name"].str.slice(0, 20)
    warehouse = warehouse[["w_warehouse_sk", "w_substr"]]

    ship_mode = pd.read_parquet(
        f"{path}/ship_mode{suffix}", columns=["sm_ship_mode_sk", "sm_type"]
    )
    web_site = pd.read_parquet(
        f"{path}/web_site{suffix}", columns=["web_site_sk", "web_name"]
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_month_seq"]
    )
    date_dim = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1211)
    ][["d_date_sk"]]

    df = (
        web_sales.merge(date_dim, left_on="ws_ship_date_sk", right_on="d_date_sk")
        .merge(warehouse, left_on="ws_warehouse_sk", right_on="w_warehouse_sk")
        .merge(ship_mode, left_on="ws_ship_mode_sk", right_on="sm_ship_mode_sk")
        .merge(web_site, left_on="ws_web_site_sk", right_on="web_site_sk")
    )

    days = df["ws_ship_date_sk"] - df["ws_sold_date_sk"]
    df = df.assign(
        d30=(days <= 30).astype("int64"),
        d31_60=((days > 30) & (days <= 60)).astype("int64"),
        d61_90=((days > 60) & (days <= 90)).astype("int64"),
        d91_120=((days > 90) & (days <= 120)).astype("int64"),
        d_gt_120=(days > 120).astype("int64"),
    )

    grouped = (
        df.groupby(["w_substr", "sm_type", "web_name"], dropna=False)[
            ["d30", "d31_60", "d61_90", "d91_120", "d_gt_120"]
        ]
        .sum()
        .reset_index()
    )

    grouped = grouped.sort_values(
        ["w_substr", "sm_type", "web_name"], na_position="first"
    ).head(100)
    return grouped.reset_index(drop=True)
