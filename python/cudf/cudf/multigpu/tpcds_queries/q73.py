# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 73.Finds customers whose early-of-month store tickets in selected counties number between one and five."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_dom", "d_year"]
    )
    date_dim = date_dim[
        (date_dim["d_dom"] >= 1)
        & (date_dim["d_dom"] <= 2)
        & (date_dim["d_year"].isin([1999, 2000, 2001]))
    ][["d_date_sk"]]

    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_county"]
    )
    store = store[
        store["s_county"].isin(
            [
                "Orange County",
                "Bronx County",
                "Franklin Parish",
                "Williamson County",
            ]
        )
    ][["s_store_sk"]]

    household = pd.read_parquet(
        f"{path}/household_demographics{suffix}",
        columns=[
            "hd_demo_sk",
            "hd_buy_potential",
            "hd_vehicle_count",
            "hd_dep_count",
        ],
    )
    household = household[
        household["hd_buy_potential"].isin(["Unknown", ">10000"])
        & (household["hd_vehicle_count"] > 0)
    ]
    ratio = household["hd_dep_count"].astype("float64") / household[
        "hd_vehicle_count"
    ].astype("float64")
    household = household[ratio > 1][["hd_demo_sk"]]

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_store_sk",
            "ss_hdemo_sk",
            "ss_ticket_number",
            "ss_customer_sk",
        ],
    )
    joined = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    )
    joined = joined.merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    joined = joined.merge(
        household, left_on="ss_hdemo_sk", right_on="hd_demo_sk"
    )

    joined["cnt"] = 1
    dj = (
        joined.groupby(["ss_ticket_number", "ss_customer_sk"], dropna=False)[
            ["cnt"]
        ]
        .sum()
        .reset_index()
    )
    dj = dj[(dj["cnt"] >= 1) & (dj["cnt"] <= 5)]

    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=[
            "c_customer_sk",
            "c_last_name",
            "c_first_name",
            "c_salutation",
            "c_preferred_cust_flag",
        ],
    )
    result = dj.merge(
        customer, left_on="ss_customer_sk", right_on="c_customer_sk"
    )
    result = result.sort_values(
        ["cnt", "c_last_name"], ascending=[False, True]
    )
    return result[
        [
            "c_last_name",
            "c_first_name",
            "c_salutation",
            "c_preferred_cust_flag",
            "ss_ticket_number",
            "cnt",
        ]
    ].reset_index(drop=True)
