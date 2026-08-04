# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 34.Customers with large store tickets bought early or late in the month in one county."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_dom", "d_year"]
    )
    date_dim = date_dim[
        (
            ((date_dim["d_dom"] >= 1) & (date_dim["d_dom"] <= 3))
            | ((date_dim["d_dom"] >= 25) & (date_dim["d_dom"] <= 28))
        )
        & (date_dim["d_year"].isin([1999, 2000, 2001]))
    ][["d_date_sk"]]

    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_county"]
    )
    store = store[store["s_county"] == "Williamson County"][["s_store_sk"]]

    household_demographics = pd.read_parquet(
        f"{path}/household_demographics{suffix}",
        columns=["hd_demo_sk", "hd_buy_potential", "hd_dep_count", "hd_vehicle_count"],
    )
    household_demographics = household_demographics[
        household_demographics["hd_buy_potential"].isin([">10000", "Unknown"])
        & (household_demographics["hd_vehicle_count"] > 0)
    ]
    ratio = (
        household_demographics["hd_dep_count"] * 1.000
    ) / household_demographics["hd_vehicle_count"]
    household_demographics = household_demographics[ratio > 1.2][["hd_demo_sk"]]

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
        household_demographics, left_on="ss_hdemo_sk", right_on="hd_demo_sk"
    )

    tickets = (
        joined.groupby(["ss_ticket_number", "ss_customer_sk"], as_index=False)
        .size()
        .rename(columns={"size": "cnt"})
    )
    tickets = tickets[(tickets["cnt"] >= 15) & (tickets["cnt"] <= 20)]

    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=[
            "c_customer_sk",
            "c_salutation",
            "c_first_name",
            "c_last_name",
            "c_preferred_cust_flag",
        ],
    )
    result = tickets.merge(
        customer, left_on="ss_customer_sk", right_on="c_customer_sk"
    )

    result = result[
        [
            "c_last_name",
            "c_first_name",
            "c_salutation",
            "c_preferred_cust_flag",
            "ss_ticket_number",
            "cnt",
        ]
    ].sort_values(
        [
            "c_last_name",
            "c_first_name",
            "c_salutation",
            "c_preferred_cust_flag",
            "ss_ticket_number",
        ],
        ascending=[True, True, True, False, True],
        na_position="first",
    )
    return result.reset_index(drop=True)
