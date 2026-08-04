# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 46.Weekend store tickets bought in a city other than the customer's own city."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    store_sales = pd.read_parquet(
        f"{run_config.dataset_path}/store_sales{run_config.suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_store_sk",
            "ss_hdemo_sk",
            "ss_addr_sk",
            "ss_ticket_number",
            "ss_customer_sk",
            "ss_coupon_amt",
            "ss_net_profit",
        ],
    )
    date_dim = pd.read_parquet(
        f"{run_config.dataset_path}/date_dim{run_config.suffix}",
        columns=["d_date_sk", "d_dow", "d_year"],
    )
    store = pd.read_parquet(
        f"{run_config.dataset_path}/store{run_config.suffix}",
        columns=["s_store_sk", "s_city"],
    )
    household_demographics = pd.read_parquet(
        f"{run_config.dataset_path}/household_demographics{run_config.suffix}",
        columns=["hd_demo_sk", "hd_dep_count", "hd_vehicle_count"],
    )
    customer_address = pd.read_parquet(
        f"{run_config.dataset_path}/customer_address{run_config.suffix}",
        columns=["ca_address_sk", "ca_city"],
    )
    customer = pd.read_parquet(
        f"{run_config.dataset_path}/customer{run_config.suffix}",
        columns=["c_customer_sk", "c_current_addr_sk", "c_first_name", "c_last_name"],
    )

    date_dim = date_dim[
        date_dim["d_dow"].isin([6, 0]) & date_dim["d_year"].isin([1999, 2000, 2001])
    ]
    store = store[store["s_city"].isin(["Fairview", "Midway"])]
    household_demographics = household_demographics[
        (household_demographics["hd_dep_count"] == 4)
        | (household_demographics["hd_vehicle_count"] == 3)
    ]

    merged = (
        store_sales.merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
        .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
        .merge(
            household_demographics,
            left_on="ss_hdemo_sk",
            right_on="hd_demo_sk",
        )
        .merge(customer_address, left_on="ss_addr_sk", right_on="ca_address_sk")
    )

    dn = (
        merged.groupby(
            ["ss_ticket_number", "ss_customer_sk", "ss_addr_sk", "ca_city"],
            dropna=False,
        )[["ss_coupon_amt", "ss_net_profit"]]
        .sum()
        .reset_index()
        .rename(
            columns={
                "ca_city": "bought_city",
                "ss_coupon_amt": "amt",
                "ss_net_profit": "profit",
            }
        )
    )

    joined = dn.merge(
        customer, left_on="ss_customer_sk", right_on="c_customer_sk"
    ).merge(
        customer_address, left_on="c_current_addr_sk", right_on="ca_address_sk"
    )

    # ca_city <> bought_city is unknown, hence false, when either side is NULL.
    joined = joined[
        joined["ca_city"].notna()
        & joined["bought_city"].notna()
        & (joined["ca_city"] != joined["bought_city"])
    ]

    result = (
        joined.sort_values(
            [
                "c_last_name",
                "c_first_name",
                "ca_city",
                "bought_city",
                "ss_ticket_number",
            ],
            na_position="first",
        )
        .head(100)
        .reset_index(drop=True)
    )
    return result[
        [
            "c_last_name",
            "c_first_name",
            "ca_city",
            "bought_city",
            "ss_ticket_number",
            "amt",
            "profit",
        ]
    ]
