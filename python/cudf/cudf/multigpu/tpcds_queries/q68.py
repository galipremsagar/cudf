# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 68.Early-month store tickets from two cities, bought by
customers who live somewhere else."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_store_sk",
            "ss_hdemo_sk",
            "ss_addr_sk",
            "ss_ticket_number",
            "ss_customer_sk",
            "ss_ext_sales_price",
            "ss_ext_list_price",
            "ss_ext_tax",
        ],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_dom", "d_year"]
    )
    date_dim = date_dim[
        (date_dim["d_dom"] >= 1)
        & (date_dim["d_dom"] <= 2)
        & date_dim["d_year"].isin([1999, 2000, 2001])
    ][["d_date_sk"]]

    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_city"]
    )
    store = store[store["s_city"].isin(["Fairview", "Midway"])][["s_store_sk"]]

    household_demographics = pd.read_parquet(
        f"{path}/household_demographics{suffix}",
        columns=["hd_demo_sk", "hd_dep_count", "hd_vehicle_count"],
    )
    household_demographics = household_demographics[
        (household_demographics["hd_dep_count"] == 4)
        | (household_demographics["hd_vehicle_count"] == 3)
    ][["hd_demo_sk"]]

    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}", columns=["ca_address_sk", "ca_city"]
    )

    merged = (
        store_sales.merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
        .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
        .merge(
            household_demographics, left_on="ss_hdemo_sk", right_on="hd_demo_sk"
        )
        .merge(customer_address, left_on="ss_addr_sk", right_on="ca_address_sk")
    )

    tickets = (
        merged.groupby(
            ["ss_ticket_number", "ss_customer_sk", "ss_addr_sk", "ca_city"],
            dropna=False,
        )
        .agg(
            extended_price=("ss_ext_sales_price", "sum"),
            list_price=("ss_ext_list_price", "sum"),
            extended_tax=("ss_ext_tax", "sum"),
        )
        .reset_index()
        .rename(columns={"ca_city": "bought_city"})
    )

    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=[
            "c_customer_sk",
            "c_current_addr_sk",
            "c_first_name",
            "c_last_name",
        ],
    )

    joined = tickets.merge(
        customer, left_on="ss_customer_sk", right_on="c_customer_sk"
    ).merge(customer_address, left_on="c_current_addr_sk", right_on="ca_address_sk")

    # ca_city <> bought_city is unknown, hence false, when either side is NULL.
    joined = joined[
        joined["ca_city"].notna()
        & joined["bought_city"].notna()
        & (joined["ca_city"] != joined["bought_city"])
    ]

    joined = joined.sort_values(
        ["c_last_name", "ss_ticket_number"], na_position="first"
    ).head(100)
    result = joined[
        [
            "c_last_name",
            "c_first_name",
            "ca_city",
            "bought_city",
            "ss_ticket_number",
            "extended_price",
            "extended_tax",
            "list_price",
        ]
    ]
    return result.reset_index(drop=True)
