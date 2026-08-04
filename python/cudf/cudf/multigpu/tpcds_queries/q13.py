# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 13. Average store sale size in 2001 for selected demographic, price and state/profit combinations."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=[
            "ss_store_sk",
            "ss_sold_date_sk",
            "ss_hdemo_sk",
            "ss_cdemo_sk",
            "ss_addr_sk",
            "ss_sales_price",
            "ss_net_profit",
            "ss_quantity",
            "ss_ext_sales_price",
            "ss_ext_wholesale_cost",
        ],
    )
    store = pd.read_parquet(f"{base}/store{suffix}", columns=["s_store_sk"])
    customer_demographics = pd.read_parquet(
        f"{base}/customer_demographics{suffix}",
        columns=["cd_demo_sk", "cd_marital_status", "cd_education_status"],
    )
    household_demographics = pd.read_parquet(
        f"{base}/household_demographics{suffix}",
        columns=["hd_demo_sk", "hd_dep_count"],
    )
    customer_address = pd.read_parquet(
        f"{base}/customer_address{suffix}",
        columns=["ca_address_sk", "ca_country", "ca_state"],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )

    # Every branch of the first OR needs one of three (marital, education)
    # pairs and hd_dep_count in (1, 3); every branch of the second needs
    # ca_country = 'United States'.  Narrowing the dimensions first is the same
    # set of rows and a far smaller join.
    pairs = (
        (
            (customer_demographics["cd_marital_status"] == "M")
            & (customer_demographics["cd_education_status"] == "Advanced Degree")
        )
        | (
            (customer_demographics["cd_marital_status"] == "S")
            & (customer_demographics["cd_education_status"] == "College")
        )
        | (
            (customer_demographics["cd_marital_status"] == "W")
            & (customer_demographics["cd_education_status"] == "2 yr Degree")
        )
    )
    customer_demographics = customer_demographics[pairs]
    household_demographics = household_demographics[
        household_demographics["hd_dep_count"].isin([1, 3])
    ]
    customer_address = customer_address[
        customer_address["ca_country"] == "United States"
    ][["ca_address_sk", "ca_state"]]
    date_dim = date_dim[date_dim["d_year"] == 2001][["d_date_sk"]]

    joined = (
        store_sales.merge(store, left_on="ss_store_sk", right_on="s_store_sk")
        .merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
        .merge(
            household_demographics, left_on="ss_hdemo_sk", right_on="hd_demo_sk"
        )
        .merge(customer_demographics, left_on="ss_cdemo_sk", right_on="cd_demo_sk")
        .merge(customer_address, left_on="ss_addr_sk", right_on="ca_address_sk")
    )

    sales_price = joined["ss_sales_price"].astype("float64")
    net_profit = joined["ss_net_profit"].astype("float64")
    marital = joined["cd_marital_status"]
    education = joined["cd_education_status"]
    dep_count = joined["hd_dep_count"]
    state = joined["ca_state"]

    demo = (
        (
            (marital == "M")
            & (education == "Advanced Degree")
            & (sales_price >= 100.00)
            & (sales_price <= 150.00)
            & (dep_count == 3)
        )
        | (
            (marital == "S")
            & (education == "College")
            & (sales_price >= 50.00)
            & (sales_price <= 100.00)
            & (dep_count == 1)
        )
        | (
            (marital == "W")
            & (education == "2 yr Degree")
            & (sales_price >= 150.00)
            & (sales_price <= 200.00)
            & (dep_count == 1)
        )
    )
    address = (
        (
            state.isin(["TX", "OH"])
            & (net_profit >= 100)
            & (net_profit <= 200)
        )
        | (
            state.isin(["OR", "NM", "KY"])
            & (net_profit >= 150)
            & (net_profit <= 300)
        )
        | (
            state.isin(["VA", "TX", "MS"])
            & (net_profit >= 50)
            & (net_profit <= 250)
        )
    )

    joined = joined[demo & address]
    joined = joined.assign(
        _all=0,
        ss_ext_sales_price_f=joined["ss_ext_sales_price"].astype("float64"),
        ss_ext_wholesale_cost_f=joined["ss_ext_wholesale_cost"].astype("float64"),
    )

    result = joined.groupby("_all").agg(
        avg1=("ss_quantity", "mean"),
        avg2=("ss_ext_sales_price_f", "mean"),
        avg3=("ss_ext_wholesale_cost_f", "mean"),
        total=("ss_ext_wholesale_cost", "sum"),
    )
    return result.reset_index(drop=True)
