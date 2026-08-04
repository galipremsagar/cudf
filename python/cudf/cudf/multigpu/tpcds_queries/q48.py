# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 48.Total quantity sold in 2000 for combinations of customer demographic, price band, state and profit band."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    store_sales = pd.read_parquet(
        f"{run_config.dataset_path}/store_sales{run_config.suffix}",
        columns=[
            "ss_store_sk",
            "ss_sold_date_sk",
            "ss_cdemo_sk",
            "ss_addr_sk",
            "ss_quantity",
            "ss_sales_price",
            "ss_net_profit",
        ],
    )
    store = pd.read_parquet(
        f"{run_config.dataset_path}/store{run_config.suffix}",
        columns=["s_store_sk"],
    )
    customer_demographics = pd.read_parquet(
        f"{run_config.dataset_path}/customer_demographics{run_config.suffix}",
        columns=["cd_demo_sk", "cd_marital_status", "cd_education_status"],
    )
    customer_address = pd.read_parquet(
        f"{run_config.dataset_path}/customer_address{run_config.suffix}",
        columns=["ca_address_sk", "ca_country", "ca_state"],
    )
    date_dim = pd.read_parquet(
        f"{run_config.dataset_path}/date_dim{run_config.suffix}",
        columns=["d_date_sk", "d_year"],
    )

    date_dim = date_dim[date_dim["d_year"] == 2000]

    store_sales = store_sales.assign(
        ss_sales_price=store_sales["ss_sales_price"].astype("float64"),
        ss_net_profit=store_sales["ss_net_profit"].astype("float64"),
    )

    merged = (
        store_sales.merge(store, left_on="ss_store_sk", right_on="s_store_sk")
        .merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
        .merge(
            customer_demographics,
            left_on="ss_cdemo_sk",
            right_on="cd_demo_sk",
        )
        .merge(customer_address, left_on="ss_addr_sk", right_on="ca_address_sk")
    )

    marital = merged["cd_marital_status"]
    education = merged["cd_education_status"]
    price = merged["ss_sales_price"]
    demographic = (
        ((marital == "M") & (education == "4 yr Degree")
         & (price >= 100.00) & (price <= 150.00))
        | ((marital == "D") & (education == "2 yr Degree")
           & (price >= 50.00) & (price <= 100.00))
        | ((marital == "S") & (education == "College")
           & (price >= 150.00) & (price <= 200.00))
    )

    country = merged["ca_country"]
    state = merged["ca_state"]
    profit = merged["ss_net_profit"]
    address = (
        ((country == "United States") & state.isin(["CO", "OH", "TX"])
         & (profit >= 0) & (profit <= 2000))
        | ((country == "United States") & state.isin(["OR", "MN", "KY"])
           & (profit >= 150) & (profit <= 3000))
        | ((country == "United States") & state.isin(["VA", "CA", "MS"])
           & (profit >= 50) & (profit <= 25000))
    )

    total = merged.loc[demographic & address, "ss_quantity"].sum()
    return pd.DataFrame({"ss_quantity": [total]})
