# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 35.Demographic profile of customers who bought in stores and also on the web or by catalog."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_qoy"]
    )
    date_dim = date_dim[(date_dim["d_year"] == 2002) & (date_dim["d_qoy"] < 4)][
        ["d_date_sk"]
    ]

    def buyers(table, date_key, customer_key):
        sales = pd.read_parquet(
            f"{path}/{table}{suffix}", columns=[date_key, customer_key]
        )
        sales = sales.merge(date_dim, left_on=date_key, right_on="d_date_sk")
        return (
            sales[[customer_key]]
            .rename(columns={customer_key: "customer_sk"})
            .dropna()
            .drop_duplicates()
        )

    store_buyers = buyers("store_sales", "ss_sold_date_sk", "ss_customer_sk")
    web_buyers = buyers("web_sales", "ws_sold_date_sk", "ws_bill_customer_sk")
    catalog_buyers = buyers("catalog_sales", "cs_sold_date_sk", "cs_ship_customer_sk")
    other_buyers = pd.concat(
        [web_buyers, catalog_buyers], ignore_index=True
    ).drop_duplicates()

    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=["c_customer_sk", "c_current_addr_sk", "c_current_cdemo_sk"],
    )
    customer = customer.merge(
        store_buyers, left_on="c_customer_sk", right_on="customer_sk"
    ).drop(columns=["customer_sk"])
    customer = customer.merge(
        other_buyers, left_on="c_customer_sk", right_on="customer_sk"
    ).drop(columns=["customer_sk"])

    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}", columns=["ca_address_sk", "ca_state"]
    )
    customer_demographics = pd.read_parquet(
        f"{path}/customer_demographics{suffix}",
        columns=[
            "cd_demo_sk",
            "cd_gender",
            "cd_marital_status",
            "cd_dep_count",
            "cd_dep_employed_count",
            "cd_dep_college_count",
        ],
    )

    frame = customer.merge(
        customer_address, left_on="c_current_addr_sk", right_on="ca_address_sk"
    ).merge(
        customer_demographics, left_on="c_current_cdemo_sk", right_on="cd_demo_sk"
    )

    frame["dep_count"] = frame["cd_dep_count"]
    frame["dep_employed_count"] = frame["cd_dep_employed_count"]
    frame["dep_college_count"] = frame["cd_dep_college_count"]
    frame["one"] = 1

    keys = [
        "ca_state",
        "cd_gender",
        "cd_marital_status",
        "cd_dep_count",
        "cd_dep_employed_count",
        "cd_dep_college_count",
    ]
    grouped = (
        frame.groupby(keys, dropna=False)
        .agg(
            cnt1=("one", "sum"),
            min1=("dep_count", "min"),
            max1=("dep_count", "max"),
            avg1=("dep_count", "mean"),
            min2=("dep_employed_count", "min"),
            max2=("dep_employed_count", "max"),
            avg2=("dep_employed_count", "mean"),
            min3=("dep_college_count", "min"),
            max3=("dep_college_count", "max"),
            avg3=("dep_college_count", "mean"),
        )
        .reset_index()
    )
    grouped["cnt2"] = grouped["cnt1"]
    grouped["cnt3"] = grouped["cnt1"]

    grouped = grouped.sort_values(keys, na_position="first").head(100)
    result = grouped[
        [
            "ca_state",
            "cd_gender",
            "cd_marital_status",
            "cd_dep_count",
            "cnt1",
            "min1",
            "max1",
            "avg1",
            "cd_dep_employed_count",
            "cnt2",
            "min2",
            "max2",
            "avg2",
            "cd_dep_college_count",
            "cnt3",
            "min3",
            "max3",
            "avg3",
        ]
    ]
    return result.reset_index(drop=True)
