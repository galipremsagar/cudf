# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 10. Demographics of customers in five counties who bought in early 2002."""

from __future__ import annotations

import pandas as pd

_COUNTIES = [
    "Rush County",
    "Toole County",
    "Jefferson County",
    "Dona Ana County",
    "La Porte County",
]
_GROUP_COLUMNS = [
    "cd_gender",
    "cd_marital_status",
    "cd_education_status",
    "cd_purchase_estimate",
    "cd_credit_rating",
    "cd_dep_count",
    "cd_dep_employed_count",
    "cd_dep_college_count",
]


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    dates = date_dim[
        (date_dim["d_year"] == 2002)
        & (date_dim["d_moy"] >= 1)
        & (date_dim["d_moy"] <= 4)
    ][["d_date_sk"]]

    def buyers(table, date_column, customer_column):
        sales = pd.read_parquet(
            f"{path}/{table}{suffix}", columns=[date_column, customer_column]
        )
        sales = sales.merge(
            dates, left_on=date_column, right_on="d_date_sk", how="inner"
        )
        return sales[customer_column].dropna().unique()

    store_buyers = buyers("store_sales", "ss_sold_date_sk", "ss_customer_sk")
    web_buyers = buyers("web_sales", "ws_sold_date_sk", "ws_bill_customer_sk")
    catalog_buyers = buyers("catalog_sales", "cs_sold_date_sk", "cs_ship_customer_sk")

    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=["c_customer_sk", "c_current_addr_sk", "c_current_cdemo_sk"],
    )
    customer = customer[
        customer["c_customer_sk"].isin(store_buyers)
        & (
            customer["c_customer_sk"].isin(web_buyers)
            | customer["c_customer_sk"].isin(catalog_buyers)
        )
    ]

    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}", columns=["ca_address_sk", "ca_county"]
    )
    addresses = customer_address[customer_address["ca_county"].isin(_COUNTIES)][
        ["ca_address_sk"]
    ]
    customer_demographics = pd.read_parquet(
        f"{path}/customer_demographics{suffix}",
        columns=["cd_demo_sk", *_GROUP_COLUMNS],
    )

    joined = customer.merge(
        addresses, left_on="c_current_addr_sk", right_on="ca_address_sk", how="inner"
    ).merge(
        customer_demographics,
        left_on="c_current_cdemo_sk",
        right_on="cd_demo_sk",
        how="inner",
    )

    grouped = (
        joined.groupby(_GROUP_COLUMNS, dropna=False).size().reset_index(name="cnt")
    )
    grouped = grouped.sort_values(
        _GROUP_COLUMNS, na_position="first", kind="stable"
    ).head(100)

    count = grouped["cnt"]
    result = pd.DataFrame(
        {
            "cd_gender": grouped["cd_gender"],
            "cd_marital_status": grouped["cd_marital_status"],
            "cd_education_status": grouped["cd_education_status"],
            "cnt1": count,
            "cd_purchase_estimate": grouped["cd_purchase_estimate"],
            "cnt2": count,
            "cd_credit_rating": grouped["cd_credit_rating"],
            "cnt3": count,
            "cd_dep_count": grouped["cd_dep_count"],
            "cnt4": count,
            "cd_dep_employed_count": grouped["cd_dep_employed_count"],
            "cnt5": count,
            "cd_dep_college_count": grouped["cd_dep_college_count"],
            "cnt6": count,
        }
    )
    return result.reset_index(drop=True)
