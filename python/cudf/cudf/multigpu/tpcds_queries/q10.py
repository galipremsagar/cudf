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

    # Each EXISTS becomes a distinct one-column frame joined back as a
    # semi-join.  Pulling the customer keys out with ``unique()`` would move
    # them to the host, which is both a fallback and unnecessary.
    def buyers(table, date_column, customer_column):
        sales = pd.read_parquet(
            f"{path}/{table}{suffix}", columns=[date_column, customer_column]
        )
        sales = sales.merge(
            dates, left_on=date_column, right_on="d_date_sk", how="inner"
        )
        return (
            sales[[customer_column]]
            .dropna()
            .drop_duplicates()
            .rename(columns={customer_column: "c_customer_sk"})
        )

    store_buyers = buyers("store_sales", "ss_sold_date_sk", "ss_customer_sk")
    web_buyers = buyers("web_sales", "ws_sold_date_sk", "ws_bill_customer_sk")
    catalog_buyers = buyers("catalog_sales", "cs_sold_date_sk", "cs_ship_customer_sk")
    # (web OR catalog) is one distinct key set, so the pair of EXISTS clauses
    # stays a single semi-join and cannot duplicate a customer.
    other_buyers = pd.concat(
        [web_buyers, catalog_buyers], ignore_index=True
    ).drop_duplicates()

    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=["c_customer_sk", "c_current_addr_sk", "c_current_cdemo_sk"],
    )
    customer = customer.merge(store_buyers, on="c_customer_sk", how="inner").merge(
        other_buyers, on="c_customer_sk", how="inner"
    )

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

    # ``count(*)`` as a named ``size`` aggregate: the demographic columns are
    # nullable and SQL's GROUP BY keeps a NULL as a group of its own.
    grouped = (
        joined.groupby(_GROUP_COLUMNS, dropna=False)
        .agg(cnt=("c_customer_sk", "size"))
        .reset_index()
    )
    grouped = grouped.sort_values(
        _GROUP_COLUMNS, na_position="first", kind="stable"
    ).head(100)

    # The same count is projected six times, once after each demographic
    # column.  Building this with ``assign`` keeps the frame on the GPU;
    # ``pd.DataFrame({...})`` would ask pandas to iterate the columns.
    grouped = grouped.assign(**{f"cnt{i}": grouped["cnt"] for i in range(1, 7)})
    result = grouped[
        [
            "cd_gender",
            "cd_marital_status",
            "cd_education_status",
            "cnt1",
            "cd_purchase_estimate",
            "cnt2",
            "cd_credit_rating",
            "cnt3",
            "cd_dep_count",
            "cnt4",
            "cd_dep_employed_count",
            "cnt5",
            "cd_dep_college_count",
            "cnt6",
        ]
    ]
    return result.reset_index(drop=True)
