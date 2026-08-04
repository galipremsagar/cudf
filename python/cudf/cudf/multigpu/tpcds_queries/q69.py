# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 69.Demographics of customers in three states who bought in a
store but neither on the web nor from the catalog in a quarter of 2001."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    date_dim = date_dim[
        (date_dim["d_year"] == 2001)
        & (date_dim["d_moy"] >= 4)
        & (date_dim["d_moy"] <= 6)
    ][["d_date_sk"]]

    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=["c_customer_sk", "c_current_addr_sk", "c_current_cdemo_sk"],
    )
    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}", columns=["ca_address_sk", "ca_state"]
    )
    customer_address = customer_address[
        customer_address["ca_state"].isin(["KY", "GA", "NM"])
    ][["ca_address_sk"]]

    df = customer.merge(
        customer_address, left_on="c_current_addr_sk", right_on="ca_address_sk"
    )

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=["ss_customer_sk", "ss_sold_date_sk"],
    )
    bought_in_store = (
        store_sales.merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")[
            ["ss_customer_sk"]
        ]
        .dropna()
        .drop_duplicates()
    )
    df = df.merge(
        bought_in_store, left_on="c_customer_sk", right_on="ss_customer_sk"
    )

    web_sales = pd.read_parquet(
        f"{path}/web_sales{suffix}",
        columns=["ws_bill_customer_sk", "ws_sold_date_sk"],
    )
    bought_on_web = (
        web_sales.merge(date_dim, left_on="ws_sold_date_sk", right_on="d_date_sk")[
            ["ws_bill_customer_sk"]
        ]
        .dropna()
        .drop_duplicates()
        .assign(_web=1)
    )
    df = df.merge(
        bought_on_web,
        left_on="c_customer_sk",
        right_on="ws_bill_customer_sk",
        how="left",
    )
    df = df[df["_web"].isna()]

    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=["cs_ship_customer_sk", "cs_sold_date_sk"],
    )
    bought_from_catalog = (
        catalog_sales.merge(
            date_dim, left_on="cs_sold_date_sk", right_on="d_date_sk"
        )[["cs_ship_customer_sk"]]
        .dropna()
        .drop_duplicates()
        .assign(_catalog=1)
    )
    df = df.merge(
        bought_from_catalog,
        left_on="c_customer_sk",
        right_on="cs_ship_customer_sk",
        how="left",
    )
    df = df[df["_catalog"].isna()]

    customer_demographics = pd.read_parquet(
        f"{path}/customer_demographics{suffix}",
        columns=[
            "cd_demo_sk",
            "cd_gender",
            "cd_marital_status",
            "cd_education_status",
            "cd_purchase_estimate",
            "cd_credit_rating",
        ],
    )
    df = df.merge(
        customer_demographics, left_on="c_current_cdemo_sk", right_on="cd_demo_sk"
    )

    keys = [
        "cd_gender",
        "cd_marital_status",
        "cd_education_status",
        "cd_purchase_estimate",
        "cd_credit_rating",
    ]
    grouped = (
        df.groupby(keys, dropna=False).size().reset_index().rename(columns={0: "cnt1"})
    )
    grouped = grouped.sort_values(keys, na_position="last").head(100)
    grouped = grouped.assign(cnt2=grouped["cnt1"], cnt3=grouped["cnt1"])
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
        ]
    ]
    return result.reset_index(drop=True)
