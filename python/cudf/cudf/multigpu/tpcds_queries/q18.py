# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 18. Rolled-up averages of 1998 catalog purchases by women of unknown education, per item and place."""

from __future__ import annotations

import pandas as pd

_STATES = ["MS", "IN", "ND", "OK", "NM", "VA", "MS"]
_BIRTH_MONTHS = [1, 6, 8, 9, 12, 2]
_AGGS = {
    "agg1": "cs_quantity",
    "agg2": "cs_list_price",
    "agg3": "cs_coupon_amt",
    "agg4": "cs_sales_price",
    "agg5": "cs_net_profit",
    "agg6": "c_birth_year",
    "agg7": "cd_dep_count",
}
_KEYS = ["i_item_id", "ca_country", "ca_state", "ca_county"]


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    catalog_sales = pd.read_parquet(
        f"{base}/catalog_sales{suffix}",
        columns=[
            "cs_sold_date_sk",
            "cs_item_sk",
            "cs_bill_cdemo_sk",
            "cs_bill_customer_sk",
            "cs_quantity",
            "cs_list_price",
            "cs_coupon_amt",
            "cs_sales_price",
            "cs_net_profit",
        ],
    )
    customer_demographics = pd.read_parquet(
        f"{base}/customer_demographics{suffix}",
        columns=["cd_demo_sk", "cd_gender", "cd_education_status", "cd_dep_count"],
    )
    customer = pd.read_parquet(
        f"{base}/customer{suffix}",
        columns=[
            "c_customer_sk",
            "c_current_cdemo_sk",
            "c_current_addr_sk",
            "c_birth_month",
            "c_birth_year",
        ],
    )
    customer_address = pd.read_parquet(
        f"{base}/customer_address{suffix}",
        columns=["ca_address_sk", "ca_country", "ca_state", "ca_county"],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    item = pd.read_parquet(
        f"{base}/item{suffix}", columns=["i_item_sk", "i_item_id"]
    )

    cd1 = customer_demographics[
        (customer_demographics["cd_gender"] == "F")
        & (customer_demographics["cd_education_status"] == "Unknown")
    ][["cd_demo_sk", "cd_dep_count"]]
    cd2 = customer_demographics[["cd_demo_sk"]].rename(
        columns={"cd_demo_sk": "cd2_demo_sk"}
    )
    customer = customer[customer["c_birth_month"].isin(_BIRTH_MONTHS)]
    customer_address = customer_address[customer_address["ca_state"].isin(_STATES)]
    date_dim = date_dim[date_dim["d_year"] == 1998][["d_date_sk"]]

    joined = (
        catalog_sales.merge(date_dim, left_on="cs_sold_date_sk", right_on="d_date_sk")
        .merge(item, left_on="cs_item_sk", right_on="i_item_sk")
        .merge(cd1, left_on="cs_bill_cdemo_sk", right_on="cd_demo_sk")
        .merge(customer, left_on="cs_bill_customer_sk", right_on="c_customer_sk")
        .merge(cd2, left_on="c_current_cdemo_sk", right_on="cd2_demo_sk")
        .merge(
            customer_address, left_on="c_current_addr_sk", right_on="ca_address_sk"
        )
    )

    names = list(_AGGS)
    values = joined[_KEYS].assign(
        _all=0,
        **{name: joined[column].astype("float64") for name, column in _AGGS.items()},
    )

    # ROLLUP: every prefix of the four grouping columns, plus the grand total.
    levels = []
    for depth in range(4, -1, -1):
        keys = _KEYS[:depth] if depth else ["_all"]
        level = values.groupby(keys, dropna=False)[names].mean().reset_index()
        for key in _KEYS:
            if key not in keys:
                level[key] = None
        levels.append(level[_KEYS + names])

    result = pd.concat(levels, ignore_index=True)
    result = result.sort_values(
        ["ca_country", "ca_state", "ca_county", "i_item_id"],
        na_position="first",
        kind="stable",
    )
    return result.head(100).reset_index(drop=True)
