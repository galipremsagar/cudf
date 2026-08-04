# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 15. Catalog sales revenue by customer ZIP for selected ZIPs, states, or large sales in Q2 2001."""

from __future__ import annotations

import pandas as pd

_ZIPS = [
    "85669",
    "86197",
    "88274",
    "83405",
    "86475",
    "85392",
    "85460",
    "80348",
    "81792",
]
_STATES = ["CA", "WA", "GA"]


def query(run_config):
    base = f"{run_config.dataset_path}"
    suffix = run_config.suffix

    catalog_sales = pd.read_parquet(
        f"{base}/catalog_sales{suffix}",
        columns=["cs_bill_customer_sk", "cs_sold_date_sk", "cs_sales_price"],
    )
    customer = pd.read_parquet(
        f"{base}/customer{suffix}",
        columns=["c_customer_sk", "c_current_addr_sk"],
    )
    customer_address = pd.read_parquet(
        f"{base}/customer_address{suffix}",
        columns=["ca_address_sk", "ca_zip", "ca_state"],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}",
        columns=["d_date_sk", "d_qoy", "d_year"],
    )

    date_dim = date_dim[(date_dim["d_qoy"] == 2) & (date_dim["d_year"] == 2001)]

    merged = catalog_sales.merge(
        date_dim[["d_date_sk"]], left_on="cs_sold_date_sk", right_on="d_date_sk"
    )
    merged = merged.merge(
        customer, left_on="cs_bill_customer_sk", right_on="c_customer_sk"
    )
    merged = merged.merge(
        customer_address, left_on="c_current_addr_sk", right_on="ca_address_sk"
    )

    keep = (
        merged["ca_zip"].str.slice(0, 5).isin(_ZIPS)
        | merged["ca_state"].isin(_STATES)
        | (merged["cs_sales_price"] > 500)
    )
    merged = merged[keep]

    grouped = (
        merged.groupby("ca_zip", dropna=False)["cs_sales_price"]
        .sum()
        .reset_index()
    )
    grouped = grouped.sort_values("ca_zip", na_position="first")
    return grouped.head(100).reset_index(drop=True)
