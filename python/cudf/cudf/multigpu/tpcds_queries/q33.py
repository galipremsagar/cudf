# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 33.Electronics sales by manufacturer across all three channels for one month and time zone."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    date_dim = date_dim[(date_dim["d_year"] == 1998) & (date_dim["d_moy"] == 5)][
        ["d_date_sk"]
    ]

    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}", columns=["ca_address_sk", "ca_gmt_offset"]
    )
    customer_address = customer_address[
        customer_address["ca_gmt_offset"].astype("float64") == -5
    ][["ca_address_sk"]]

    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_manufact_id", "i_category"]
    )
    electronics = (
        item[item["i_category"] == "Electronics"]["i_manufact_id"].dropna().unique()
    )
    item = item[item["i_manufact_id"].isin(electronics)][
        ["i_item_sk", "i_manufact_id"]
    ]

    def channel(table, date_key, address_key, item_key, value):
        sales = pd.read_parquet(
            f"{path}/{table}{suffix}",
            columns=[date_key, address_key, item_key, value],
        )
        sales = sales.merge(date_dim, left_on=date_key, right_on="d_date_sk")
        sales = sales.merge(
            customer_address, left_on=address_key, right_on="ca_address_sk"
        )
        sales = sales.merge(item, left_on=item_key, right_on="i_item_sk")
        return (
            sales.groupby("i_manufact_id", as_index=False)[value]
            .sum()
            .rename(columns={value: "total_sales"})
        )

    parts = [
        channel(
            "store_sales",
            "ss_sold_date_sk",
            "ss_addr_sk",
            "ss_item_sk",
            "ss_ext_sales_price",
        ),
        channel(
            "catalog_sales",
            "cs_sold_date_sk",
            "cs_bill_addr_sk",
            "cs_item_sk",
            "cs_ext_sales_price",
        ),
        channel(
            "web_sales",
            "ws_sold_date_sk",
            "ws_bill_addr_sk",
            "ws_item_sk",
            "ws_ext_sales_price",
        ),
    ]

    combined = pd.concat(parts, ignore_index=True)
    result = combined.groupby("i_manufact_id", as_index=False)["total_sales"].sum()
    result = result.sort_values("total_sales").head(100)
    return result.reset_index(drop=True)
