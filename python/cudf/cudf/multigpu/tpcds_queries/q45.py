# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 45.Web sales in Q2 2001 by customer zip and city, for selected zips or selected item ids."""

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
_ITEM_SKS = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]


def query(run_config):
    web_sales = pd.read_parquet(
        f"{run_config.dataset_path}/web_sales{run_config.suffix}",
        columns=[
            "ws_bill_customer_sk",
            "ws_item_sk",
            "ws_sold_date_sk",
            "ws_sales_price",
        ],
    )
    customer = pd.read_parquet(
        f"{run_config.dataset_path}/customer{run_config.suffix}",
        columns=["c_customer_sk", "c_current_addr_sk"],
    )
    customer_address = pd.read_parquet(
        f"{run_config.dataset_path}/customer_address{run_config.suffix}",
        columns=["ca_address_sk", "ca_zip", "ca_city"],
    )
    date_dim = pd.read_parquet(
        f"{run_config.dataset_path}/date_dim{run_config.suffix}",
        columns=["d_date_sk", "d_qoy", "d_year"],
    )
    item = pd.read_parquet(
        f"{run_config.dataset_path}/item{run_config.suffix}",
        columns=["i_item_sk", "i_item_id"],
    )

    date_dim = date_dim[(date_dim["d_qoy"] == 2) & (date_dim["d_year"] == 2001)]

    # The IN-subquery is one arm of an OR, so it cannot be a semi-join on the
    # joined rows; it is marked with a flag instead, attached to `item` by a
    # left join against the distinct qualifying i_item_id values. (isin() with
    # a column of a partitioned frame would only see the local partition.)
    target_item_ids = (
        item[item["i_item_sk"].isin(_ITEM_SKS)][["i_item_id"]]
        .dropna()
        .drop_duplicates()
        .assign(_id_hit=1)
    )
    item = item.merge(target_item_ids, on="i_item_id", how="left")

    merged = (
        web_sales.merge(
            date_dim, left_on="ws_sold_date_sk", right_on="d_date_sk"
        )
        .merge(customer, left_on="ws_bill_customer_sk", right_on="c_customer_sk")
        .merge(
            customer_address,
            left_on="c_current_addr_sk",
            right_on="ca_address_sk",
        )
        .merge(item, left_on="ws_item_sk", right_on="i_item_sk")
    )

    merged = merged[
        merged["ca_zip"].str.slice(0, 5).isin(_ZIPS)
        | merged["_id_hit"].notna()
    ]

    # libcudf has no group-by sum for fixed-point columns.
    merged = merged.assign(
        ws_sales_price=merged["ws_sales_price"].astype("float64")
    )

    grouped = (
        merged.groupby(["ca_zip", "ca_city"], dropna=False)["ws_sales_price"]
        .sum()
        .reset_index()
    )

    result = (
        grouped.sort_values(["ca_zip", "ca_city"])
        .head(100)
        .reset_index(drop=True)
    )
    return result[["ca_zip", "ca_city", "ws_sales_price"]]
