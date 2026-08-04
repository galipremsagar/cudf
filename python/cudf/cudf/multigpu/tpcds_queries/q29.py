# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 29.Quantities sold, returned and later bought by catalog, per item and store."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_item_sk",
            "ss_customer_sk",
            "ss_store_sk",
            "ss_ticket_number",
            "ss_quantity",
        ],
    )
    store_returns = pd.read_parquet(
        f"{path}/store_returns{suffix}",
        columns=[
            "sr_returned_date_sk",
            "sr_item_sk",
            "sr_customer_sk",
            "sr_ticket_number",
            "sr_return_quantity",
        ],
    )
    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=[
            "cs_sold_date_sk",
            "cs_bill_customer_sk",
            "cs_item_sk",
            "cs_quantity",
        ],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_moy", "d_year"]
    )
    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_store_id", "s_store_name"]
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_item_id", "i_item_desc"]
    )

    # Every join below is an inner join, and SQL never matches NULL to NULL.
    # pandas/cuDF do, so a NULL ss_customer_sk pairs with every NULL
    # sr_customer_sk sharing the item/ticket, inventing rows SQL never produces
    # (SF1 has no NULL foreign keys; SF10+ has ~4.5%). Dropping NULL join keys
    # up front is exactly what inner-join semantics imply.
    store_sales = store_sales.dropna(
        subset=[
            "ss_sold_date_sk",
            "ss_item_sk",
            "ss_customer_sk",
            "ss_store_sk",
            "ss_ticket_number",
        ]
    )
    store_returns = store_returns.dropna(
        subset=[
            "sr_returned_date_sk",
            "sr_item_sk",
            "sr_customer_sk",
            "sr_ticket_number",
        ]
    )
    catalog_sales = catalog_sales.dropna(
        subset=["cs_sold_date_sk", "cs_bill_customer_sk", "cs_item_sk"]
    )

    d1 = date_dim[(date_dim["d_moy"] == 9) & (date_dim["d_year"] == 1999)][
        ["d_date_sk"]
    ]
    d2 = date_dim[
        (date_dim["d_moy"] >= 9)
        & (date_dim["d_moy"] <= 9 + 3)
        & (date_dim["d_year"] == 1999)
    ][["d_date_sk"]]
    d3 = date_dim[date_dim["d_year"].isin([1999, 2000, 2001])][["d_date_sk"]]

    sold = store_sales.merge(
        d1, left_on="ss_sold_date_sk", right_on="d_date_sk"
    ).drop(columns=["ss_sold_date_sk", "d_date_sk"])
    returned = store_returns.merge(
        d2, left_on="sr_returned_date_sk", right_on="d_date_sk"
    ).drop(columns=["sr_returned_date_sk", "d_date_sk"])
    catalog = catalog_sales.merge(
        d3, left_on="cs_sold_date_sk", right_on="d_date_sk"
    ).drop(columns=["cs_sold_date_sk", "d_date_sk"])

    df = sold.merge(
        returned,
        left_on=["ss_customer_sk", "ss_item_sk", "ss_ticket_number"],
        right_on=["sr_customer_sk", "sr_item_sk", "sr_ticket_number"],
    )
    df = df.merge(
        catalog,
        left_on=["sr_customer_sk", "sr_item_sk"],
        right_on=["cs_bill_customer_sk", "cs_item_sk"],
    )
    df = df.merge(item, left_on="ss_item_sk", right_on="i_item_sk")
    df = df.merge(store, left_on="ss_store_sk", right_on="s_store_sk")

    df = df.rename(
        columns={
            "ss_quantity": "store_sales_quantity",
            "sr_return_quantity": "store_returns_quantity",
            "cs_quantity": "catalog_sales_quantity",
        }
    )
    keys = ["i_item_id", "i_item_desc", "s_store_id", "s_store_name"]
    # min_count=1: SQL SUM of a group whose values are all NULL is NULL, while
    # pandas would report 0 for it. ss_quantity is nullable, and at SF100 two
    # of the 71 groups have no non-null quantity at all.
    out = df.groupby(keys, as_index=False, dropna=False)[
        [
            "store_sales_quantity",
            "store_returns_quantity",
            "catalog_sales_quantity",
        ]
    ].sum(min_count=1)

    out = out.sort_values(keys, na_position="last").head(100)
    return out.reset_index(drop=True)
