# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 17. Variability of quantities sold, returned and re-bought by catalog, per item and store state."""

from __future__ import annotations

import pandas as pd

_QUARTERS = ["2001Q1", "2001Q2", "2001Q3"]


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
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
        f"{base}/store_returns{suffix}",
        columns=[
            "sr_returned_date_sk",
            "sr_customer_sk",
            "sr_item_sk",
            "sr_ticket_number",
            "sr_return_quantity",
        ],
    )
    catalog_sales = pd.read_parquet(
        f"{base}/catalog_sales{suffix}",
        columns=["cs_sold_date_sk", "cs_bill_customer_sk", "cs_item_sk", "cs_quantity"],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_quarter_name"]
    )
    store = pd.read_parquet(
        f"{base}/store{suffix}", columns=["s_store_sk", "s_state"]
    )
    item = pd.read_parquet(
        f"{base}/item{suffix}", columns=["i_item_sk", "i_item_id", "i_item_desc"]
    )

    d1 = date_dim[date_dim["d_quarter_name"] == "2001Q1"][["d_date_sk"]].rename(
        columns={"d_date_sk": "d1_date_sk"}
    )
    d2 = date_dim[date_dim["d_quarter_name"].isin(_QUARTERS)][["d_date_sk"]].rename(
        columns={"d_date_sk": "d2_date_sk"}
    )
    d3 = d2.rename(columns={"d2_date_sk": "d3_date_sk"})

    joined = (
        store_sales.merge(d1, left_on="ss_sold_date_sk", right_on="d1_date_sk")
        .merge(item, left_on="ss_item_sk", right_on="i_item_sk")
        .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
        .merge(
            store_returns,
            left_on=["ss_customer_sk", "ss_item_sk", "ss_ticket_number"],
            right_on=["sr_customer_sk", "sr_item_sk", "sr_ticket_number"],
        )
        .merge(d2, left_on="sr_returned_date_sk", right_on="d2_date_sk")
        .merge(
            catalog_sales,
            left_on=["sr_customer_sk", "sr_item_sk"],
            right_on=["cs_bill_customer_sk", "cs_item_sk"],
        )
        .merge(d3, left_on="cs_sold_date_sk", right_on="d3_date_sk")
    )

    grouped = joined.groupby(
        ["i_item_id", "i_item_desc", "s_state"], dropna=False
    ).agg(
        store_sales_quantitycount=("ss_quantity", "count"),
        store_sales_quantityave=("ss_quantity", "mean"),
        store_sales_quantitystdev=("ss_quantity", "std"),
        store_returns_quantitycount=("sr_return_quantity", "count"),
        store_returns_quantityave=("sr_return_quantity", "mean"),
        store_returns_quantitystdev=("sr_return_quantity", "std"),
        catalog_sales_quantitycount=("cs_quantity", "count"),
        catalog_sales_quantityave=("cs_quantity", "mean"),
        catalog_sales_quantitystdev=("cs_quantity", "std"),
    )
    grouped = grouped.reset_index()

    grouped["store_sales_quantitycov"] = (
        grouped["store_sales_quantitystdev"] / grouped["store_sales_quantityave"]
    )
    grouped["store_returns_quantitycov"] = (
        grouped["store_returns_quantitystdev"] / grouped["store_returns_quantityave"]
    )
    grouped["catalog_sales_quantitycov"] = (
        grouped["catalog_sales_quantitystdev"] / grouped["catalog_sales_quantityave"]
    )

    grouped = grouped.sort_values(
        ["i_item_id", "i_item_desc", "s_state"], na_position="first", kind="stable"
    )
    result = grouped[
        [
            "i_item_id",
            "i_item_desc",
            "s_state",
            "store_sales_quantitycount",
            "store_sales_quantityave",
            "store_sales_quantitystdev",
            "store_sales_quantitycov",
            "store_returns_quantitycount",
            "store_returns_quantityave",
            "store_returns_quantitystdev",
            "store_returns_quantitycov",
            "catalog_sales_quantitycount",
            "catalog_sales_quantityave",
            "catalog_sales_quantitystdev",
            "catalog_sales_quantitycov",
        ]
    ]
    return result.head(100).reset_index(drop=True)
