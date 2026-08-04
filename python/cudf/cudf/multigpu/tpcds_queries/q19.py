# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 19. Top brands by November 1998 store revenue where the customer lives outside the store's ZIP."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_moy", "d_year"]
    )
    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_item_sk",
            "ss_customer_sk",
            "ss_store_sk",
            "ss_ext_sales_price",
        ],
    )
    item = pd.read_parquet(
        f"{base}/item{suffix}",
        columns=[
            "i_item_sk",
            "i_brand_id",
            "i_brand",
            "i_manufact_id",
            "i_manufact",
            "i_manager_id",
        ],
    )
    customer = pd.read_parquet(
        f"{base}/customer{suffix}", columns=["c_customer_sk", "c_current_addr_sk"]
    )
    customer_address = pd.read_parquet(
        f"{base}/customer_address{suffix}", columns=["ca_address_sk", "ca_zip"]
    )
    store = pd.read_parquet(
        f"{base}/store{suffix}", columns=["s_store_sk", "s_zip"]
    )

    date_dim = date_dim[(date_dim["d_moy"] == 11) & (date_dim["d_year"] == 1998)][
        ["d_date_sk"]
    ]
    item = item[item["i_manager_id"] == 8][
        ["i_item_sk", "i_brand_id", "i_brand", "i_manufact_id", "i_manufact"]
    ]

    joined = (
        store_sales.merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
        .merge(item, left_on="ss_item_sk", right_on="i_item_sk")
        .merge(customer, left_on="ss_customer_sk", right_on="c_customer_sk")
        .merge(
            customer_address, left_on="c_current_addr_sk", right_on="ca_address_sk"
        )
        .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    )

    # Materialise the two ZIP prefixes as real columns of the joined frame:
    # comparing two deferred expressions against each other otherwise has to be
    # resolved against a single column rather than the join.
    joined = joined.assign(
        _customer_zip=joined["ca_zip"].str.slice(0, 5),
        _store_zip=joined["s_zip"].str.slice(0, 5),
        # ss_ext_sales_price is DECIMAL, which libcudf cannot group-sum.
        ss_ext_sales_price=joined["ss_ext_sales_price"].astype("float64"),
    )
    customer_zip = joined["_customer_zip"]
    store_zip = joined["_store_zip"]
    # A NULL on either side makes the SQL <> unknown, so the row is dropped.
    differs = (
        customer_zip.notna() & store_zip.notna() & (customer_zip != store_zip)
    )
    joined = joined[differs]

    grouped = (
        joined.groupby(
            ["i_brand", "i_brand_id", "i_manufact_id", "i_manufact"], dropna=False
        )["ss_ext_sales_price"]
        .sum()
        .reset_index()
        .rename(columns={"ss_ext_sales_price": "ext_price"})
    )

    grouped = grouped.sort_values(
        ["ext_price", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact"],
        ascending=[False, True, True, True, True],
        na_position="last",
        kind="stable",
    )
    result = grouped[
        ["i_brand_id", "i_brand", "i_manufact_id", "i_manufact", "ext_price"]
    ]
    return result.head(100).reset_index(drop=True)
