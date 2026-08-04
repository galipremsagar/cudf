# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 97.Customer/item pairs bought in store only, catalog only, or both."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=["ss_customer_sk", "ss_item_sk", "ss_sold_date_sk"],
    )
    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=["cs_bill_customer_sk", "cs_item_sk", "cs_sold_date_sk"],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_month_seq"]
    )

    date_dim = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1200 + 11)
    ][["d_date_sk"]]

    ssci = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    )[["ss_customer_sk", "ss_item_sk"]].drop_duplicates()
    ssci.columns = ["customer_sk", "item_sk"]

    csci = catalog_sales.merge(
        date_dim, left_on="cs_sold_date_sk", right_on="d_date_sk"
    )[["cs_bill_customer_sk", "cs_item_sk"]].drop_duplicates()
    csci.columns = ["customer_sk", "item_sk"]

    # A NULL customer_sk never satisfies the join condition, and the CASE
    # expressions all require a non-NULL customer_sk, so those rows count zero
    # towards every total.
    ssci = ssci[ssci["customer_sk"].notna()]
    csci = csci[csci["customer_sk"].notna()]

    # A full outer join with a side marker on each input stands in for
    # merge(indicator=True): both inputs are already distinct on the join keys,
    # so the join is one-to-one and a null marker means "no match".
    ssci = ssci.assign(_in_store=1)
    csci = csci.assign(_in_catalog=1)

    merged = ssci.merge(csci, on=["customer_sk", "item_sk"], how="outer")
    in_store = merged["_in_store"].notna()
    in_catalog = merged["_in_catalog"].notna()

    return pd.DataFrame(
        {
            "store_only": [int((in_store & ~in_catalog).sum())],
            "catalog_only": [int((~in_store & in_catalog).sum())],
            "store_and_catalog": [int((in_store & in_catalog).sum())],
        }
    )
