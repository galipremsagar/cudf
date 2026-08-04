# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 38.How many customer/day pairs bought in all three channels in the same year."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_date", "d_month_seq"]
    )
    date_dim = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1200 + 11)
    ][["d_date_sk", "d_date"]]

    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=["c_customer_sk", "c_last_name", "c_first_name"],
    )
    # INTERSECT treats two nulls as equal, a merge key does not, so nulls are
    # given a sentinel that cannot occur in the data.
    customer["c_last_name"] = customer["c_last_name"].fillna("<<null>>")
    customer["c_first_name"] = customer["c_first_name"].fillna("<<null>>")

    def channel(table, date_key, customer_key):
        sales = pd.read_parquet(
            f"{path}/{table}{suffix}", columns=[date_key, customer_key]
        )
        sales = sales.merge(date_dim, left_on=date_key, right_on="d_date_sk")
        sales = sales.merge(customer, left_on=customer_key, right_on="c_customer_sk")
        return sales[["c_last_name", "c_first_name", "d_date"]].drop_duplicates()

    store = channel("store_sales", "ss_sold_date_sk", "ss_customer_sk")
    catalog = channel("catalog_sales", "cs_sold_date_sk", "cs_bill_customer_sk")
    web = channel("web_sales", "ws_sold_date_sk", "ws_bill_customer_sk")

    hot = store.merge(catalog, on=["c_last_name", "c_first_name", "d_date"]).merge(
        web, on=["c_last_name", "c_first_name", "d_date"]
    )

    return pd.DataFrame({"count": [len(hot)]})
