# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 87. How many customer/day pairs bought in stores but never from the catalog or the web."""

from __future__ import annotations

import pandas as pd

# EXCEPT matches NULL with NULL, so missing names get a value no name can take.
_NULL = "###NULL###"


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_month_seq"]
    )
    customer = pd.read_parquet(
        f"{base}/customer{suffix}",
        columns=["c_customer_sk", "c_last_name", "c_first_name"],
    )
    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=["ss_sold_date_sk", "ss_customer_sk"],
    )
    catalog_sales = pd.read_parquet(
        f"{base}/catalog_sales{suffix}",
        columns=["cs_sold_date_sk", "cs_bill_customer_sk"],
    )
    web_sales = pd.read_parquet(
        f"{base}/web_sales{suffix}",
        columns=["ws_sold_date_sk", "ws_bill_customer_sk"],
    )

    # d_date_sk is one-to-one with d_date, so it stands in for the date here.
    dates = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1200 + 11)
    ][["d_date_sk"]]

    customer = customer.assign(
        c_last_name=customer["c_last_name"].fillna(_NULL),
        c_first_name=customer["c_first_name"].fillna(_NULL),
    )

    keys = ["c_last_name", "c_first_name", "d_date_sk"]

    def distinct(sales, date_key, customer_key):
        frame = sales.merge(dates, left_on=date_key, right_on="d_date_sk")
        frame = frame.merge(
            customer, left_on=customer_key, right_on="c_customer_sk"
        )
        return frame[keys].drop_duplicates()

    cool = distinct(store_sales, "ss_sold_date_sk", "ss_customer_sk")
    for sales, date_key, customer_key in (
        (catalog_sales, "cs_sold_date_sk", "cs_bill_customer_sk"),
        (web_sales, "ws_sold_date_sk", "ws_bill_customer_sk"),
    ):
        other = distinct(sales, date_key, customer_key)
        merged = cool.merge(other, on=keys, how="left", indicator=True)
        cool = merged[merged["_merge"] == "left_only"][keys]

    return pd.DataFrame({"count_star": [len(cool)]})
