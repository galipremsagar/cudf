# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 4. Customers whose catalog spend grew faster than their store and web spend."""

from __future__ import annotations

import pandas as pd

_CUSTOMER_COLUMNS = [
    "c_customer_id",
    "c_first_name",
    "c_last_name",
    "c_preferred_cust_flag",
    "c_birth_country",
    "c_login",
    "c_email_address",
]
_GROUP_COLUMNS = [
    "customer_id",
    "customer_first_name",
    "customer_last_name",
    "customer_preferred_cust_flag",
    "customer_birth_country",
    "customer_login",
    "customer_email_address",
    "dyear",
]
_RENAME = {
    "c_customer_id": "customer_id",
    "c_first_name": "customer_first_name",
    "c_last_name": "customer_last_name",
    "c_preferred_cust_flag": "customer_preferred_cust_flag",
    "c_birth_country": "customer_birth_country",
    "c_login": "customer_login",
    "c_email_address": "customer_email_address",
    "d_year": "dyear",
}


def _year_total(customer, dates, sales, customer_key, date_key, prefix):
    total = (
        (
            sales[f"{prefix}_ext_list_price"].astype("float64")
            - sales[f"{prefix}_ext_wholesale_cost"].astype("float64")
            - sales[f"{prefix}_ext_discount_amt"].astype("float64")
        )
        + sales[f"{prefix}_ext_sales_price"].astype("float64")
    ) / 2
    sales = sales[[customer_key, date_key]].assign(amount=total)

    joined = sales.merge(
        dates, left_on=date_key, right_on="d_date_sk", how="inner"
    ).merge(customer, left_on=customer_key, right_on="c_customer_sk", how="inner")
    joined = joined.rename(columns=_RENAME)
    return (
        joined.groupby(_GROUP_COLUMNS, dropna=False)["amount"]
        .sum()
        .reset_index()
        .rename(columns={"amount": "year_total"})
    )


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    customer = pd.read_parquet(
        f"{path}/customer{suffix}", columns=["c_customer_sk", *_CUSTOMER_COLUMNS]
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    dates = date_dim[date_dim["d_year"].isin([2001, 2002])]

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_customer_sk",
            "ss_sold_date_sk",
            "ss_ext_list_price",
            "ss_ext_wholesale_cost",
            "ss_ext_discount_amt",
            "ss_ext_sales_price",
        ],
    )
    store_year = _year_total(
        customer, dates, store_sales, "ss_customer_sk", "ss_sold_date_sk", "ss"
    )
    del store_sales

    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=[
            "cs_bill_customer_sk",
            "cs_sold_date_sk",
            "cs_ext_list_price",
            "cs_ext_wholesale_cost",
            "cs_ext_discount_amt",
            "cs_ext_sales_price",
        ],
    )
    catalog_year = _year_total(
        customer, dates, catalog_sales, "cs_bill_customer_sk", "cs_sold_date_sk", "cs"
    )
    del catalog_sales

    web_sales = pd.read_parquet(
        f"{path}/web_sales{suffix}",
        columns=[
            "ws_bill_customer_sk",
            "ws_sold_date_sk",
            "ws_ext_list_price",
            "ws_ext_wholesale_cost",
            "ws_ext_discount_amt",
            "ws_ext_sales_price",
        ],
    )
    web_year = _year_total(
        customer, dates, web_sales, "ws_bill_customer_sk", "ws_sold_date_sk", "ws"
    )
    del web_sales

    def slice_year(frame, year, name):
        out = frame[frame["dyear"] == year]
        return out[["customer_id", "year_total"]].rename(
            columns={"year_total": name}
        )

    s_first = slice_year(store_year, 2001, "s_first")
    s_first = s_first[s_first["s_first"] > 0]
    c_first = slice_year(catalog_year, 2001, "c_first")
    c_first = c_first[c_first["c_first"] > 0]
    w_first = slice_year(web_year, 2001, "w_first")
    w_first = w_first[w_first["w_first"] > 0]

    s_second = store_year[store_year["dyear"] == 2002][
        [
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_preferred_cust_flag",
            "year_total",
        ]
    ].rename(columns={"year_total": "s_second"})
    c_second = slice_year(catalog_year, 2002, "c_second")
    w_second = slice_year(web_year, 2002, "w_second")

    merged = (
        s_second.merge(s_first, on="customer_id", how="inner")
        .merge(c_first, on="customer_id", how="inner")
        .merge(c_second, on="customer_id", how="inner")
        .merge(w_first, on="customer_id", how="inner")
        .merge(w_second, on="customer_id", how="inner")
    )

    c_ratio = merged["c_second"] / merged["c_first"]
    s_ratio = merged["s_second"] / merged["s_first"]
    w_ratio = merged["w_second"] / merged["w_first"]
    merged = merged[(c_ratio > s_ratio) & (c_ratio > w_ratio)]

    result = merged[
        [
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_preferred_cust_flag",
        ]
    ].sort_values(
        [
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_preferred_cust_flag",
        ],
        na_position="first",
        kind="stable",
    )
    return result.head(100).reset_index(drop=True)
