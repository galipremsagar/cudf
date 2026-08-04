# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 11. Customers whose web spending grew faster than their store spending from 2001 to 2002."""

from __future__ import annotations

import pandas as pd


def _year_total(sales, customer_key, date_key, list_price, discount, dates):
    joined = sales.merge(dates, left_on=date_key, right_on="d_date_sk")
    joined = joined.assign(
        year_total=joined[list_price].astype("float64")
        - joined[discount].astype("float64")
    )
    return (
        joined.groupby([customer_key, "d_year"], dropna=False)["year_total"]
        .sum()
        .reset_index()
        .rename(columns={customer_key: "customer_sk"})
    )


def _for_year(totals, year, name):
    return totals[totals["d_year"] == year][["customer_sk", "year_total"]].rename(
        columns={"year_total": name}
    )


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    customer = pd.read_parquet(
        f"{base}/customer{suffix}",
        columns=[
            "c_customer_sk",
            "c_customer_id",
            "c_first_name",
            "c_last_name",
            "c_preferred_cust_flag",
        ],
    )
    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=[
            "ss_customer_sk",
            "ss_sold_date_sk",
            "ss_ext_list_price",
            "ss_ext_discount_amt",
        ],
    )
    web_sales = pd.read_parquet(
        f"{base}/web_sales{suffix}",
        columns=[
            "ws_bill_customer_sk",
            "ws_sold_date_sk",
            "ws_ext_list_price",
            "ws_ext_discount_amt",
        ],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )

    # Only 2001 and 2002 are ever selected out of the year_total CTE.
    dates = date_dim[date_dim["d_year"].isin([2001, 2002])]

    # c_customer_id is unique per customer row, so grouping by the customer key
    # is the same partition as grouping by the seven customer attributes.
    store_totals = _year_total(
        store_sales,
        "ss_customer_sk",
        "ss_sold_date_sk",
        "ss_ext_list_price",
        "ss_ext_discount_amt",
        dates,
    )
    web_totals = _year_total(
        web_sales,
        "ws_bill_customer_sk",
        "ws_sold_date_sk",
        "ws_ext_list_price",
        "ws_ext_discount_amt",
        dates,
    )

    s_first = _for_year(store_totals, 2001, "s_first")
    s_second = _for_year(store_totals, 2002, "s_second")
    w_first = _for_year(web_totals, 2001, "w_first")
    w_second = _for_year(web_totals, 2002, "w_second")

    joined = (
        s_first.merge(s_second, on="customer_sk")
        .merge(w_first, on="customer_sk")
        .merge(w_second, on="customer_sk")
    )
    joined = joined[(joined["s_first"] > 0) & (joined["w_first"] > 0)]
    joined = joined[
        joined["w_second"] / joined["w_first"]
        > joined["s_second"] / joined["s_first"]
    ]

    result = joined.merge(
        customer, left_on="customer_sk", right_on="c_customer_sk"
    )
    result = result[
        [
            "c_customer_id",
            "c_first_name",
            "c_last_name",
            "c_preferred_cust_flag",
        ]
    ].sort_values(
        [
            "c_customer_id",
            "c_first_name",
            "c_last_name",
            "c_preferred_cust_flag",
        ],
        na_position="first",
        kind="stable",
    )
    return result.head(100).reset_index(drop=True)
