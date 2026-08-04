# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 74.Finds customers whose web spending grew faster than their store spending from 2001 to 2002."""

from __future__ import annotations

import pandas as pd


def _cents(series):
    """A DECIMAL(*,2) column as an exact whole number of cents."""
    return (series.astype("float64") * 100).round()


def _year_total(path, suffix, customer, date_dim, table, customer_sk, date_sk,
                net_paid):
    sales = pd.read_parquet(
        f"{path}/{table}{suffix}", columns=[customer_sk, date_sk, net_paid]
    )
    joined = sales.merge(
        customer, left_on=customer_sk, right_on="c_customer_sk"
    )
    joined = joined.merge(date_dim, left_on=date_sk, right_on="d_date_sk")
    joined["year_total"] = _cents(joined[net_paid])
    joined["paid_seen"] = joined["year_total"].notna().astype("int64")

    keys = ["c_customer_id", "c_first_name", "c_last_name", "d_year"]
    totals = (
        joined.groupby(keys, dropna=False)[["year_total", "paid_seen"]]
        .sum()
        .reset_index()
    )
    # SUM over a group whose values are all NULL is NULL, not zero.
    totals["year_total"] = totals["year_total"].where(totals["paid_seen"] > 0)
    return totals[keys + ["year_total"]]


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=["c_customer_sk", "c_customer_id", "c_first_name",
                 "c_last_name"],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    date_dim = date_dim[date_dim["d_year"].isin([2001, 2002])]

    store_totals = _year_total(
        path, suffix, customer, date_dim, "store_sales",
        "ss_customer_sk", "ss_sold_date_sk", "ss_net_paid",
    )
    web_totals = _year_total(
        path, suffix, customer, date_dim, "web_sales",
        "ws_bill_customer_sk", "ws_sold_date_sk", "ws_net_paid",
    )

    def slice_year(totals, year, positive_only):
        part = totals[totals["d_year"] == year]
        if positive_only:
            part = part[part["year_total"] > 0]
        return part

    s_first = slice_year(store_totals, 2001, True)[
        ["c_customer_id", "year_total"]
    ].rename(columns={"year_total": "s_first"})
    s_second = slice_year(store_totals, 2002, False).rename(
        columns={"year_total": "s_second"}
    )
    w_first = slice_year(web_totals, 2001, True)[
        ["c_customer_id", "year_total"]
    ].rename(columns={"year_total": "w_first"})
    w_second = slice_year(web_totals, 2002, False)[
        ["c_customer_id", "year_total"]
    ].rename(columns={"year_total": "w_second"})

    joined = s_second.merge(s_first, on="c_customer_id")
    joined = joined.merge(w_second, on="c_customer_id")
    joined = joined.merge(w_first, on="c_customer_id")

    joined = joined[
        (joined["w_second"] / joined["w_first"])
        > (joined["s_second"] / joined["s_first"])
    ]

    result = joined.sort_values("c_customer_id", na_position="first").head(100)
    return result[
        ["c_customer_id", "c_first_name", "c_last_name"]
    ].reset_index(drop=True)
