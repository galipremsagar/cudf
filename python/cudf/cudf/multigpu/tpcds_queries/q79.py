# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 79.Lists Monday store tickets from mid-size stores with their coupon amount and net profit, by customer."""

from __future__ import annotations

import pandas as pd


def _cents(series):
    """A DECIMAL(*,2) column as an exact whole number of cents."""
    return (series.astype("float64") * 100).round()


def _decimal_str(cents):
    """Cents rendered the way DuckDB prints a DECIMAL(*,2)."""
    missing = cents.isna()
    filled = cents.fillna(0.0)
    negative = filled < 0
    magnitude = filled.abs()
    whole = (magnitude // 100).astype("int64").astype("str")
    frac = (magnitude % 100).astype("int64").astype("str").str.zfill(2)
    text = whole + "." + frac
    text = text.where(~negative, "-" + text)
    return text.where(~missing)


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_dow", "d_year"]
    )
    date_dim = date_dim[
        (date_dim["d_dow"] == 1) & (date_dim["d_year"].isin([1999, 2000, 2001]))
    ][["d_date_sk"]]

    store = pd.read_parquet(
        f"{path}/store{suffix}",
        columns=["s_store_sk", "s_number_employees", "s_city"],
    )
    store = store[
        (store["s_number_employees"] >= 200)
        & (store["s_number_employees"] <= 295)
    ][["s_store_sk", "s_city"]]

    household = pd.read_parquet(
        f"{path}/household_demographics{suffix}",
        columns=["hd_demo_sk", "hd_dep_count", "hd_vehicle_count"],
    )
    household = household[
        (household["hd_dep_count"] == 6) | (household["hd_vehicle_count"] > 2)
    ][["hd_demo_sk"]]

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_store_sk",
            "ss_hdemo_sk",
            "ss_ticket_number",
            "ss_customer_sk",
            "ss_addr_sk",
            "ss_coupon_amt",
            "ss_net_profit",
        ],
    )
    joined = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    )
    joined = joined.merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    joined = joined.merge(
        household, left_on="ss_hdemo_sk", right_on="hd_demo_sk"
    )

    joined["amt"] = _cents(joined["ss_coupon_amt"])
    joined["profit"] = _cents(joined["ss_net_profit"])
    joined["amt_seen"] = joined["amt"].notna().astype("int64")
    joined["profit_seen"] = joined["profit"].notna().astype("int64")

    keys = ["ss_ticket_number", "ss_customer_sk", "ss_addr_sk", "s_city"]
    ms = (
        joined.groupby(keys, dropna=False)[
            ["amt", "profit", "amt_seen", "profit_seen"]
        ]
        .sum()
        .reset_index()
    )
    # SUM over a group whose values are all NULL is NULL, not zero.
    ms["amt"] = ms["amt"].where(ms["amt_seen"] > 0)
    ms["profit"] = ms["profit"].where(ms["profit_seen"] > 0)

    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=["c_customer_sk", "c_last_name", "c_first_name"],
    )
    result = ms.merge(
        customer, left_on="ss_customer_sk", right_on="c_customer_sk"
    )
    result["city"] = result["s_city"].str.slice(0, 30)

    result = result.sort_values(
        ["c_last_name", "c_first_name", "city", "profit", "ss_ticket_number"],
        na_position="first",
    ).head(100)
    result["amt_str"] = _decimal_str(result["amt"])
    result["profit_str"] = _decimal_str(result["profit"])
    return result[
        [
            "c_last_name",
            "c_first_name",
            "city",
            "ss_ticket_number",
            "amt_str",
            "profit_str",
        ]
    ].reset_index(drop=True)
