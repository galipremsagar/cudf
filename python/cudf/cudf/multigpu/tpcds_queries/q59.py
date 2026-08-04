# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 59. Weekday store sales of one year over the same week a year earlier."""

from __future__ import annotations

import pandas as pd

DAYS = [
    ("Sunday", "sun_sales"),
    ("Monday", "mon_sales"),
    ("Tuesday", "tue_sales"),
    ("Wednesday", "wed_sales"),
    ("Thursday", "thu_sales"),
    ("Friday", "fri_sales"),
    ("Saturday", "sat_sales"),
]


def _half(wss, store, date_dim, low, high, tag):
    weeks = date_dim[
        (date_dim["d_month_seq"] >= low) & (date_dim["d_month_seq"] <= high)
    ][["d_week_seq"]]
    joined = wss.merge(
        store, left_on="ss_store_sk", right_on="s_store_sk"
    ).merge(weeks, on="d_week_seq")
    columns = ["s_store_name", "d_week_seq", "s_store_id"] + [c for _, c in DAYS]
    joined = joined[columns]
    return joined.rename(columns={c: f"{c}{tag}" for c in columns})


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}",
        columns=["d_date_sk", "d_week_seq", "d_day_name", "d_month_seq"],
    )
    store = pd.read_parquet(
        f"{base}/store{suffix}", columns=["s_store_sk", "s_store_id", "s_store_name"]
    )
    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=["ss_sold_date_sk", "ss_store_sk", "ss_sales_price"],
    )

    daily = store_sales.merge(
        date_dim[["d_date_sk", "d_week_seq", "d_day_name"]],
        left_on="ss_sold_date_sk",
        right_on="d_date_sk",
    )
    # libcudf cannot group-by-sum a fixed-point column, so the price moves to
    # float64 first; TPC-DS amounts are well inside float64's exact range.
    price = daily["ss_sales_price"].astype("float64")
    per_day = {}
    for day, column in DAYS:
        value = price.where(daily["d_day_name"] == day)
        per_day[column] = value
        # A SUM over an all-NULL group is NULL, not zero, and these sums land
        # in a division, so the distinction has to survive the aggregation.
        per_day[f"{column}_n"] = value.notna()
    daily = daily.assign(**per_day)

    wss = daily.groupby(["d_week_seq", "ss_store_sk"], as_index=False)[
        list(per_day)
    ].sum()
    wss = wss.assign(
        **{
            column: wss[column].where(wss[f"{column}_n"] > 0)
            for _, column in DAYS
        }
    ).drop(columns=[f"{c}_n" for _, c in DAYS])

    this_year = _half(wss, store, date_dim, 1212, 1212 + 11, "1")
    last_year = _half(wss, store, date_dim, 1212 + 12, 1212 + 23, "2")
    last_year = last_year.assign(_week_key=last_year["d_week_seq2"] - 52)

    paired = this_year.merge(
        last_year,
        left_on=["s_store_id1", "d_week_seq1"],
        right_on=["s_store_id2", "_week_key"],
    )

    result = paired.sort_values(
        ["s_store_name1", "s_store_id1", "d_week_seq1"], na_position="first"
    ).head(100)

    ratios = {}
    for _, column in DAYS:
        denominator = result[f"{column}2"]
        # SQL division by zero is NULL, not an infinity.
        ratios[f"{column}_ratio"] = result[f"{column}1"] / denominator.where(
            denominator != 0
        )
    result = result.assign(**ratios)

    return result[
        ["s_store_name1", "s_store_id1", "d_week_seq1"]
        + [f"{c}_ratio" for _, c in DAYS]
    ].reset_index(drop=True)
