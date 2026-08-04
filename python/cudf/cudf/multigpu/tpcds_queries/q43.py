# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 43.Weekday-by-weekday sales totals for each store in the GMT-5 zone during 2000."""

from __future__ import annotations

import pandas as pd

_DAYS = [
    ("Sunday", "sun_sales"),
    ("Monday", "mon_sales"),
    ("Tuesday", "tue_sales"),
    ("Wednesday", "wed_sales"),
    ("Thursday", "thu_sales"),
    ("Friday", "fri_sales"),
    ("Saturday", "sat_sales"),
]


def query(run_config):
    date_dim = pd.read_parquet(
        f"{run_config.dataset_path}/date_dim{run_config.suffix}",
        columns=["d_date_sk", "d_year", "d_day_name"],
    )
    store_sales = pd.read_parquet(
        f"{run_config.dataset_path}/store_sales{run_config.suffix}",
        columns=["ss_sold_date_sk", "ss_store_sk", "ss_sales_price"],
    )
    store = pd.read_parquet(
        f"{run_config.dataset_path}/store{run_config.suffix}",
        columns=["s_store_sk", "s_store_name", "s_store_id", "s_gmt_offset"],
    )

    date_dim = date_dim[date_dim["d_year"] == 2000]
    store = store.assign(s_gmt_offset=store["s_gmt_offset"].astype("float64"))
    store = store[store["s_gmt_offset"] == -5]

    merged = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    ).merge(store, left_on="ss_store_sk", right_on="s_store_sk")

    keys = ["s_store_name", "s_store_id"]
    per_day = (
        merged.groupby(keys + ["d_day_name"], dropna=False)["ss_sales_price"]
        .sum()
        .reset_index()
    )

    # One group per store, as GROUP BY gives, then a column per weekday. A
    # weekday a store never sold on stays NULL, which is what summing over an
    # empty CASE arm does.
    grouped = merged.groupby(keys, dropna=False).size().reset_index()[keys]
    value_columns = []
    for name, column in _DAYS:
        day = per_day.loc[
            per_day["d_day_name"] == name, keys + ["ss_sales_price"]
        ].rename(columns={"ss_sales_price": column})
        grouped = grouped.merge(day, on=keys, how="left")
        value_columns.append(column)

    result = (
        grouped.sort_values(keys + value_columns)
        .head(100)
        .reset_index(drop=True)
    )
    return result[keys + value_columns]
