# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 47.Category/brand/store months of 1999 whose sales stray more than 10% from the yearly monthly average, with the neighbouring months' totals."""

from __future__ import annotations

import pandas as pd

_KEYS = ["i_category", "i_brand", "s_store_name", "s_company_name"]


def query(run_config):
    item = pd.read_parquet(
        f"{run_config.dataset_path}/item{run_config.suffix}",
        columns=["i_item_sk", "i_category", "i_brand"],
    )
    store_sales = pd.read_parquet(
        f"{run_config.dataset_path}/store_sales{run_config.suffix}",
        columns=["ss_item_sk", "ss_sold_date_sk", "ss_store_sk", "ss_sales_price"],
    )
    date_dim = pd.read_parquet(
        f"{run_config.dataset_path}/date_dim{run_config.suffix}",
        columns=["d_date_sk", "d_year", "d_moy"],
    )
    store = pd.read_parquet(
        f"{run_config.dataset_path}/store{run_config.suffix}",
        columns=["s_store_sk", "s_store_name", "s_company_name"],
    )

    date_dim = date_dim[
        (date_dim["d_year"] == 1999)
        | ((date_dim["d_year"] == 1998) & (date_dim["d_moy"] == 12))
        | ((date_dim["d_year"] == 2000) & (date_dim["d_moy"] == 1))
    ]

    merged = (
        store_sales.merge(item, left_on="ss_item_sk", right_on="i_item_sk")
        .merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
        .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    )

    # libcudf has no group-by sum for fixed-point columns; and avg() over a
    # decimal sum is a double in SQL, so the window average and every
    # comparison against it are floating point regardless.
    merged = merged.assign(
        ss_sales_price=merged["ss_sales_price"].astype("float64")
    )

    # The self-joins below are equijoins, so a row with a NULL key matches
    # nothing, and a NULL key also puts a row in a window partition of its own
    # -- so the default dropna=True, which drops those groups here, cannot
    # change any surviving row's average or rank.
    v1 = (
        merged.groupby(_KEYS + ["d_year", "d_moy"])["ss_sales_price"]
        .sum()
        .reset_index()
        .rename(columns={"ss_sales_price": "sum_sales"})
    )

    average = (
        v1.groupby(_KEYS + ["d_year"], as_index=False)["sum_sales"]
        .mean()
        .rename(columns={"sum_sales": "avg_monthly_sales"})
    )
    v1 = v1.merge(average, on=_KEYS + ["d_year"])

    # (d_year, d_moy) is unique inside a partition, so rank() over that order
    # is a plain row number. It is read off a global running count: the sort
    # makes each partition contiguous, so the count at a row minus the count at
    # the partition's first row is the position within the partition.
    v1 = v1.sort_values(_KEYS + ["d_year", "d_moy"]).assign(_one=1)
    v1["_running"] = v1["_one"].cumsum()
    starts = (
        v1.groupby(_KEYS, as_index=False)["_running"]
        .min()
        .rename(columns={"_running": "_start"})
    )
    v1 = v1.merge(starts, on=_KEYS)
    v1["rn"] = v1["_running"] - v1["_start"] + 1

    lag = v1[_KEYS + ["rn", "sum_sales"]].rename(columns={"sum_sales": "psum"})
    lag["rn"] = lag["rn"] + 1
    lead = v1[_KEYS + ["rn", "sum_sales"]].rename(columns={"sum_sales": "nsum"})
    lead["rn"] = lead["rn"] - 1

    v2 = v1.merge(lag, on=_KEYS + ["rn"]).merge(lead, on=_KEYS + ["rn"])

    v2 = v2.assign(_delta=v2["sum_sales"] - v2["avg_monthly_sales"])
    v2 = v2[
        (v2["d_year"] == 1999)
        & (v2["avg_monthly_sales"] > 0)
        & (v2["_delta"].abs() / v2["avg_monthly_sales"] > 0.1)
    ]

    columns = _KEYS + [
        "d_year",
        "d_moy",
        "avg_monthly_sales",
        "sum_sales",
        "psum",
        "nsum",
    ]

    result = (
        v2.sort_values(["_delta"] + columns)
        .head(100)
        .reset_index(drop=True)
    )
    return result[columns]
