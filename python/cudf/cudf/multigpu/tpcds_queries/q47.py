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

    v1 = (
        merged.groupby(_KEYS + ["d_year", "d_moy"], dropna=False)["ss_sales_price"]
        .sum()
        .reset_index()
        .rename(columns={"ss_sales_price": "sum_sales"})
    )

    # avg() over a decimal sum is a double in SQL, so the window average and
    # every comparison against it are done in floating point; sum_sales itself
    # stays exact.
    v1["_sales"] = v1["sum_sales"].astype("float64")
    v1["avg_monthly_sales"] = v1.groupby(_KEYS + ["d_year"], dropna=False)[
        "_sales"
    ].transform("mean")

    # (d_year, d_moy) is unique inside a partition, so rank() over that order
    # is a plain row number.
    v1 = v1.sort_values(_KEYS + ["d_year", "d_moy"]).reset_index(drop=True)
    v1["rn"] = v1.groupby(_KEYS, dropna=False).cumcount() + 1

    # The self-joins are equijoins, so rows with a NULL key match nothing.
    v1 = v1.dropna(subset=_KEYS)

    lag = v1[_KEYS + ["rn", "sum_sales"]].rename(columns={"sum_sales": "psum"})
    lag["rn"] = lag["rn"] + 1
    lead = v1[_KEYS + ["rn", "sum_sales"]].rename(columns={"sum_sales": "nsum"})
    lead["rn"] = lead["rn"] - 1

    v2 = v1.merge(lag, on=_KEYS + ["rn"]).merge(lead, on=_KEYS + ["rn"])

    v2 = v2.assign(_delta=v2["_sales"] - v2["avg_monthly_sales"])
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
