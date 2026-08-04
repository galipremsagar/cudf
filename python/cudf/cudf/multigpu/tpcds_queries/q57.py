# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 57. Call-centre catalog months whose sales stray from the yearly average, with neighbours."""

from __future__ import annotations

import pandas as pd

KEYS = ["i_category", "i_brand", "cc_name"]


def _sql_sum(frame, keys, value, name):
    """``sum(value)`` grouped by ``keys``, NULL when every input is NULL."""
    frame = frame.assign(_nonnull=frame[value].notna())
    grouped = frame.groupby(keys, as_index=False)[[value, "_nonnull"]].sum()
    grouped[value] = grouped[value].where(grouped["_nonnull"] > 0)
    return grouped.drop(columns=["_nonnull"]).rename(columns={value: name})


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    item = pd.read_parquet(
        f"{base}/item{suffix}", columns=["i_item_sk", "i_category", "i_brand"]
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    call_center = pd.read_parquet(
        f"{base}/call_center{suffix}", columns=["cc_call_center_sk", "cc_name"]
    )
    catalog_sales = pd.read_parquet(
        f"{base}/catalog_sales{suffix}",
        columns=[
            "cs_item_sk",
            "cs_sold_date_sk",
            "cs_call_center_sk",
            "cs_sales_price",
        ],
    )

    dates = date_dim[
        (date_dim["d_year"] == 1999)
        | ((date_dim["d_year"] == 1998) & (date_dim["d_moy"] == 12))
        | ((date_dim["d_year"] == 2000) & (date_dim["d_moy"] == 1))
    ][["d_date_sk", "d_year", "d_moy"]]

    joined = (
        catalog_sales.merge(item, left_on="cs_item_sk", right_on="i_item_sk")
        .merge(dates, left_on="cs_sold_date_sk", right_on="d_date_sk")
        .merge(call_center, left_on="cs_call_center_sk", right_on="cc_call_center_sk")
    )

    # Rows whose category/brand/call-centre is NULL never satisfy the equijoins
    # of v2, so dropping them here (as the default groupby does) is faithful.
    v1 = _sql_sum(
        joined, KEYS + ["d_year", "d_moy"], "cs_sales_price", "sum_sales"
    )
    v1["_sum"] = v1["sum_sales"].astype("float64")
    v1["avg_monthly_sales"] = v1.groupby(KEYS + ["d_year"])["_sum"].transform("mean")

    v1 = v1.sort_values(KEYS + ["d_year", "d_moy"]).reset_index(drop=True)
    v1["rn"] = v1.groupby(KEYS).cumcount() + 1

    lag = v1[KEYS + ["rn", "sum_sales"]].rename(columns={"sum_sales": "psum"})
    lag = lag.assign(rn=lag["rn"] + 1)
    lead = v1[KEYS + ["rn", "sum_sales"]].rename(columns={"sum_sales": "nsum"})
    lead = lead.assign(rn=lead["rn"] - 1)

    v2 = v1.merge(lag, on=KEYS + ["rn"]).merge(lead, on=KEYS + ["rn"])

    average = v2["avg_monthly_sales"]
    deviation = (v2["_sum"] - average).abs() / average
    selected = v2[(v2["d_year"] == 1999) & (average > 0) & (deviation > 0.1)].copy()

    selected["_diff"] = selected["_sum"] - selected["avg_monthly_sales"]
    selected["_psum"] = selected["psum"].astype("float64")
    selected["_nsum"] = selected["nsum"].astype("float64")

    result = selected.sort_values(
        [
            "_diff",
            "i_category",
            "i_brand",
            "cc_name",
            "d_year",
            "d_moy",
            "avg_monthly_sales",
            "_sum",
            "_psum",
            "_nsum",
        ],
        na_position="first",
    ).head(100)
    return result[
        [
            "i_category",
            "i_brand",
            "cc_name",
            "d_year",
            "d_moy",
            "avg_monthly_sales",
            "sum_sales",
            "psum",
            "nsum",
        ]
    ].reset_index(drop=True)
