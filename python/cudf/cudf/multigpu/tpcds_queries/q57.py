# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 57. Call-centre catalog months whose sales stray from the yearly average, with neighbours."""

from __future__ import annotations

import pandas as pd

KEYS = ["i_category", "i_brand", "cc_name"]


def _sql_sum(frame, keys, value, name):
    """``sum(value)`` grouped by ``keys``, NULL when every input is NULL.

    The money column is cast to float64 first: libcudf has no group-by sum for
    fixed-point columns, and TPC-DS amounts are far inside float64's exactly
    representable range.
    """
    values = frame[value].astype("float64")
    frame = frame.assign(_value=values, _nonnull=values.notna())
    grouped = frame.groupby(keys, as_index=False)[["_value", "_nonnull"]].sum()
    grouped[name] = grouped["_value"].where(grouped["_nonnull"] > 0)
    return grouped.drop(columns=["_value", "_nonnull"])


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
    # avg(sum(...)) over (partition by i_category, i_brand, cc_name, d_year),
    # as a group-by plus a merge: the multi-GPU layer has no groupby.transform.
    averages = (
        v1.groupby(KEYS + ["d_year"], as_index=False)["sum_sales"]
        .mean()
        .rename(columns={"sum_sales": "avg_monthly_sales"})
    )
    v1 = v1.merge(averages, on=KEYS + ["d_year"])

    # rank() over (partition by KEYS order by d_year, d_moy).  (d_year, d_moy)
    # is unique inside a partition, so the rank of a month is just how many
    # months of that partition are at or before it -- a self-join and a count,
    # which is what the multi-GPU layer supports.  Each partition holds at most
    # the 14 months the date filter admits, so the self-join stays tiny.
    v1 = v1.assign(_month=v1["d_year"] * 12 + v1["d_moy"])
    months = v1[KEYS + ["_month"]]
    pairs = months.merge(
        months.rename(columns={"_month": "_earlier"}), on=KEYS
    )
    ranks = (
        pairs[pairs["_earlier"] <= pairs["_month"]]
        .groupby(KEYS + ["_month"], as_index=False)
        .agg(rn=("_earlier", "size"))
    )
    v1 = v1.merge(ranks, on=KEYS + ["_month"])

    lag = v1[KEYS + ["rn", "sum_sales"]].rename(columns={"sum_sales": "psum"})
    lag = lag.assign(rn=lag["rn"] + 1)
    lead = v1[KEYS + ["rn", "sum_sales"]].rename(columns={"sum_sales": "nsum"})
    lead = lead.assign(rn=lead["rn"] - 1)

    # Projecting the joined frame down to the columns the rest of the query
    # needs also runs the joins, so the columns below are plain series rather
    # than expressions still pending against them.
    v2 = v1.merge(lag, on=KEYS + ["rn"]).merge(lead, on=KEYS + ["rn"])[
        KEYS
        + ["d_year", "d_moy", "avg_monthly_sales", "sum_sales", "psum", "nsum"]
    ]

    average = v2["avg_monthly_sales"]
    deviation = (v2["sum_sales"] - average).abs() / average
    selected = v2[(v2["d_year"] == 1999) & (average > 0) & (deviation > 0.1)]

    selected = selected.assign(
        _diff=selected["sum_sales"] - selected["avg_monthly_sales"]
    )

    result = selected.sort_values(
        [
            "_diff",
            "i_category",
            "i_brand",
            "cc_name",
            "d_year",
            "d_moy",
            "avg_monthly_sales",
            "sum_sales",
            "psum",
            "nsum",
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
