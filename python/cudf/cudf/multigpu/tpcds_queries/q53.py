# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 53. Manufacturers whose quarterly store sales stray from their own yearly average."""

from __future__ import annotations

import pandas as pd

MONTHS = [1200 + i for i in range(12)]

CATEGORIES_A = ["Books", "Children", "Electronics"]
CLASSES_A = ["personal", "portable", "reference", "self-help"]
BRANDS_A = [
    "scholaramalgamalg #14",
    "scholaramalgamalg #7",
    "exportiunivamalg #9",
    "scholaramalgamalg #9",
]
CATEGORIES_B = ["Women", "Music", "Men"]
CLASSES_B = ["accessories", "classical", "fragrances", "pants"]
BRANDS_B = [
    "amalgimporto #1",
    "edu packscholar #1",
    "exportiimporto #1",
    "importoamalg #1",
]


def _sql_sum(frame, keys, value, name):
    """``sum(value)`` grouped by ``keys``, NULL when every input is NULL.

    The money column is cast to float64 first: libcudf has no group-by sum for
    fixed-point columns, and TPC-DS amounts are far inside float64's exactly
    representable range.
    """
    values = frame[value].astype("float64")
    frame = frame.assign(_value=values, _nonnull=values.notna())
    grouped = frame.groupby(keys, as_index=False, dropna=False)[
        ["_value", "_nonnull"]
    ].sum()
    grouped[name] = grouped["_value"].where(grouped["_nonnull"] > 0)
    return grouped.drop(columns=["_value", "_nonnull"])


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    item = pd.read_parquet(
        f"{base}/item{suffix}",
        columns=["i_item_sk", "i_manufact_id", "i_category", "i_class", "i_brand"],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}",
        columns=["d_date_sk", "d_month_seq", "d_qoy"],
    )
    store = pd.read_parquet(f"{base}/store{suffix}", columns=["s_store_sk"])
    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=["ss_item_sk", "ss_sold_date_sk", "ss_store_sk", "ss_sales_price"],
    )

    keep = (
        item["i_category"].isin(CATEGORIES_A)
        & item["i_class"].isin(CLASSES_A)
        & item["i_brand"].isin(BRANDS_A)
    ) | (
        item["i_category"].isin(CATEGORIES_B)
        & item["i_class"].isin(CLASSES_B)
        & item["i_brand"].isin(BRANDS_B)
    )
    items = item[keep][["i_item_sk", "i_manufact_id"]]
    dates = date_dim[date_dim["d_month_seq"].isin(MONTHS)][["d_date_sk", "d_qoy"]]

    joined = (
        store_sales.merge(items, left_on="ss_item_sk", right_on="i_item_sk")
        .merge(dates, left_on="ss_sold_date_sk", right_on="d_date_sk")
        .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    )

    grouped = _sql_sum(
        joined, ["i_manufact_id", "d_qoy"], "ss_sales_price", "sum_sales"
    )
    # avg(sum(...)) over (partition by i_manufact_id), expressed as a group-by
    # plus a merge -- the multi-GPU layer has no group-by ``transform``.
    averages = (
        grouped.groupby("i_manufact_id", as_index=False, dropna=False)["sum_sales"]
        .mean()
        .rename(columns={"sum_sales": "avg_quarterly_sales"})
    )
    # Projecting the joined frame down to the three columns the rest of the
    # query needs also runs the join, so the columns below are plain series
    # rather than expressions still pending against it.
    grouped = grouped.merge(averages, on="i_manufact_id")[
        ["i_manufact_id", "sum_sales", "avg_quarterly_sales"]
    ]

    average = grouped["avg_quarterly_sales"]
    deviation = (grouped["sum_sales"] - average).abs() / average
    selected = grouped[(average > 0) & (deviation > 0.1)]

    result = selected.sort_values(
        ["avg_quarterly_sales", "sum_sales", "i_manufact_id"]
    ).head(100)
    return result[["i_manufact_id", "sum_sales", "avg_quarterly_sales"]].reset_index(
        drop=True
    )
