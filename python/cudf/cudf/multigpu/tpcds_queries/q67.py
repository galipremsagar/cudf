# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 67.The hundred best-selling rollup combinations of item, date
and store within each category over a year."""

from __future__ import annotations

from decimal import Decimal

import pandas as pd

ZERO = Decimal("0.00")

KEYS = [
    "i_category",
    "i_class",
    "i_brand",
    "i_product_name",
    "d_year",
    "d_qoy",
    "d_moy",
    "s_store_id",
]

NUMERIC_KEYS = {"d_year", "d_qoy", "d_moy"}


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_item_sk",
            "ss_store_sk",
            "ss_sales_price",
            "ss_quantity",
        ],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}",
        columns=["d_date_sk", "d_month_seq", "d_year", "d_qoy", "d_moy"],
    )
    date_dim = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1211)
    ][["d_date_sk", "d_year", "d_qoy", "d_moy"]]
    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_store_id"]
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}",
        columns=[
            "i_item_sk",
            "i_category",
            "i_class",
            "i_brand",
            "i_product_name",
        ],
    )

    df = (
        store_sales.merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
        .merge(item, left_on="ss_item_sk", right_on="i_item_sk")
        .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    )

    # coalesce(ss_sales_price*ss_quantity, 0)
    known = df["ss_sales_price"].notna() & df["ss_quantity"].notna()
    quantity = df["ss_quantity"].fillna(0).astype("int64")
    df = df.assign(
        sumsales=(df["ss_sales_price"].fillna(ZERO) * quantity).where(known, ZERO)
    )

    # GROUP BY rollup(...): the full key, then one column dropped at a time.
    finest = df.groupby(KEYS, dropna=False)["sumsales"].sum().reset_index()
    levels = [finest]
    current = finest
    for depth in range(len(KEYS) - 1, 0, -1):
        current = (
            current.groupby(KEYS[:depth], dropna=False)["sumsales"].sum().reset_index()
        )
        levels.append(current)
    total = current["sumsales"].sum()
    levels.append(pd.DataFrame({"sumsales": [total]}))

    padded = []
    for level in levels:
        missing = {
            key: (float("nan") if key in NUMERIC_KEYS else None)
            for key in KEYS
            if key not in level.columns
        }
        padded.append(level.assign(**missing)[KEYS + ["sumsales"]])
    rollup = pd.concat(padded, ignore_index=True)

    rollup["_sumsales"] = rollup["sumsales"].astype("float64")
    rollup["rk"] = rollup.groupby("i_category", dropna=False)["_sumsales"].rank(
        method="min", ascending=False
    )
    rollup = rollup[rollup["rk"] <= 100]

    rollup = rollup.sort_values(
        KEYS + ["_sumsales", "rk"], na_position="first"
    ).head(100)
    return rollup[KEYS + ["sumsales", "rk"]].reset_index(drop=True)
