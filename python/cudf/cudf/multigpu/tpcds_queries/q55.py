# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 55. Extended sales price per brand for manager 28 in November 1999."""

from __future__ import annotations

import pandas as pd


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

    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}",
        columns=["d_date_sk", "d_year", "d_moy"],
    )
    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=["ss_sold_date_sk", "ss_item_sk", "ss_ext_sales_price"],
    )
    item = pd.read_parquet(
        f"{base}/item{suffix}",
        columns=["i_item_sk", "i_brand_id", "i_brand", "i_manager_id"],
    )

    dates = date_dim[(date_dim["d_moy"] == 11) & (date_dim["d_year"] == 1999)][
        ["d_date_sk"]
    ]
    items = item[item["i_manager_id"] == 28][["i_item_sk", "i_brand_id", "i_brand"]]

    joined = store_sales.merge(
        dates, left_on="ss_sold_date_sk", right_on="d_date_sk"
    ).merge(items, left_on="ss_item_sk", right_on="i_item_sk")

    grouped = _sql_sum(
        joined, ["i_brand", "i_brand_id"], "ss_ext_sales_price", "ext_price"
    )
    grouped = grouped.rename(columns={"i_brand_id": "brand_id", "i_brand": "brand"})

    result = grouped.sort_values(
        ["ext_price", "brand_id"], ascending=[False, True]
    ).head(100)
    return result[["brand_id", "brand", "ext_price"]].reset_index(drop=True)
