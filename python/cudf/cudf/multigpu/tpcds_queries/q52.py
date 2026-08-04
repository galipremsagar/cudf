# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 52. Extended sales price per brand for manager 1 in November 2000."""

from __future__ import annotations

import pandas as pd


def _sql_sum(frame, keys, value, name):
    """``sum(value)`` grouped by ``keys``, NULL when every input is NULL."""
    frame = frame.assign(_nonnull=frame[value].notna().astype("int64"))
    grouped = frame.groupby(keys, as_index=False, dropna=False)[
        [value, "_nonnull"]
    ].sum()
    grouped[value] = grouped[value].where(grouped["_nonnull"] > 0)
    return grouped.drop(columns=["_nonnull"]).rename(columns={value: name})


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

    dates = date_dim[(date_dim["d_moy"] == 11) & (date_dim["d_year"] == 2000)][
        ["d_date_sk", "d_year"]
    ]
    items = item[item["i_manager_id"] == 1][["i_item_sk", "i_brand_id", "i_brand"]]

    joined = store_sales.merge(
        dates, left_on="ss_sold_date_sk", right_on="d_date_sk"
    ).merge(items, left_on="ss_item_sk", right_on="i_item_sk")

    # libcudf has no group-by sum for fixed-point columns; a decimal(7,2) money
    # value and a twelve-thousand-row sum of them are both exact in float64.
    joined = joined.assign(
        ss_ext_sales_price=joined["ss_ext_sales_price"].astype("float64")
    )

    grouped = _sql_sum(
        joined, ["d_year", "i_brand", "i_brand_id"], "ss_ext_sales_price", "ext_price"
    )
    grouped = grouped.rename(columns={"i_brand_id": "brand_id", "i_brand": "brand"})

    result = grouped.sort_values(
        ["d_year", "ext_price", "brand_id"], ascending=[True, False, True]
    ).head(100)
    return result[["d_year", "brand_id", "brand", "ext_price"]].reset_index(drop=True)
