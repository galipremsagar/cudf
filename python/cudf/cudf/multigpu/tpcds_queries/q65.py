# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 65.Items whose revenue in a store is no more than a tenth of
that store's average item revenue over a year."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=["ss_store_sk", "ss_item_sk", "ss_sold_date_sk", "ss_sales_price"],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_month_seq"]
    )
    date_dim = date_dim[
        (date_dim["d_month_seq"] >= 1176) & (date_dim["d_month_seq"] <= 1187)
    ][["d_date_sk"]]

    sold = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    )
    # libcudf has no group-by sum for decimals, so the price becomes float64
    # first; TPC-DS money is far inside float64's exactly representable range.
    sold = sold.assign(ss_sales_price=sold["ss_sales_price"].astype("float64"))
    by_item = (
        sold.groupby(["ss_store_sk", "ss_item_sk"], dropna=False)
        .agg(revenue=("ss_sales_price", "sum"), n=("ss_sales_price", "count"))
        .reset_index()
    )
    # SUM over an all-NULL group is NULL, which neither reaches the average nor
    # satisfies the comparison below.
    by_item = by_item[by_item["n"] > 0]
    # avg(revenue) per store: a group-by plus a merge, which is what the
    # multi-GPU layer supports -- groupby().transform() is not distributed.
    ave = (
        by_item.groupby("ss_store_sk", dropna=False, as_index=False)["revenue"]
        .mean()
        .rename(columns={"revenue": "ave"})
    )
    by_item = by_item.merge(ave, on="ss_store_sk")
    by_item = by_item[by_item["revenue"] <= 0.1 * by_item["ave"]]

    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_store_name"]
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}",
        columns=[
            "i_item_sk",
            "i_item_desc",
            "i_current_price",
            "i_wholesale_cost",
            "i_brand",
        ],
    )

    joined = by_item.merge(
        store, left_on="ss_store_sk", right_on="s_store_sk"
    ).merge(item, left_on="ss_item_sk", right_on="i_item_sk")

    joined = joined.sort_values(
        ["s_store_name", "i_item_desc"], na_position="first"
    ).head(100)
    result = joined[
        [
            "s_store_name",
            "i_item_desc",
            "revenue",
            "i_current_price",
            "i_wholesale_cost",
            "i_brand",
        ]
    ]
    return result.reset_index(drop=True)
