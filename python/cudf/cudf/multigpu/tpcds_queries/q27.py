# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 27.Average Tennessee store sale per item, with the per-item and overall rollups."""

from __future__ import annotations

import pandas as pd

_AGGS = ["agg1", "agg2", "agg3", "agg4"]


def _nulled(series):
    """A column of the same dtype as ``series`` holding only nulls."""
    return series.where(series.isna())


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_item_sk",
            "ss_store_sk",
            "ss_cdemo_sk",
            "ss_quantity",
            "ss_list_price",
            "ss_coupon_amt",
            "ss_sales_price",
        ],
    )
    customer_demographics = pd.read_parquet(
        f"{path}/customer_demographics{suffix}",
        columns=[
            "cd_demo_sk",
            "cd_gender",
            "cd_marital_status",
            "cd_education_status",
        ],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_state"]
    )
    item = pd.read_parquet(f"{path}/item{suffix}", columns=["i_item_sk", "i_item_id"])

    customer_demographics = customer_demographics[
        (customer_demographics["cd_gender"] == "M")
        & (customer_demographics["cd_marital_status"] == "S")
        & (customer_demographics["cd_education_status"] == "College")
    ][["cd_demo_sk"]]
    date_dim = date_dim[date_dim["d_year"] == 2002][["d_date_sk"]]
    store = store[(store["s_state"] == "TN").fillna(False)][
        ["s_store_sk", "s_state"]
    ]

    df = store_sales.merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
    df = df.merge(customer_demographics, left_on="ss_cdemo_sk", right_on="cd_demo_sk")
    df = df.merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    df = df.merge(item, left_on="ss_item_sk", right_on="i_item_sk")

    results = df.assign(
        agg1=df["ss_quantity"].astype("float64"),
        agg2=df["ss_list_price"].astype("float64"),
        agg3=df["ss_coupon_amt"].astype("float64"),
        agg4=df["ss_sales_price"].astype("float64"),
    )[["i_item_id", "s_state", *_AGGS]]

    by_state = results.groupby(
        ["i_item_id", "s_state"], as_index=False, dropna=False
    )[_AGGS].mean()
    by_state = by_state.assign(g_state=0)

    by_item = results.groupby("i_item_id", as_index=False, dropna=False)[_AGGS].mean()
    by_item = by_item.assign(g_state=1)
    by_item["s_state"] = _nulled(by_item["i_item_id"])

    overall = by_item.head(1).copy()
    overall["i_item_id"] = _nulled(overall["i_item_id"])
    overall["s_state"] = _nulled(overall["s_state"])
    overall["g_state"] = 1
    for column in _AGGS:
        overall[column] = results[column].mean()

    columns = ["i_item_id", "s_state", "g_state", *_AGGS]
    out = pd.concat(
        [by_state[columns], by_item[columns], overall[columns]], ignore_index=True
    )
    out = out.sort_values(["i_item_id", "s_state"], na_position="first").head(100)
    return out.reset_index(drop=True)
