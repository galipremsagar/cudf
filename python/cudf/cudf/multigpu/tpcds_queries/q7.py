# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 7. Average promoted store sales in 2000 for one customer demographic."""

from __future__ import annotations

import pandas as pd

_MEASURES = ["ss_quantity", "ss_list_price", "ss_coupon_amt", "ss_sales_price"]


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_item_sk",
            "ss_cdemo_sk",
            "ss_promo_sk",
            *_MEASURES,
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
    item = pd.read_parquet(f"{path}/item{suffix}", columns=["i_item_sk", "i_item_id"])
    promotion = pd.read_parquet(
        f"{path}/promotion{suffix}",
        columns=["p_promo_sk", "p_channel_email", "p_channel_event"],
    )

    demographics = customer_demographics[
        (customer_demographics["cd_gender"] == "M")
        & (customer_demographics["cd_marital_status"] == "S")
        & (customer_demographics["cd_education_status"] == "College")
    ][["cd_demo_sk"]]
    dates = date_dim[date_dim["d_year"] == 2000][["d_date_sk"]]
    # Each comparison is made null-free before the OR. cuDF's boolean OR does
    # not implement SQL three-valued logic: NULL | True yields NULL, not True,
    # so a row qualifying on one channel is dropped when another channel is
    # NULL. pandas never reaches that case because NaN == "Y" is already False.
    # cuDF differs from both, and this cost q61 3 promotion rows and 39,529 in
    # revenue against DuckDB.
    promotions = promotion[
        (promotion["p_channel_email"] == "N").fillna(False)
        | (promotion["p_channel_event"] == "N").fillna(False)
    ][["p_promo_sk"]]

    for column in _MEASURES:
        store_sales[column] = store_sales[column].astype("float64")

    joined = (
        store_sales.merge(
            dates, left_on="ss_sold_date_sk", right_on="d_date_sk", how="inner"
        )
        .merge(item, left_on="ss_item_sk", right_on="i_item_sk", how="inner")
        .merge(demographics, left_on="ss_cdemo_sk", right_on="cd_demo_sk", how="inner")
        .merge(promotions, left_on="ss_promo_sk", right_on="p_promo_sk", how="inner")
    )

    grouped = (
        joined.groupby("i_item_id", dropna=False)[_MEASURES].mean().reset_index()
    )
    grouped.columns = ["i_item_id", "agg1", "agg2", "agg3", "agg4"]
    grouped = grouped.sort_values("i_item_id", na_position="first", kind="stable")
    return grouped.head(100).reset_index(drop=True)
