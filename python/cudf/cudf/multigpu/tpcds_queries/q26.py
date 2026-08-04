# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 26.Average catalog sale per item for single male college graduates in 2000."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=[
            "cs_sold_date_sk",
            "cs_item_sk",
            "cs_bill_cdemo_sk",
            "cs_promo_sk",
            "cs_quantity",
            "cs_list_price",
            "cs_coupon_amt",
            "cs_sales_price",
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

    customer_demographics = customer_demographics[
        (customer_demographics["cd_gender"] == "M")
        & (customer_demographics["cd_marital_status"] == "S")
        & (customer_demographics["cd_education_status"] == "College")
    ][["cd_demo_sk"]]
    date_dim = date_dim[date_dim["d_year"] == 2000][["d_date_sk"]]
    # NULL OR TRUE is TRUE in SQL, so each side is resolved before the OR.
    promotion = promotion[
        (promotion["p_channel_email"] == "N").fillna(False)
        | (promotion["p_channel_event"] == "N").fillna(False)
    ][["p_promo_sk"]]

    df = catalog_sales.merge(date_dim, left_on="cs_sold_date_sk", right_on="d_date_sk")
    df = df.merge(
        customer_demographics, left_on="cs_bill_cdemo_sk", right_on="cd_demo_sk"
    )
    df = df.merge(promotion, left_on="cs_promo_sk", right_on="p_promo_sk")
    df = df.merge(item, left_on="cs_item_sk", right_on="i_item_sk")

    df = df.assign(
        agg1=df["cs_quantity"].astype("float64"),
        agg2=df["cs_list_price"].astype("float64"),
        agg3=df["cs_coupon_amt"].astype("float64"),
        agg4=df["cs_sales_price"].astype("float64"),
    )
    out = df.groupby("i_item_id", as_index=False, dropna=False)[
        ["agg1", "agg2", "agg3", "agg4"]
    ].mean()

    out = out.sort_values("i_item_id", na_position="last").head(100)
    return out[["i_item_id", "agg1", "agg2", "agg3", "agg4"]].reset_index(drop=True)
