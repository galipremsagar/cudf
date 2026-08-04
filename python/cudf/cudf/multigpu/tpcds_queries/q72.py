# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 72.Counts 1999 catalog orders that were short of warehouse stock and shipped late, split by whether a promotion applied."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}",
        columns=["d_date_sk", "d_date", "d_week_seq", "d_year"],
    )
    date_dim["d_date"] = pd.to_datetime(date_dim["d_date"])

    d1 = date_dim[date_dim["d_year"] == 1999][
        ["d_date_sk", "d_date", "d_week_seq"]
    ].rename(columns={"d_date_sk": "d1_date_sk", "d_date": "d1_date"})
    d3 = date_dim[["d_date_sk", "d_date"]].rename(
        columns={"d_date_sk": "d3_date_sk", "d_date": "d3_date"}
    )

    customer_demographics = pd.read_parquet(
        f"{path}/customer_demographics{suffix}",
        columns=["cd_demo_sk", "cd_marital_status"],
    )
    customer_demographics = customer_demographics[
        customer_demographics["cd_marital_status"] == "D"
    ][["cd_demo_sk"]]

    household_demographics = pd.read_parquet(
        f"{path}/household_demographics{suffix}",
        columns=["hd_demo_sk", "hd_buy_potential"],
    )
    household_demographics = household_demographics[
        household_demographics["hd_buy_potential"] == ">10000"
    ][["hd_demo_sk"]]

    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=[
            "cs_item_sk",
            "cs_bill_cdemo_sk",
            "cs_bill_hdemo_sk",
            "cs_sold_date_sk",
            "cs_ship_date_sk",
            "cs_promo_sk",
            "cs_order_number",
            "cs_quantity",
        ],
    )
    sales = catalog_sales.merge(
        d1, left_on="cs_sold_date_sk", right_on="d1_date_sk"
    )
    sales = sales.merge(
        customer_demographics, left_on="cs_bill_cdemo_sk", right_on="cd_demo_sk"
    )
    sales = sales.merge(
        household_demographics,
        left_on="cs_bill_hdemo_sk",
        right_on="hd_demo_sk",
    )
    sales = sales.merge(d3, left_on="cs_ship_date_sk", right_on="d3_date_sk")
    sales = sales[
        sales["d3_date"] > sales["d1_date"] + pd.Timedelta(days=5)
    ]

    # d1.d_week_seq = d2.d_week_seq, so only the 1999 weeks of inventory count.
    weeks = d1[["d_week_seq"]].drop_duplicates()
    d2 = date_dim[["d_date_sk", "d_week_seq"]].merge(weeks, on="d_week_seq")
    inventory = pd.read_parquet(
        f"{path}/inventory{suffix}",
        columns=[
            "inv_date_sk",
            "inv_item_sk",
            "inv_warehouse_sk",
            "inv_quantity_on_hand",
        ],
    )
    inventory = inventory.merge(
        d2, left_on="inv_date_sk", right_on="d_date_sk"
    )[["inv_item_sk", "inv_warehouse_sk", "inv_quantity_on_hand",
       "d_week_seq"]]

    joined = sales.merge(
        inventory,
        left_on=["cs_item_sk", "d_week_seq"],
        right_on=["inv_item_sk", "d_week_seq"],
    )
    joined = joined[
        joined["inv_quantity_on_hand"] < joined["cs_quantity"]
    ]

    warehouse = pd.read_parquet(
        f"{path}/warehouse{suffix}",
        columns=["w_warehouse_sk", "w_warehouse_name"],
    )
    joined = joined.merge(
        warehouse, left_on="inv_warehouse_sk", right_on="w_warehouse_sk"
    )

    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_item_desc"]
    )
    joined = joined.merge(item, left_on="cs_item_sk", right_on="i_item_sk")

    promotion = pd.read_parquet(
        f"{path}/promotion{suffix}", columns=["p_promo_sk"]
    )
    joined = joined.merge(
        promotion, how="left", left_on="cs_promo_sk", right_on="p_promo_sk"
    )

    catalog_returns = pd.read_parquet(
        f"{path}/catalog_returns{suffix}",
        columns=["cr_item_sk", "cr_order_number"],
    )
    joined = joined.merge(
        catalog_returns,
        how="left",
        left_on=["cs_item_sk", "cs_order_number"],
        right_on=["cr_item_sk", "cr_order_number"],
    )

    joined["promo"] = joined["p_promo_sk"].notna().astype("int64")
    joined["no_promo"] = joined["p_promo_sk"].isna().astype("int64")
    joined["total_cnt"] = 1

    keys = ["i_item_desc", "w_warehouse_name", "d_week_seq"]
    result = (
        joined.groupby(keys, dropna=False)[
            ["no_promo", "promo", "total_cnt"]
        ]
        .sum()
        .reset_index()
    )
    result = result.sort_values(
        ["total_cnt"] + keys,
        ascending=[False, True, True, True],
        na_position="first",
    ).head(100)
    return result[keys + ["no_promo", "promo", "total_cnt"]].reset_index(
        drop=True
    )
