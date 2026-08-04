# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 37.Catalog items in a price band that were well stocked over a two month window."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    item = pd.read_parquet(
        f"{path}/item{suffix}",
        columns=[
            "i_item_sk",
            "i_item_id",
            "i_item_desc",
            "i_current_price",
            "i_manufact_id",
        ],
    )
    price = item["i_current_price"].astype("float64")
    item = item[
        (price >= 68)
        & (price <= 68 + 30)
        & (item["i_manufact_id"].isin([677, 940, 694, 808]))
    ][["i_item_sk", "i_item_id", "i_item_desc", "i_current_price"]]

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_date"]
    )
    d_date = pd.to_datetime(date_dim["d_date"])
    date_dim = date_dim[
        (d_date >= pd.Timestamp("2000-02-01")) & (d_date <= pd.Timestamp("2000-04-01"))
    ][["d_date_sk"]]

    inventory = pd.read_parquet(
        f"{path}/inventory{suffix}",
        columns=["inv_item_sk", "inv_date_sk", "inv_quantity_on_hand"],
    )
    inventory = inventory[
        (inventory["inv_quantity_on_hand"] >= 100)
        & (inventory["inv_quantity_on_hand"] <= 500)
    ]
    stocked = inventory.merge(date_dim, left_on="inv_date_sk", right_on="d_date_sk")[
        ["inv_item_sk"]
    ].drop_duplicates()

    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}", columns=["cs_item_sk"]
    )
    sold = catalog_sales[["cs_item_sk"]].drop_duplicates()

    joined = item.merge(stocked, left_on="i_item_sk", right_on="inv_item_sk").merge(
        sold, left_on="i_item_sk", right_on="cs_item_sk"
    )

    result = joined[["i_item_id", "i_item_desc", "i_current_price"]].drop_duplicates()
    result = result.sort_values("i_item_id").head(100)
    return result.reset_index(drop=True)
