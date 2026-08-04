# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 82. Items in a price band, from given manufacturers, well stocked in mid-2000 and sold in stores."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    item = pd.read_parquet(
        f"{base}/item{suffix}",
        columns=[
            "i_item_sk",
            "i_item_id",
            "i_item_desc",
            "i_current_price",
            "i_manufact_id",
        ],
    )
    inventory = pd.read_parquet(
        f"{base}/inventory{suffix}",
        columns=["inv_date_sk", "inv_item_sk", "inv_quantity_on_hand"],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_date"]
    )
    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}", columns=["ss_item_sk"]
    )

    price = item["i_current_price"].astype("float64")
    item = item[
        (price >= 62)
        & (price <= 62 + 30)
        & item["i_manufact_id"].isin([129, 270, 821, 423])
    ]

    date_dim["d_date"] = pd.to_datetime(date_dim["d_date"])
    dates = date_dim[
        (date_dim["d_date"] >= pd.Timestamp("2000-05-25"))
        & (date_dim["d_date"] <= pd.Timestamp("2000-07-24"))
    ][["d_date_sk"]]

    inventory = inventory[
        (inventory["inv_quantity_on_hand"] >= 100)
        & (inventory["inv_quantity_on_hand"] <= 500)
    ]
    stocked = inventory.merge(dates, left_on="inv_date_sk", right_on="d_date_sk")
    stocked = stocked[["inv_item_sk"]].drop_duplicates()
    sold = store_sales[["ss_item_sk"]].drop_duplicates()

    result = item.merge(stocked, left_on="i_item_sk", right_on="inv_item_sk")
    result = result.merge(sold, left_on="i_item_sk", right_on="ss_item_sk")

    result = result[["i_item_id", "i_item_desc", "i_current_price"]].drop_duplicates()
    result = result.sort_values("i_item_id", na_position="last")
    return result.head(100).reset_index(drop=True)
