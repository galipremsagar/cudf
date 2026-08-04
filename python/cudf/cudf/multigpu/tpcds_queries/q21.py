# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 21.Inventory of cheap items before and after a date, per warehouse."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    inventory = pd.read_parquet(
        f"{path}/inventory{suffix}",
        columns=[
            "inv_date_sk",
            "inv_item_sk",
            "inv_warehouse_sk",
            "inv_quantity_on_hand",
        ],
    )
    warehouse = pd.read_parquet(
        f"{path}/warehouse{suffix}",
        columns=["w_warehouse_sk", "w_warehouse_name"],
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}",
        columns=["i_item_sk", "i_item_id", "i_current_price"],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_date"]
    )

    price = item["i_current_price"].astype("float64")
    # A NULL price is not BETWEEN anything, hence the fill.
    in_range = ((price >= 0.99) & (price <= 1.49)).fillna(False)
    item = item[in_range][["i_item_sk", "i_item_id"]]

    d_date = pd.to_datetime(date_dim["d_date"])
    date_dim = date_dim.assign(d_date=d_date)
    date_dim = date_dim[
        (d_date >= pd.Timestamp("2000-02-10"))
        & (d_date <= pd.Timestamp("2000-04-10"))
    ]

    df = inventory.merge(item, left_on="inv_item_sk", right_on="i_item_sk")
    df = df.merge(warehouse, left_on="inv_warehouse_sk", right_on="w_warehouse_sk")
    df = df.merge(date_dim, left_on="inv_date_sk", right_on="d_date_sk")

    # The flag is materialized as a column of its own first: the two masked
    # quantities are then built from columns of one and the same frame, which
    # is what the chunked layer can line up chunk for chunk.
    df = df.assign(is_before=df["d_date"] < pd.Timestamp("2000-03-11"))
    quantity = df["inv_quantity_on_hand"]
    is_before = df["is_before"]
    df = df.assign(
        inv_before=quantity.where(is_before, 0),
        inv_after=quantity.where(~is_before, 0),
    )

    grouped = df.groupby(
        ["w_warehouse_name", "i_item_id"], as_index=False, dropna=False
    )[["inv_before", "inv_after"]].sum()

    before = grouped["inv_before"].astype("float64")
    after = grouped["inv_after"].astype("float64")
    # Dividing by a zero ``inv_before`` gives an infinity that fails the upper
    # bound anyway, and the explicit ``> 0`` is the CASE that makes the SQL
    # ratio NULL there -- so no NULL has to be manufactured.
    ratio = (after * 1.000) / before
    grouped = grouped[
        (before > 0) & (ratio >= 2.000 / 3.000) & (ratio <= 3.000 / 2.000)
    ]

    result = grouped.sort_values(
        ["w_warehouse_name", "i_item_id"], na_position="first"
    ).head(100)
    return result[["w_warehouse_name", "i_item_id", "inv_before", "inv_after"]].reset_index(
        drop=True
    )
