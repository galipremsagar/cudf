# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 39.Warehouse/item pairs whose inventory is highly variable in both January and February 2001."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    date_dim = date_dim[date_dim["d_year"] == 2001][["d_date_sk", "d_moy"]]

    warehouse = pd.read_parquet(
        f"{path}/warehouse{suffix}", columns=["w_warehouse_sk", "w_warehouse_name"]
    )
    item = pd.read_parquet(f"{path}/item{suffix}", columns=["i_item_sk"])

    inventory = pd.read_parquet(
        f"{path}/inventory{suffix}",
        columns=[
            "inv_date_sk",
            "inv_item_sk",
            "inv_warehouse_sk",
            "inv_quantity_on_hand",
        ],
    )

    joined = inventory.merge(item, left_on="inv_item_sk", right_on="i_item_sk")
    joined = joined.merge(
        warehouse, left_on="inv_warehouse_sk", right_on="w_warehouse_sk"
    )
    joined = joined.merge(date_dim, left_on="inv_date_sk", right_on="d_date_sk")

    foo = (
        joined.groupby(
            ["w_warehouse_name", "w_warehouse_sk", "i_item_sk", "d_moy"],
            dropna=False,
        )
        .agg(
            stdev=("inv_quantity_on_hand", "std"),
            mean=("inv_quantity_on_hand", "mean"),
        )
        .reset_index()
    )
    foo["stdev"] = foo["stdev"] * 1.000

    ratio = (foo["stdev"] / foo["mean"]).where(foo["mean"] != 0, 0)
    inv = foo[ratio > 1].copy()
    inv["cov"] = (inv["stdev"] / inv["mean"]).where(inv["mean"] != 0)

    columns = ["w_warehouse_sk", "i_item_sk", "d_moy", "mean", "cov"]
    inv1 = inv[inv["d_moy"] == 1][columns]
    inv2 = inv[inv["d_moy"] == 1 + 1][columns]

    joined = inv1.merge(
        inv2, on=["w_warehouse_sk", "i_item_sk"], suffixes=("_1", "_2")
    )

    joined = joined.sort_values(
        [
            "w_warehouse_sk",
            "i_item_sk",
            "d_moy_1",
            "mean_1",
            "cov_1",
            "d_moy_2",
            "mean_2",
            "cov_2",
        ],
        na_position="first",
    ).reset_index(drop=True)

    result = joined[["w_warehouse_sk", "i_item_sk", "d_moy_1", "mean_1", "cov_1"]].copy()
    result["w_warehouse_sk_2"] = joined["w_warehouse_sk"]
    result["i_item_sk_2"] = joined["i_item_sk"]
    result["d_moy_2"] = joined["d_moy_2"]
    result["mean_2"] = joined["mean_2"]
    result["cov_2"] = joined["cov_2"]
    return result
