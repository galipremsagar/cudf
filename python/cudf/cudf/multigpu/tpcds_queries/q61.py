# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 61.What share of jewelry sales in a gmt-offset -5 market came
from promoted items during November 1998."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_store_sk",
            "ss_promo_sk",
            "ss_customer_sk",
            "ss_item_sk",
            "ss_ext_sales_price",
        ],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    date_dim = date_dim[(date_dim["d_year"] == 1998) & (date_dim["d_moy"] == 11)][
        ["d_date_sk"]
    ]

    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_gmt_offset"]
    )
    store["s_gmt_offset"] = store["s_gmt_offset"].astype("float64")
    store = store[store["s_gmt_offset"] == -5][["s_store_sk"]]

    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_category"]
    )
    item = item[item["i_category"] == "Jewelry"][["i_item_sk"]]

    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}",
        columns=["ca_address_sk", "ca_gmt_offset"],
    )
    customer_address["ca_gmt_offset"] = customer_address["ca_gmt_offset"].astype(
        "float64"
    )
    customer_address = customer_address[customer_address["ca_gmt_offset"] == -5][
        ["ca_address_sk"]
    ]

    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=["c_customer_sk", "c_current_addr_sk"],
    )
    customer = customer.merge(
        customer_address,
        left_on="c_current_addr_sk",
        right_on="ca_address_sk",
    )[["c_customer_sk"]]

    promotion = pd.read_parquet(
        f"{path}/promotion{suffix}",
        columns=[
            "p_promo_sk",
            "p_channel_dmail",
            "p_channel_email",
            "p_channel_tv",
        ],
    )
    # Each comparison is made null-free before the OR. cuDF's boolean OR does
    # not implement SQL three-valued logic: NULL | True yields NULL, not True,
    # so a row qualifying on one channel is dropped when another channel is
    # NULL. pandas never reaches that case because NaN == "Y" is already False.
    # cuDF differs from both, and this cost q61 3 promotion rows and 39,529 in
    # revenue against DuckDB.
    promotion = promotion[
        (promotion["p_channel_dmail"] == "Y").fillna(False)
        | (promotion["p_channel_email"] == "Y").fillna(False)
        | (promotion["p_channel_tv"] == "Y").fillna(False)
    ][["p_promo_sk"]]

    base = (
        store_sales.merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
        .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
        .merge(customer, left_on="ss_customer_sk", right_on="c_customer_sk")
        .merge(item, left_on="ss_item_sk", right_on="i_item_sk")
    )

    total = base["ss_ext_sales_price"].sum()
    promotions = base.merge(
        promotion, left_on="ss_promo_sk", right_on="p_promo_sk"
    )["ss_ext_sales_price"].sum()

    result = pd.DataFrame({"promotions": [promotions], "total": [total]})
    result["ratio"] = float(promotions) / float(total) * 100
    result = result.sort_values(["promotions", "total"]).head(100)
    return result.reset_index(drop=True)
