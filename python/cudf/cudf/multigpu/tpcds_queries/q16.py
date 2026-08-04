# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 16. Shipping cost and profit of never-returned Georgia catalog orders that shipped from several warehouses."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    catalog_sales = pd.read_parquet(
        f"{base}/catalog_sales{suffix}",
        columns=[
            "cs_ship_date_sk",
            "cs_ship_addr_sk",
            "cs_call_center_sk",
            "cs_warehouse_sk",
            "cs_order_number",
            "cs_ext_ship_cost",
            "cs_net_profit",
        ],
    )
    catalog_returns = pd.read_parquet(
        f"{base}/catalog_returns{suffix}", columns=["cr_order_number"]
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_date"]
    )
    customer_address = pd.read_parquet(
        f"{base}/customer_address{suffix}", columns=["ca_address_sk", "ca_state"]
    )
    call_center = pd.read_parquet(
        f"{base}/call_center{suffix}", columns=["cc_call_center_sk", "cc_county"]
    )

    d_date = pd.to_datetime(date_dim["d_date"])
    date_dim = date_dim[
        (d_date >= pd.Timestamp("2002-02-01"))
        & (d_date <= pd.Timestamp("2002-04-02"))
    ][["d_date_sk"]]
    customer_address = customer_address[customer_address["ca_state"] == "GA"][
        ["ca_address_sk"]
    ]
    call_center = call_center[call_center["cc_county"] == "Williamson County"][
        ["cc_call_center_sk"]
    ]

    # EXISTS: the order has a second line shipped from a different warehouse.
    # A NULL warehouse can never satisfy <>, on either side.
    pairs = catalog_sales[["cs_order_number", "cs_warehouse_sk"]].dropna()
    warehouses = (
        pairs.drop_duplicates()
        .groupby("cs_order_number")
        .size()
        .reset_index(name="_n_warehouses")
    )
    multi_warehouse = warehouses[warehouses["_n_warehouses"] > 1][
        ["cs_order_number"]
    ]

    returned = catalog_returns.drop_duplicates()

    joined = (
        catalog_sales[catalog_sales["cs_warehouse_sk"].notna()]
        .merge(date_dim, left_on="cs_ship_date_sk", right_on="d_date_sk")
        .merge(customer_address, left_on="cs_ship_addr_sk", right_on="ca_address_sk")
        .merge(
            call_center, left_on="cs_call_center_sk", right_on="cc_call_center_sk"
        )
        .merge(multi_warehouse, on="cs_order_number")
    )

    # NOT EXISTS: no catalog return quotes this order number.
    joined = joined.merge(
        returned, left_on="cs_order_number", right_on="cr_order_number", how="left"
    )
    joined = joined[joined["cr_order_number"].isna()]

    # Both money columns are DECIMAL, which libcudf cannot group-sum.
    joined = joined.assign(
        _all=0,
        cs_ext_ship_cost=joined["cs_ext_ship_cost"].astype("float64"),
        cs_net_profit=joined["cs_net_profit"].astype("float64"),
    )
    result = joined.groupby("_all").agg(
        order_count=("cs_order_number", "nunique"),
        total_shipping_cost=("cs_ext_ship_cost", "sum"),
        total_net_profit=("cs_net_profit", "sum"),
    )
    return result.reset_index(drop=True)
