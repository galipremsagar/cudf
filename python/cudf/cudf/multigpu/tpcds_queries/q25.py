# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 25.Store profit, store return loss and later catalog profit per item and store."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_item_sk",
            "ss_customer_sk",
            "ss_store_sk",
            "ss_ticket_number",
            "ss_net_profit",
        ],
    )
    store_returns = pd.read_parquet(
        f"{path}/store_returns{suffix}",
        columns=[
            "sr_returned_date_sk",
            "sr_item_sk",
            "sr_customer_sk",
            "sr_ticket_number",
            "sr_net_loss",
        ],
    )
    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=[
            "cs_sold_date_sk",
            "cs_bill_customer_sk",
            "cs_item_sk",
            "cs_net_profit",
        ],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_moy", "d_year"]
    )
    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_store_id", "s_store_name"]
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_item_id", "i_item_desc"]
    )

    # The three money columns are DECIMALs and no GPU groupby sums a decimal.
    store_sales = store_sales.assign(
        ss_net_profit=store_sales["ss_net_profit"].astype("float64")
    )
    store_returns = store_returns.assign(
        sr_net_loss=store_returns["sr_net_loss"].astype("float64")
    )
    catalog_sales = catalog_sales.assign(
        cs_net_profit=catalog_sales["cs_net_profit"].astype("float64")
    )

    d1 = date_dim[(date_dim["d_moy"] == 4) & (date_dim["d_year"] == 2001)][
        ["d_date_sk"]
    ]
    d2 = date_dim[
        (date_dim["d_moy"] >= 4)
        & (date_dim["d_moy"] <= 10)
        & (date_dim["d_year"] == 2001)
    ][["d_date_sk"]]

    sold = store_sales.merge(
        d1, left_on="ss_sold_date_sk", right_on="d_date_sk"
    ).drop(columns=["ss_sold_date_sk", "d_date_sk"])
    returned = store_returns.merge(
        d2, left_on="sr_returned_date_sk", right_on="d_date_sk"
    ).drop(columns=["sr_returned_date_sk", "d_date_sk"])
    catalog = catalog_sales.merge(
        d2, left_on="cs_sold_date_sk", right_on="d_date_sk"
    ).drop(columns=["cs_sold_date_sk", "d_date_sk"])

    df = sold.merge(
        returned,
        left_on=["ss_customer_sk", "ss_item_sk", "ss_ticket_number"],
        right_on=["sr_customer_sk", "sr_item_sk", "sr_ticket_number"],
    )
    df = df.merge(
        catalog,
        left_on=["sr_customer_sk", "sr_item_sk"],
        right_on=["cs_bill_customer_sk", "cs_item_sk"],
    )
    df = df.merge(item, left_on="ss_item_sk", right_on="i_item_sk")
    df = df.merge(store, left_on="ss_store_sk", right_on="s_store_sk")

    df = df.rename(
        columns={
            "ss_net_profit": "store_sales_profit",
            "sr_net_loss": "store_returns_loss",
            "cs_net_profit": "catalog_sales_profit",
        }
    )
    keys = ["i_item_id", "i_item_desc", "s_store_id", "s_store_name"]
    out = df.groupby(keys, as_index=False, dropna=False)[
        ["store_sales_profit", "store_returns_loss", "catalog_sales_profit"]
    ].sum()

    out = out.sort_values(keys, na_position="last").head(100)
    return out.reset_index(drop=True)
