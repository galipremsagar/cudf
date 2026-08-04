# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 92.Web discounts far above an item's own average discount."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    web_sales = pd.read_parquet(
        f"{path}/web_sales{suffix}",
        columns=["ws_item_sk", "ws_sold_date_sk", "ws_ext_discount_amt"],
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_manufact_id"]
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_date"]
    )

    d_date = pd.to_datetime(date_dim["d_date"])
    date_dim = date_dim[
        (d_date >= pd.Timestamp("2000-01-27")) & (d_date <= pd.Timestamp("2000-04-26"))
    ]

    sales = web_sales.merge(
        date_dim[["d_date_sk"]], left_on="ws_sold_date_sk", right_on="d_date_sk"
    )
    sales = sales.assign(amt=sales["ws_ext_discount_amt"].astype("float64"))

    avg_amt = sales.groupby("ws_item_sk", as_index=False)["amt"].mean()
    avg_amt.columns = ["ws_item_sk", "avg_amt"]

    items = item[item["i_manufact_id"] == 350][["i_item_sk"]]
    df = sales.merge(items, left_on="ws_item_sk", right_on="i_item_sk")
    df = df.merge(avg_amt, on="ws_item_sk")
    df = df[df["amt"] > 1.3 * df["avg_amt"]]

    total = df["ws_ext_discount_amt"].sum()
    return pd.DataFrame({"Excess Discount Amount": [total]})
