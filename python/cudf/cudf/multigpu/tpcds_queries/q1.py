# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 1. Customers whose 2000 store returns far exceed their store's average."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_returns = pd.read_parquet(
        f"{path}/store_returns{suffix}",
        columns=[
            "sr_returned_date_sk",
            "sr_customer_sk",
            "sr_store_sk",
            "sr_return_amt",
        ],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_state"]
    )
    customer = pd.read_parquet(
        f"{path}/customer{suffix}", columns=["c_customer_sk", "c_customer_id"]
    )

    store_returns = store_returns.assign(
        sr_return_amt=store_returns["sr_return_amt"].astype("float64")
    )
    dates = date_dim[date_dim["d_year"] == 2000][["d_date_sk"]]

    joined = store_returns.merge(
        dates, left_on="sr_returned_date_sk", right_on="d_date_sk", how="inner"
    )
    ctr = (
        joined.groupby(["sr_customer_sk", "sr_store_sk"], dropna=False)[
            "sr_return_amt"
        ]
        .sum()
        .reset_index()
    )
    ctr = ctr.rename(
        columns={
            "sr_customer_sk": "ctr_customer_sk",
            "sr_store_sk": "ctr_store_sk",
            "sr_return_amt": "ctr_total_return",
        }
    )

    store_avg = (
        ctr.groupby("ctr_store_sk", dropna=False)["ctr_total_return"]
        .mean()
        .reset_index()
        .rename(columns={"ctr_total_return": "avg_total_return"})
    )
    store_avg = store_avg.assign(
        threshold=store_avg["avg_total_return"] * 1.2
    )[["ctr_store_sk", "threshold"]]

    ctr = ctr.merge(store_avg, on="ctr_store_sk", how="inner")
    ctr = ctr[ctr["ctr_total_return"] > ctr["threshold"]]

    stores_tn = store[store["s_state"] == "TN"][["s_store_sk"]]
    ctr = ctr.merge(
        stores_tn, left_on="ctr_store_sk", right_on="s_store_sk", how="inner"
    )
    result = ctr.merge(
        customer, left_on="ctr_customer_sk", right_on="c_customer_sk", how="inner"
    )

    result = result[["c_customer_id"]].sort_values(
        "c_customer_id", na_position="first", kind="stable"
    )
    return result.head(100).reset_index(drop=True)
