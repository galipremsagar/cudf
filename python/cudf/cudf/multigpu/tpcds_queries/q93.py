# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 93.Sales net of returns, per customer, for one return reason."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_item_sk",
            "ss_ticket_number",
            "ss_customer_sk",
            "ss_quantity",
            "ss_sales_price",
        ],
    )
    store_returns = pd.read_parquet(
        f"{path}/store_returns{suffix}",
        columns=[
            "sr_item_sk",
            "sr_ticket_number",
            "sr_reason_sk",
            "sr_return_quantity",
        ],
    )
    reason = pd.read_parquet(
        f"{path}/reason{suffix}", columns=["r_reason_sk", "r_reason_desc"]
    )

    reason = reason[reason["r_reason_desc"] == "reason 28"][["r_reason_sk"]]
    returns = store_returns.merge(
        reason, left_on="sr_reason_sk", right_on="r_reason_sk"
    )

    # The LEFT OUTER JOIN is made inner by the WHERE clause on sr_reason_sk.
    df = store_sales.merge(
        returns[["sr_item_sk", "sr_ticket_number", "sr_return_quantity"]],
        left_on=["ss_item_sk", "ss_ticket_number"],
        right_on=["sr_item_sk", "sr_ticket_number"],
    )

    # A missing sr_return_quantity means "subtract nothing", which is exactly
    # what the CASE expression says.
    act_sales = (df["ss_quantity"] - df["sr_return_quantity"].fillna(0)) * df[
        "ss_sales_price"
    ].astype("float64")
    df = df.assign(act_sales=act_sales)

    grouped = df.groupby("ss_customer_sk", dropna=False, as_index=False)[
        "act_sales"
    ].sum(min_count=1)
    grouped = grouped.sort_values(
        ["act_sales", "ss_customer_sk"], na_position="first"
    ).head(100)
    result = grouped[["ss_customer_sk", "act_sales"]]
    result.columns = ["ss_customer_sk", "sumsales"]
    return result.reset_index(drop=True)
