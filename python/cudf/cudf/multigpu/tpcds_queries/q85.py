# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 85. Average quantity, refund and fee by web return reason for selected demographics and states."""

from __future__ import annotations

import pandas as pd

_MARITAL = ["M", "S", "W"]
_EDUCATION = ["Advanced Degree", "College", "2 yr Degree"]
_STATES = ["IN", "OH", "NJ", "WI", "CT", "KY", "LA", "IA", "AR"]


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    web_sales = pd.read_parquet(
        f"{base}/web_sales{suffix}",
        columns=[
            "ws_web_page_sk",
            "ws_item_sk",
            "ws_order_number",
            "ws_sold_date_sk",
            "ws_quantity",
            "ws_sales_price",
            "ws_net_profit",
        ],
    )
    web_returns = pd.read_parquet(
        f"{base}/web_returns{suffix}",
        columns=[
            "wr_item_sk",
            "wr_order_number",
            "wr_refunded_cdemo_sk",
            "wr_returning_cdemo_sk",
            "wr_refunded_addr_sk",
            "wr_reason_sk",
            "wr_refunded_cash",
            "wr_fee",
        ],
    )
    web_page = pd.read_parquet(
        f"{base}/web_page{suffix}", columns=["wp_web_page_sk"]
    )
    customer_demographics = pd.read_parquet(
        f"{base}/customer_demographics{suffix}",
        columns=["cd_demo_sk", "cd_marital_status", "cd_education_status"],
    )
    customer_address = pd.read_parquet(
        f"{base}/customer_address{suffix}",
        columns=["ca_address_sk", "ca_country", "ca_state"],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    reason = pd.read_parquet(
        f"{base}/reason{suffix}", columns=["r_reason_sk", "r_reason_desc"]
    )

    web_sales["ws_sales_price"] = web_sales["ws_sales_price"].astype("float64")
    web_sales["ws_net_profit"] = web_sales["ws_net_profit"].astype("float64")
    web_returns["wr_refunded_cash"] = web_returns["wr_refunded_cash"].astype("float64")
    web_returns["wr_fee"] = web_returns["wr_fee"].astype("float64")

    dates = date_dim[date_dim["d_year"] == 2000][["d_date_sk"]]
    web_sales = web_sales.merge(dates, left_on="ws_sold_date_sk", right_on="d_date_sk")

    frame = web_sales.merge(
        web_returns,
        left_on=["ws_item_sk", "ws_order_number"],
        right_on=["wr_item_sk", "wr_order_number"],
    )
    frame = frame.merge(
        web_page, left_on="ws_web_page_sk", right_on="wp_web_page_sk"
    )

    # Both demographics rows must carry the same marital/education pair, and
    # every branch of the predicate names one of these three pairs.
    demo = customer_demographics[
        customer_demographics["cd_marital_status"].isin(_MARITAL)
        & customer_demographics["cd_education_status"].isin(_EDUCATION)
    ]
    cd1 = demo.rename(
        columns={
            "cd_demo_sk": "cd1_demo_sk",
            "cd_marital_status": "cd1_marital_status",
            "cd_education_status": "cd1_education_status",
        }
    )
    cd2 = demo.rename(
        columns={
            "cd_demo_sk": "cd2_demo_sk",
            "cd_marital_status": "cd2_marital_status",
            "cd_education_status": "cd2_education_status",
        }
    )
    frame = frame.merge(
        cd1, left_on="wr_refunded_cdemo_sk", right_on="cd1_demo_sk"
    )
    frame = frame.merge(
        cd2, left_on="wr_returning_cdemo_sk", right_on="cd2_demo_sk"
    )

    address = customer_address[
        (customer_address["ca_country"] == "United States")
        & customer_address["ca_state"].isin(_STATES)
    ]
    frame = frame.merge(
        address, left_on="wr_refunded_addr_sk", right_on="ca_address_sk"
    )
    frame = frame.merge(reason, left_on="wr_reason_sk", right_on="r_reason_sk")

    same_demo = (
        frame["cd1_marital_status"] == frame["cd2_marital_status"]
    ) & (frame["cd1_education_status"] == frame["cd2_education_status"])
    price = frame["ws_sales_price"]
    demo_ok = same_demo & (
        (
            (frame["cd1_marital_status"] == "M")
            & (frame["cd1_education_status"] == "Advanced Degree")
            & (price >= 100.00)
            & (price <= 150.00)
        )
        | (
            (frame["cd1_marital_status"] == "S")
            & (frame["cd1_education_status"] == "College")
            & (price >= 50.00)
            & (price <= 100.00)
        )
        | (
            (frame["cd1_marital_status"] == "W")
            & (frame["cd1_education_status"] == "2 yr Degree")
            & (price >= 150.00)
            & (price <= 200.00)
        )
    )

    profit = frame["ws_net_profit"]
    state = frame["ca_state"]
    address_ok = (
        (state.isin(["IN", "OH", "NJ"]) & (profit >= 100) & (profit <= 200))
        | (state.isin(["WI", "CT", "KY"]) & (profit >= 150) & (profit <= 300))
        | (state.isin(["LA", "IA", "AR"]) & (profit >= 50) & (profit <= 250))
    )

    frame = frame[demo_ok & address_ok]

    grouped = frame.groupby("r_reason_desc", as_index=False, dropna=False).agg(
        avg1=("ws_quantity", "mean"),
        avg2=("wr_refunded_cash", "mean"),
        avg3=("wr_fee", "mean"),
    )
    grouped["reason"] = grouped["r_reason_desc"].str.slice(0, 20)

    result = grouped[["reason", "avg1", "avg2", "avg3"]]
    result = result.sort_values(
        ["reason", "avg1", "avg2", "avg3"], na_position="last"
    )
    return result.head(100).reset_index(drop=True)
