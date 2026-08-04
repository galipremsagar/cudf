# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 78.Compares each customer's never-returned store purchases in 2000 against what they bought on the web and catalog channels."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _cents(series):
    """A DECIMAL(*,2) column as an exact whole number of cents."""
    return (series.astype("float64") * 100).round()


def _round_half_away(series, digits):
    """SQL ``round``: a half goes away from zero, not to the even neighbour.

    ``Series.round`` is numpy's, which rounds a tie to even -- 0.125 becomes
    0.12 where SQL gives 0.13. The ratio here is a quotient of two integer
    sums, so exact halves are common and the difference is not academic.
    """
    scale = 10.0**digits
    scaled = series.astype("float64") * scale
    return np.floor(np.abs(scaled) + 0.5) / scale * np.sign(scaled)


def _decimal_str(cents):
    """Cents rendered the way DuckDB prints a DECIMAL(*,2)."""
    missing = cents.isna()
    filled = cents.fillna(0.0)
    negative = filled < 0
    magnitude = filled.abs()
    whole = (magnitude // 100).astype("int64").astype("str")
    frac = (magnitude % 100).astype("int64").astype("str").str.zfill(2)
    text = whole + "." + frac
    text = text.where(~negative, "-" + text)
    return text.where(~missing)


def _unreturned(path, suffix, date_dim, table, returns_table, prefix,
                order_key, return_order_key, item_sk, return_item_sk,
                customer_sk, date_sk, quantity, cost, price):
    sales = pd.read_parquet(
        f"{path}/{table}{suffix}",
        columns=[order_key, item_sk, customer_sk, date_sk, quantity, cost,
                 price],
    )
    returns = pd.read_parquet(
        f"{path}/{returns_table}{suffix}",
        columns=[return_order_key, return_item_sk],
    ).drop_duplicates()
    returns["returned"] = 1
    sales = sales.merge(
        returns,
        how="left",
        left_on=[order_key, item_sk],
        right_on=[return_order_key, return_item_sk],
    )
    sales = sales[sales["returned"].isna()]
    sales = sales.merge(date_dim, left_on=date_sk, right_on="d_date_sk")

    sales[f"{prefix}_qty"] = sales[quantity].astype("float64")
    sales[f"{prefix}_wc"] = _cents(sales[cost])
    sales[f"{prefix}_sp"] = _cents(sales[price])
    sales[f"{prefix}_qty_seen"] = sales[f"{prefix}_qty"].notna().astype("int64")
    sales[f"{prefix}_wc_seen"] = sales[f"{prefix}_wc"].notna().astype("int64")
    sales[f"{prefix}_sp_seen"] = sales[f"{prefix}_sp"].notna().astype("int64")

    measures = [
        f"{prefix}_qty", f"{prefix}_wc", f"{prefix}_sp",
        f"{prefix}_qty_seen", f"{prefix}_wc_seen", f"{prefix}_sp_seen",
    ]
    totals = (
        sales.groupby(["d_year", item_sk, customer_sk])[measures]
        .sum()
        .reset_index()
    )
    # SUM over a group whose values are all NULL is NULL, not zero.
    for name in (f"{prefix}_qty", f"{prefix}_wc", f"{prefix}_sp"):
        totals[name] = totals[name].where(totals[f"{name}_seen"] > 0)
    totals = totals.rename(
        columns={
            "d_year": f"{prefix}_sold_year",
            item_sk: f"{prefix}_item_sk",
            customer_sk: f"{prefix}_customer_sk",
        }
    )
    return totals[
        [f"{prefix}_sold_year", f"{prefix}_item_sk", f"{prefix}_customer_sk",
         f"{prefix}_qty", f"{prefix}_wc", f"{prefix}_sp"]
    ]


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    # Only 2000 survives: the joins line the three channels up on sold year.
    date_dim = date_dim[date_dim["d_year"] == 2000]

    ws = _unreturned(
        path, suffix, date_dim, "web_sales", "web_returns", "ws",
        "ws_order_number", "wr_order_number", "ws_item_sk", "wr_item_sk",
        "ws_bill_customer_sk", "ws_sold_date_sk", "ws_quantity",
        "ws_wholesale_cost", "ws_sales_price",
    )
    cs = _unreturned(
        path, suffix, date_dim, "catalog_sales", "catalog_returns", "cs",
        "cs_order_number", "cr_order_number", "cs_item_sk", "cr_item_sk",
        "cs_bill_customer_sk", "cs_sold_date_sk", "cs_quantity",
        "cs_wholesale_cost", "cs_sales_price",
    )
    ss = _unreturned(
        path, suffix, date_dim, "store_sales", "store_returns", "ss",
        "ss_ticket_number", "sr_ticket_number", "ss_item_sk", "sr_item_sk",
        "ss_customer_sk", "ss_sold_date_sk", "ss_quantity",
        "ss_wholesale_cost", "ss_sales_price",
    )

    joined = ss.merge(
        ws,
        how="left",
        left_on=["ss_sold_year", "ss_item_sk", "ss_customer_sk"],
        right_on=["ws_sold_year", "ws_item_sk", "ws_customer_sk"],
    )
    joined = joined.merge(
        cs,
        how="left",
        left_on=["ss_sold_year", "ss_item_sk", "ss_customer_sk"],
        right_on=["cs_sold_year", "cs_item_sk", "cs_customer_sk"],
    )

    joined = joined[
        (joined["ws_qty"].fillna(0.0) > 0) | (joined["cs_qty"].fillna(0.0) > 0)
    ]
    joined["other_chan_qty"] = joined["ws_qty"].fillna(0.0) + joined[
        "cs_qty"
    ].fillna(0.0)
    joined["other_chan_wc"] = joined["ws_wc"].fillna(0.0) + joined[
        "cs_wc"
    ].fillna(0.0)
    joined["other_chan_sp"] = joined["ws_sp"].fillna(0.0) + joined[
        "cs_sp"
    ].fillna(0.0)
    joined["ratio"] = _round_half_away(
        joined["ss_qty"] / joined["other_chan_qty"], 2
    )

    result = joined.sort_values(
        [
            "ss_sold_year",
            "ss_item_sk",
            "ss_customer_sk",
            "ss_qty",
            "ss_wc",
            "ss_sp",
            "other_chan_qty",
            "other_chan_wc",
            "other_chan_sp",
            "ratio",
        ],
        ascending=[True, True, True, False, False, False, True, True, True,
                   True],
        na_position="last",
    ).head(100)

    result["store_wholesale_cost"] = _decimal_str(result["ss_wc"])
    result["store_sales_price"] = _decimal_str(result["ss_sp"])
    result["other_chan_wholesale_cost"] = _decimal_str(
        result["other_chan_wc"]
    )
    result["other_chan_sales_price"] = _decimal_str(result["other_chan_sp"])
    return result[
        [
            "ss_sold_year",
            "ss_item_sk",
            "ss_customer_sk",
            "ratio",
            "ss_qty",
            "store_wholesale_cost",
            "store_sales_price",
            "other_chan_qty",
            "other_chan_wholesale_cost",
            "other_chan_sales_price",
        ]
    ].reset_index(drop=True)
