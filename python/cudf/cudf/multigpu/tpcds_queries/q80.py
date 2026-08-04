# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 80.Rolls up a month of promoted sales of expensive items, net of returns, by channel and by store, catalog page or web site."""

from __future__ import annotations

import pandas as pd


def _cents(series):
    """A DECIMAL(*,2) column as an exact whole number of cents."""
    return (series.astype("float64") * 100).round()


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


def _sum_by(frame, keys, measures):
    """GROUP BY ``keys`` summing ``measures``; an all-NULL group sums to NULL."""
    flags = [f"{name}__seen" for name in measures]
    for name, flag in zip(measures, flags):
        frame[flag] = frame[name].notna().astype("int64")
    total = (
        frame.groupby(keys, dropna=False)[measures + flags].sum().reset_index()
    )
    for name, flag in zip(measures, flags):
        total[name] = total[name].where(total[flag] > 0)
    return total[keys + measures]


MEASURES = ["sales", "returns_", "profit"]


def _channel_totals(path, suffix, date_dim, item, promotion, table,
                    returns_table, sales_columns, returns_columns,
                    order_key, return_order_key, item_sk, return_item_sk,
                    date_sk, promo_sk, price, net_profit, return_amount,
                    net_loss, dimension, dimension_sk, sales_dimension_sk,
                    dimension_id):
    sales = pd.read_parquet(f"{path}/{table}{suffix}", columns=sales_columns)
    returns = pd.read_parquet(
        f"{path}/{returns_table}{suffix}", columns=returns_columns
    )
    joined = sales.merge(
        returns,
        how="left",
        left_on=[item_sk, order_key],
        right_on=[return_item_sk, return_order_key],
    )
    joined = joined.merge(date_dim, left_on=date_sk, right_on="d_date_sk")
    joined = joined.merge(item, left_on=item_sk, right_on="i_item_sk")
    joined = joined.merge(promotion, left_on=promo_sk, right_on="p_promo_sk")

    dim = pd.read_parquet(
        f"{path}/{dimension}{suffix}", columns=[dimension_sk, dimension_id]
    )
    joined = joined.merge(
        dim, left_on=sales_dimension_sk, right_on=dimension_sk
    )

    joined["sales"] = _cents(joined[price])
    joined["returns_"] = _cents(joined[return_amount]).fillna(0.0)
    joined["profit"] = _cents(joined[net_profit]) - _cents(
        joined[net_loss]
    ).fillna(0.0)
    return _sum_by(joined, [dimension_id], MEASURES)


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_date"]
    )
    date_dim["d_date"] = pd.to_datetime(date_dim["d_date"])
    date_dim = date_dim[
        (date_dim["d_date"] >= pd.Timestamp("2000-08-23"))
        & (date_dim["d_date"] <= pd.Timestamp("2000-09-22"))
    ][["d_date_sk"]]

    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_current_price"]
    )
    item = item[item["i_current_price"].astype("float64") > 50][["i_item_sk"]]

    promotion = pd.read_parquet(
        f"{path}/promotion{suffix}", columns=["p_promo_sk", "p_channel_tv"]
    )
    promotion = promotion[promotion["p_channel_tv"] == "N"][["p_promo_sk"]]

    ssr = _channel_totals(
        path, suffix, date_dim, item, promotion,
        "store_sales", "store_returns",
        ["ss_item_sk", "ss_ticket_number", "ss_sold_date_sk", "ss_store_sk",
         "ss_promo_sk", "ss_ext_sales_price", "ss_net_profit"],
        ["sr_item_sk", "sr_ticket_number", "sr_return_amt", "sr_net_loss"],
        "ss_ticket_number", "sr_ticket_number", "ss_item_sk", "sr_item_sk",
        "ss_sold_date_sk", "ss_promo_sk", "ss_ext_sales_price",
        "ss_net_profit", "sr_return_amt", "sr_net_loss",
        "store", "s_store_sk", "ss_store_sk", "s_store_id",
    )
    csr = _channel_totals(
        path, suffix, date_dim, item, promotion,
        "catalog_sales", "catalog_returns",
        ["cs_item_sk", "cs_order_number", "cs_sold_date_sk",
         "cs_catalog_page_sk", "cs_promo_sk", "cs_ext_sales_price",
         "cs_net_profit"],
        ["cr_item_sk", "cr_order_number", "cr_return_amount", "cr_net_loss"],
        "cs_order_number", "cr_order_number", "cs_item_sk", "cr_item_sk",
        "cs_sold_date_sk", "cs_promo_sk", "cs_ext_sales_price",
        "cs_net_profit", "cr_return_amount", "cr_net_loss",
        "catalog_page", "cp_catalog_page_sk", "cs_catalog_page_sk",
        "cp_catalog_page_id",
    )
    wsr = _channel_totals(
        path, suffix, date_dim, item, promotion,
        "web_sales", "web_returns",
        ["ws_item_sk", "ws_order_number", "ws_sold_date_sk", "ws_web_site_sk",
         "ws_promo_sk", "ws_ext_sales_price", "ws_net_profit"],
        ["wr_item_sk", "wr_order_number", "wr_return_amt", "wr_net_loss"],
        "ws_order_number", "wr_order_number", "ws_item_sk", "wr_item_sk",
        "ws_sold_date_sk", "ws_promo_sk", "ws_ext_sales_price",
        "ws_net_profit", "wr_return_amt", "wr_net_loss",
        "web_site", "web_site_sk", "ws_web_site_sk", "web_site_id",
    )

    ssr["channel"] = "store channel"
    ssr["id"] = "store" + ssr["s_store_id"]
    csr["channel"] = "catalog channel"
    csr["id"] = "catalog_page" + csr["cp_catalog_page_id"]
    wsr["channel"] = "web channel"
    wsr["id"] = "web_site" + wsr["web_site_id"]

    columns = ["channel", "id"] + MEASURES
    x = pd.concat(
        [ssr[columns], csr[columns], wsr[columns]], ignore_index=True
    )

    by_id = _sum_by(x, ["channel", "id"], MEASURES)
    by_channel = _sum_by(x, ["channel"], MEASURES)
    by_channel["id"] = None
    x["_all"] = 0
    overall = _sum_by(x, ["_all"], MEASURES)
    overall["channel"] = None
    overall["id"] = None

    result = pd.concat(
        [by_id[columns], by_channel[columns], overall[columns]],
        ignore_index=True,
    )
    result = result.sort_values(["channel", "id"], na_position="first").head(
        100
    )
    for name in MEASURES:
        result[name] = _decimal_str(result[name])
    return result[columns].reset_index(drop=True)
