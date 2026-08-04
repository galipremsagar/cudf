# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 77.Rolls up a month of sales, returns and profit by channel and by store, call centre or web page."""

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

    store = pd.read_parquet(f"{path}/store{suffix}", columns=["s_store_sk"])
    web_page = pd.read_parquet(
        f"{path}/web_page{suffix}", columns=["wp_web_page_sk"]
    )

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=["ss_sold_date_sk", "ss_store_sk", "ss_ext_sales_price",
                 "ss_net_profit"],
    )
    store_sales = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    ).merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    store_sales["sales"] = _cents(store_sales["ss_ext_sales_price"])
    store_sales["profit"] = _cents(store_sales["ss_net_profit"])
    ss = _sum_by(store_sales, ["s_store_sk"], ["sales", "profit"])

    store_returns = pd.read_parquet(
        f"{path}/store_returns{suffix}",
        columns=["sr_returned_date_sk", "sr_store_sk", "sr_return_amt",
                 "sr_net_loss"],
    )
    store_returns = store_returns.merge(
        date_dim, left_on="sr_returned_date_sk", right_on="d_date_sk"
    ).merge(store, left_on="sr_store_sk", right_on="s_store_sk")
    store_returns["returns_"] = _cents(store_returns["sr_return_amt"])
    store_returns["profit_loss"] = _cents(store_returns["sr_net_loss"])
    sr = _sum_by(store_returns, ["s_store_sk"], ["returns_", "profit_loss"])

    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=["cs_sold_date_sk", "cs_call_center_sk", "cs_ext_sales_price",
                 "cs_net_profit"],
    )
    catalog_sales = catalog_sales.merge(
        date_dim, left_on="cs_sold_date_sk", right_on="d_date_sk"
    )
    catalog_sales["sales"] = _cents(catalog_sales["cs_ext_sales_price"])
    catalog_sales["profit"] = _cents(catalog_sales["cs_net_profit"])
    cs = _sum_by(catalog_sales, ["cs_call_center_sk"], ["sales", "profit"])

    catalog_returns = pd.read_parquet(
        f"{path}/catalog_returns{suffix}",
        columns=["cr_returned_date_sk", "cr_call_center_sk",
                 "cr_return_amount", "cr_net_loss"],
    )
    catalog_returns = catalog_returns.merge(
        date_dim, left_on="cr_returned_date_sk", right_on="d_date_sk"
    )
    catalog_returns["returns_"] = _cents(catalog_returns["cr_return_amount"])
    catalog_returns["profit_loss"] = _cents(catalog_returns["cr_net_loss"])
    cr = _sum_by(
        catalog_returns, ["cr_call_center_sk"], ["returns_", "profit_loss"]
    )

    web_sales = pd.read_parquet(
        f"{path}/web_sales{suffix}",
        columns=["ws_sold_date_sk", "ws_web_page_sk", "ws_ext_sales_price",
                 "ws_net_profit"],
    )
    web_sales = web_sales.merge(
        date_dim, left_on="ws_sold_date_sk", right_on="d_date_sk"
    ).merge(web_page, left_on="ws_web_page_sk", right_on="wp_web_page_sk")
    web_sales["sales"] = _cents(web_sales["ws_ext_sales_price"])
    web_sales["profit"] = _cents(web_sales["ws_net_profit"])
    ws = _sum_by(web_sales, ["wp_web_page_sk"], ["sales", "profit"])

    web_returns = pd.read_parquet(
        f"{path}/web_returns{suffix}",
        columns=["wr_returned_date_sk", "wr_web_page_sk", "wr_return_amt",
                 "wr_net_loss"],
    )
    web_returns = web_returns.merge(
        date_dim, left_on="wr_returned_date_sk", right_on="d_date_sk"
    ).merge(web_page, left_on="wr_web_page_sk", right_on="wp_web_page_sk")
    web_returns["returns_"] = _cents(web_returns["wr_return_amt"])
    web_returns["profit_loss"] = _cents(web_returns["wr_net_loss"])
    wr = _sum_by(web_returns, ["wp_web_page_sk"], ["returns_", "profit_loss"])

    store_channel = ss.merge(sr, how="left", on="s_store_sk")
    store_channel["channel"] = "store channel"
    store_channel["id"] = store_channel["s_store_sk"].astype("float64")
    store_channel["returns_"] = store_channel["returns_"].fillna(0.0)
    store_channel["profit"] = store_channel["profit"] - store_channel[
        "profit_loss"
    ].fillna(0.0)

    # ``FROM cs, cr``: an unrestricted cross join, not a join on call centre.
    catalog_channel = cs.assign(_cross=1).merge(cr.assign(_cross=1), on="_cross")
    catalog_channel["channel"] = "catalog channel"
    catalog_channel["id"] = catalog_channel["cs_call_center_sk"].astype(
        "float64"
    )
    catalog_channel["profit"] = (
        catalog_channel["profit"] - catalog_channel["profit_loss"]
    )

    web_channel = ws.merge(wr, how="left", on="wp_web_page_sk")
    web_channel["channel"] = "web channel"
    web_channel["id"] = web_channel["wp_web_page_sk"].astype("float64")
    web_channel["returns_"] = web_channel["returns_"].fillna(0.0)
    web_channel["profit"] = web_channel["profit"] - web_channel[
        "profit_loss"
    ].fillna(0.0)

    columns = ["channel", "id"] + MEASURES
    x = pd.concat(
        [
            store_channel[columns],
            catalog_channel[columns],
            web_channel[columns],
        ],
        ignore_index=True,
    )

    by_id = _sum_by(x, ["channel", "id"], MEASURES)
    by_channel = _sum_by(x, ["channel"], MEASURES)
    by_channel["id"] = float("nan")
    x["_all"] = 0
    overall = _sum_by(x, ["_all"], MEASURES)
    overall["channel"] = None
    overall["id"] = float("nan")

    result = pd.concat(
        [by_id[columns], by_channel[columns], overall[columns]],
        ignore_index=True,
    )
    result = result.sort_values(
        ["channel", "id", "returns_"],
        ascending=[True, True, False],
        na_position="first",
    ).head(100)
    for name in MEASURES:
        result[name] = _decimal_str(result[name])
    return result[columns].reset_index(drop=True)
