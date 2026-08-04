# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 58. Items whose store, catalog and web revenue agree within 10% in one week."""

from __future__ import annotations

import pandas as pd


def _sql_sum(frame, keys, value, name):
    """``sum(value)`` grouped by ``keys``, NULL when every input is NULL."""
    frame = frame.assign(_nonnull=frame[value].notna())
    grouped = frame.groupby(keys, as_index=False, dropna=False)[
        [value, "_nonnull"]
    ].sum()
    grouped[value] = grouped[value].where(grouped["_nonnull"] > 0)
    return grouped.drop(columns=["_nonnull"]).rename(columns={value: name})


def _revenue(sales, item_sk_col, date_sk_col, price_col, items, dates, name):
    joined = sales.merge(items, left_on=item_sk_col, right_on="i_item_sk").merge(
        dates, left_on=date_sk_col, right_on="d_date_sk"
    )
    grouped = _sql_sum(joined, ["i_item_id"], price_col, name)
    return grouped.rename(columns={"i_item_id": "item_id"})


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_date", "d_week_seq"]
    )
    date_dim = date_dim.assign(d_date=pd.to_datetime(date_dim["d_date"]))
    week = int(
        date_dim.loc[
            date_dim["d_date"] == pd.Timestamp("2000-01-03"), "d_week_seq"
        ].iloc[0]
    )
    # ``d_date`` is unique in date_dim, so "d_date in (the dates of that week)"
    # selects exactly the rows of that week.
    dates = date_dim[date_dim["d_week_seq"] == week][["d_date_sk"]]

    item = pd.read_parquet(
        f"{base}/item{suffix}", columns=["i_item_sk", "i_item_id"]
    )
    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=["ss_item_sk", "ss_sold_date_sk", "ss_ext_sales_price"],
    )
    catalog_sales = pd.read_parquet(
        f"{base}/catalog_sales{suffix}",
        columns=["cs_item_sk", "cs_sold_date_sk", "cs_ext_sales_price"],
    )
    web_sales = pd.read_parquet(
        f"{base}/web_sales{suffix}",
        columns=["ws_item_sk", "ws_sold_date_sk", "ws_ext_sales_price"],
    )

    ss_items = _revenue(
        store_sales,
        "ss_item_sk",
        "ss_sold_date_sk",
        "ss_ext_sales_price",
        item,
        dates,
        "ss_item_rev",
    )
    cs_items = _revenue(
        catalog_sales,
        "cs_item_sk",
        "cs_sold_date_sk",
        "cs_ext_sales_price",
        item,
        dates,
        "cs_item_rev",
    )
    ws_items = _revenue(
        web_sales,
        "ws_item_sk",
        "ws_sold_date_sk",
        "ws_ext_sales_price",
        item,
        dates,
        "ws_item_rev",
    )

    joined = ss_items.merge(cs_items, on="item_id").merge(ws_items, on="item_id")

    # The BETWEEN tests are exact decimal comparisons in SQL; done in whole
    # cents they stay exact (x between 0.9*y and 1.1*y  <=>  10x between 9y and 11y).
    ss = (joined["ss_item_rev"].astype("float64") * 100).round()
    cs = (joined["cs_item_rev"].astype("float64") * 100).round()
    ws = (joined["ws_item_rev"].astype("float64") * 100).round()
    keep = (
        (10 * ss >= 9 * cs)
        & (10 * ss <= 11 * cs)
        & (10 * ss >= 9 * ws)
        & (10 * ss <= 11 * ws)
        & (10 * cs >= 9 * ss)
        & (10 * cs <= 11 * ss)
        & (10 * cs >= 9 * ws)
        & (10 * cs <= 11 * ws)
        & (10 * ws >= 9 * ss)
        & (10 * ws <= 11 * ss)
        & (10 * ws >= 9 * cs)
        & (10 * ws <= 11 * cs)
    )
    selected = joined[keep].copy()

    ss_rev = selected["ss_item_rev"].astype("float64")
    cs_rev = selected["cs_item_rev"].astype("float64")
    ws_rev = selected["ws_item_rev"].astype("float64")
    average = (ss_rev + cs_rev + ws_rev) / 3
    selected["ss_dev"] = ss_rev / average * 100
    selected["cs_dev"] = cs_rev / average * 100
    selected["ws_dev"] = ws_rev / average * 100
    selected["average"] = average
    selected["_order"] = ss_rev

    result = selected.sort_values(
        ["item_id", "_order"], na_position="first"
    ).head(100)
    return result[
        [
            "item_id",
            "ss_item_rev",
            "ss_dev",
            "cs_item_rev",
            "cs_dev",
            "ws_item_rev",
            "ws_dev",
            "average",
        ]
    ].reset_index(drop=True)
