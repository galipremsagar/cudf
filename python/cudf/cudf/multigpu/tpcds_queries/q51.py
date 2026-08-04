# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 51. Items whose running web sales stay ahead of their running store sales."""

from __future__ import annotations

import pandas as pd

_RUNNING = ["web_cents", "web_seen", "store_cents", "store_seen"]

# ``sum()`` over a NULL-free window is exact in whole cents, and the running
# sums here are wide enough that carrying dollars in float64 would start to
# drift, so every money value is an int64 count of cents until the last step.
CENTS = 100


def _daily(sales, dates, item_col, date_col, price_col, prefix):
    """One row per (item, day) with that day's sales, in cents.

    ``_seen`` counts the non-NULL prices behind the sum, which is how SQL's
    ``sum()`` decides between 0 and NULL for a group.
    """
    joined = sales[sales[item_col].notna()].merge(
        dates, left_on=date_col, right_on="d_date_sk"
    )
    cents = (joined[price_col].astype("float64") * CENTS).round()
    joined = joined.assign(
        _cents=cents.fillna(0).astype("int64"),
        _seen=cents.notna().astype("int64"),
    )
    daily = joined.groupby([item_col, "d_date"], as_index=False)[
        ["_cents", "_seen"]
    ].sum()
    return daily.rename(
        columns={
            item_col: "item_sk",
            "_cents": f"{prefix}_cents",
            "_seen": f"{prefix}_seen",
        }
    )


def _running_by_item(frame, columns):
    """Per-item running totals of ``columns``, over the frame's current order.

    ``cumsum`` on the whole frame is a global scan; subtracting the total of
    every earlier item -- which is what that scan stood at when the item's
    first row was reached -- turns it into a per-item scan. That needs each
    item's rows to be contiguous and the items to come in the same order as
    their totals, which the caller's sort guarantees.
    """
    running = frame[columns].cumsum()
    totals = frame.groupby("item_sk", as_index=False)[columns].sum()
    totals = totals.sort_values("item_sk")
    earlier = totals[columns].cumsum()
    for column in columns:
        totals[f"_before_{column}"] = earlier[column] - totals[column]
        frame[f"_running_{column}"] = running[column]
    frame = frame.merge(
        totals[["item_sk"] + [f"_before_{c}" for c in columns]], on="item_sk"
    )
    for column in columns:
        frame[f"_running_{column}"] = (
            frame[f"_running_{column}"] - frame[f"_before_{column}"]
        )
    return frame


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}",
        columns=["d_date_sk", "d_date", "d_month_seq"],
    )
    dates = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1200 + 11)
    ][["d_date_sk", "d_date"]]

    web_sales = pd.read_parquet(
        f"{base}/web_sales{suffix}",
        columns=["ws_item_sk", "ws_sold_date_sk", "ws_sales_price"],
    )
    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=["ss_item_sk", "ss_sold_date_sk", "ss_sales_price"],
    )

    web = _daily(
        web_sales, dates, "ws_item_sk", "ws_sold_date_sk", "ws_sales_price", "web"
    )
    store = _daily(
        store_sales, dates, "ss_item_sk", "ss_sold_date_sk", "ss_sales_price", "store"
    )

    both = web.merge(store, on=["item_sk", "d_date"], how="outer")

    # Which side of the full outer join actually had a row for this day: a
    # group that exists always has a non-NULL sum here, so a NULL marks a
    # missing row rather than a NULL price.
    both = both.assign(
        _web_row=both["web_cents"].notna(), _store_row=both["store_cents"].notna()
    )
    for column in _RUNNING:
        both[column] = both[column].fillna(0).astype("int64")

    # The outer window is `max(cume_sales) over (... rows unbounded preceding)`
    # and cume_sales is itself a running sum of non-negative prices, so it is
    # non-decreasing and its running max is just the running sum carried across
    # the days only the other table contributed to. Running the sum over the
    # union of both tables' days therefore gives the same answer without a
    # segmented max, with `_seen` marking where it is still NULL.
    both = both.sort_values(["item_sk", "d_date"])
    both = _running_by_item(both, _RUNNING)

    selected = both[
        (both["_running_web_seen"] > 0)
        & (both["_running_store_seen"] > 0)
        & (both["_running_web_cents"] > both["_running_store_cents"])
    ]
    selected = (
        selected.sort_values(["item_sk", "d_date"], na_position="first")
        .head(100)
        .reset_index(drop=True)
    )

    web_total = selected["_running_web_cents"] / CENTS
    store_total = selected["_running_store_cents"] / CENTS
    result = selected[["item_sk", "d_date"]].copy()
    # web_sales is the joined-in cume_sales, so it is NULL on a day the item
    # had no web sale at all; web_cumulative is the carried-forward one.
    result["web_sales"] = web_total.where(selected["_web_row"])
    result["store_sales"] = store_total.where(selected["_store_row"])
    result["web_cumulative"] = web_total
    result["store_cumulative"] = store_total
    return result
