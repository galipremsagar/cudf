# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 51. Items whose running web sales stay ahead of their running store sales."""

from __future__ import annotations

import pandas as pd

MONEY = ["web_sales", "store_sales", "web_cumulative", "store_cumulative"]

# ``sum()``/``max()`` over a NULL-free window is exact in whole cents, and the
# running sums here are wide enough that float64 would start to drift, so every
# money value is carried as an int64 count of cents. -1 stands for SQL NULL,
# which is unambiguous because sales prices are never negative.
NULL = -1


def _cumulative(sales, dates, item_col, date_col, price_col, name):
    """``sum(sum(price)) OVER (PARTITION BY item ORDER BY d_date)``, in cents."""
    joined = sales.merge(dates, left_on=date_col, right_on="d_date_sk")
    cents = (joined[price_col].astype("float64") * 100).round()
    joined = joined.assign(
        _cents=cents.fillna(0), _nonnull=cents.notna().astype("int64")
    )

    daily = joined.groupby([item_col, "d_date"], as_index=False)[
        ["_cents", "_nonnull"]
    ].sum()
    daily = daily.sort_values([item_col, "d_date"]).reset_index(drop=True)

    by_item = daily.groupby(item_col)
    running = by_item["_cents"].cumsum().astype("int64")
    seen = by_item["_nonnull"].cumsum()
    daily[name] = running.where(seen > 0, NULL)

    daily = daily.rename(columns={item_col: "item_sk"})
    return daily[["item_sk", "d_date", name]]


def _money(cents):
    """Render cents the way DuckDB renders the DECIMAL(38,2) it computes."""
    whole = (cents // 100).astype("string")
    fraction = (cents % 100).astype("string").str.zfill(2)
    return (whole + "." + fraction).where(cents >= 0)


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

    web = _cumulative(
        web_sales[web_sales["ws_item_sk"].notna()],
        dates,
        "ws_item_sk",
        "ws_sold_date_sk",
        "ws_sales_price",
        "web_sales",
    )
    # A NULL ss_item_sk can only ever produce a store-only row of the full outer
    # join, whose web_cumulative is NULL and which the final predicate therefore
    # drops, so leaving those groups out here changes nothing.
    store = _cumulative(
        store_sales,
        dates,
        "ss_item_sk",
        "ss_sold_date_sk",
        "ss_sales_price",
        "store_sales",
    )

    both = web.merge(store, on=["item_sk", "d_date"], how="outer")
    both["web_sales"] = both["web_sales"].fillna(NULL).astype("int64")
    both["store_sales"] = both["store_sales"].fillna(NULL).astype("int64")
    both = both.sort_values(["item_sk", "d_date"]).reset_index(drop=True)

    by_item = both.groupby("item_sk")
    both["web_cumulative"] = by_item["web_sales"].cummax()
    both["store_cumulative"] = by_item["store_sales"].cummax()

    selected = both[
        (both["web_cumulative"] >= 0)
        & (both["store_cumulative"] >= 0)
        & (both["web_cumulative"] > both["store_cumulative"])
    ]
    selected = (
        selected.sort_values(["item_sk", "d_date"], na_position="first")
        .head(100)
        .reset_index(drop=True)
    )

    result = selected[["item_sk", "d_date"]].copy()
    for column in MONEY:
        result[column] = _money(selected[column])
    return result
