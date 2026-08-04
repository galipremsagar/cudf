# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 83. Per-item return quantities across store, catalog and web for three selected weeks."""

from __future__ import annotations

import pandas as pd

_DATES = ["2000-06-30", "2000-09-27", "2000-11-17"]


def _returned_quantity(returns, item, date_sks, item_key, date_key, value, name):
    frame = returns.merge(date_sks, left_on=date_key, right_on="d_date_sk")
    frame = frame.merge(item, left_on=item_key, right_on="i_item_sk")
    # min_count=1: an item whose returned quantities are all NULL sums to NULL
    frame = frame.groupby("i_item_id", as_index=False, dropna=False)[value].sum(
        min_count=1
    )
    return frame.rename(columns={"i_item_id": "item_id", value: name})


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_date", "d_week_seq"]
    )
    item = pd.read_parquet(f"{base}/item{suffix}", columns=["i_item_sk", "i_item_id"])
    store_returns = pd.read_parquet(
        f"{base}/store_returns{suffix}",
        columns=["sr_returned_date_sk", "sr_item_sk", "sr_return_quantity"],
    )
    catalog_returns = pd.read_parquet(
        f"{base}/catalog_returns{suffix}",
        columns=["cr_returned_date_sk", "cr_item_sk", "cr_return_quantity"],
    )
    web_returns = pd.read_parquet(
        f"{base}/web_returns{suffix}",
        columns=["wr_returned_date_sk", "wr_item_sk", "wr_return_quantity"],
    )

    date_dim["d_date"] = pd.to_datetime(date_dim["d_date"])
    anchors = pd.to_datetime(pd.Series(_DATES))
    weeks = date_dim[date_dim["d_date"].isin(anchors)][["d_week_seq"]].drop_duplicates()
    in_weeks = date_dim.merge(weeks, on="d_week_seq")
    wanted_dates = in_weeks[["d_date"]].drop_duplicates()
    date_sks = date_dim.merge(wanted_dates, on="d_date")[["d_date_sk"]]

    sr = _returned_quantity(
        store_returns,
        item,
        date_sks,
        "sr_item_sk",
        "sr_returned_date_sk",
        "sr_return_quantity",
        "sr_item_qty",
    )
    cr = _returned_quantity(
        catalog_returns,
        item,
        date_sks,
        "cr_item_sk",
        "cr_returned_date_sk",
        "cr_return_quantity",
        "cr_item_qty",
    )
    wr = _returned_quantity(
        web_returns,
        item,
        date_sks,
        "wr_item_sk",
        "wr_returned_date_sk",
        "wr_return_quantity",
        "wr_item_qty",
    )

    result = sr.merge(cr, on="item_id").merge(wr, on="item_id")
    total = (
        result["sr_item_qty"] + result["cr_item_qty"] + result["wr_item_qty"]
    ).astype("float64")
    result["sr_dev"] = result["sr_item_qty"] * 1.0 / total / 3.0 * 100
    result["cr_dev"] = result["cr_item_qty"] * 1.0 / total / 3.0 * 100
    result["wr_dev"] = result["wr_item_qty"] * 1.0 / total / 3.0 * 100
    result["average"] = total / 3.0

    result = result[
        [
            "item_id",
            "sr_item_qty",
            "sr_dev",
            "cr_item_qty",
            "cr_dev",
            "wr_item_qty",
            "wr_dev",
            "average",
        ]
    ]
    result = result.sort_values(["item_id", "sr_item_qty"], na_position="first")
    return result.head(100).reset_index(drop=True)
