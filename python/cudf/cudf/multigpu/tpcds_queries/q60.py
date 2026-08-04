# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 60. Music sales across all three channels, September 1998, GMT-5."""

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


def _channel(sales, item_sk_col, addr_sk_col, date_sk_col, price_col, items, dates, addrs):
    joined = (
        sales.merge(dates, left_on=date_sk_col, right_on="d_date_sk")
        .merge(addrs, left_on=addr_sk_col, right_on="ca_address_sk")
        .merge(items, left_on=item_sk_col, right_on="i_item_sk")
    )
    return _sql_sum(joined, ["i_item_id"], price_col, "total_sales")


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    customer_address = pd.read_parquet(
        f"{base}/customer_address{suffix}",
        columns=["ca_address_sk", "ca_gmt_offset"],
    )
    item = pd.read_parquet(
        f"{base}/item{suffix}", columns=["i_item_sk", "i_item_id", "i_category"]
    )

    dates = date_dim[(date_dim["d_year"] == 1998) & (date_dim["d_moy"] == 9)][
        ["d_date_sk"]
    ]
    addrs = customer_address[
        customer_address["ca_gmt_offset"].astype("float64") == -5
    ][["ca_address_sk"]]

    wanted_ids = item[item["i_category"] == "Music"][["i_item_id"]].drop_duplicates()
    items = item[["i_item_sk", "i_item_id"]].merge(wanted_ids, on="i_item_id")

    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=["ss_item_sk", "ss_sold_date_sk", "ss_addr_sk", "ss_ext_sales_price"],
    )
    catalog_sales = pd.read_parquet(
        f"{base}/catalog_sales{suffix}",
        columns=[
            "cs_item_sk",
            "cs_sold_date_sk",
            "cs_bill_addr_sk",
            "cs_ext_sales_price",
        ],
    )
    web_sales = pd.read_parquet(
        f"{base}/web_sales{suffix}",
        columns=[
            "ws_item_sk",
            "ws_sold_date_sk",
            "ws_bill_addr_sk",
            "ws_ext_sales_price",
        ],
    )

    parts = [
        _channel(
            store_sales,
            "ss_item_sk",
            "ss_addr_sk",
            "ss_sold_date_sk",
            "ss_ext_sales_price",
            items,
            dates,
            addrs,
        ),
        _channel(
            catalog_sales,
            "cs_item_sk",
            "cs_bill_addr_sk",
            "cs_sold_date_sk",
            "cs_ext_sales_price",
            items,
            dates,
            addrs,
        ),
        _channel(
            web_sales,
            "ws_item_sk",
            "ws_bill_addr_sk",
            "ws_sold_date_sk",
            "ws_ext_sales_price",
            items,
            dates,
            addrs,
        ),
    ]

    combined = pd.concat(parts, ignore_index=True)
    totals = _sql_sum(combined, ["i_item_id"], "total_sales", "total_sales")
    totals["_order"] = totals["total_sales"].astype("float64")

    result = totals.sort_values(["i_item_id", "_order"]).head(100)
    return result[["i_item_id", "total_sales"]].reset_index(drop=True)
