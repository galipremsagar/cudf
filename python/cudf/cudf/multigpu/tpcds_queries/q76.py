# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 76.Counts and totals the sales whose channel-specific key column is NULL, by channel, year, quarter and item category."""

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


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_qoy"]
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_category"]
    )

    parts = []

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_store_sk",
            "ss_sold_date_sk",
            "ss_item_sk",
            "ss_ext_sales_price",
        ],
    )
    store_sales = store_sales[store_sales["ss_store_sk"].isna()]
    store = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    ).merge(item, left_on="ss_item_sk", right_on="i_item_sk")
    parts.append(
        store[["d_year", "d_qoy", "i_category"]].assign(
            channel="store",
            col_name="ss_store_sk",
            ext_sales_price=_cents(store["ss_ext_sales_price"]),
        )
    )
    del store_sales, store

    web_sales = pd.read_parquet(
        f"{path}/web_sales{suffix}",
        columns=[
            "ws_ship_customer_sk",
            "ws_sold_date_sk",
            "ws_item_sk",
            "ws_ext_sales_price",
        ],
    )
    web_sales = web_sales[web_sales["ws_ship_customer_sk"].isna()]
    web = web_sales.merge(
        date_dim, left_on="ws_sold_date_sk", right_on="d_date_sk"
    ).merge(item, left_on="ws_item_sk", right_on="i_item_sk")
    parts.append(
        web[["d_year", "d_qoy", "i_category"]].assign(
            channel="web",
            col_name="ws_ship_customer_sk",
            ext_sales_price=_cents(web["ws_ext_sales_price"]),
        )
    )
    del web_sales, web

    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=[
            "cs_ship_addr_sk",
            "cs_sold_date_sk",
            "cs_item_sk",
            "cs_ext_sales_price",
        ],
    )
    catalog_sales = catalog_sales[catalog_sales["cs_ship_addr_sk"].isna()]
    catalog = catalog_sales.merge(
        date_dim, left_on="cs_sold_date_sk", right_on="d_date_sk"
    ).merge(item, left_on="cs_item_sk", right_on="i_item_sk")
    parts.append(
        catalog[["d_year", "d_qoy", "i_category"]].assign(
            channel="catalog",
            col_name="cs_ship_addr_sk",
            ext_sales_price=_cents(catalog["cs_ext_sales_price"]),
        )
    )
    del catalog_sales, catalog

    foo = pd.concat(parts, ignore_index=True)
    foo["sales_cnt"] = 1
    foo["priced"] = foo["ext_sales_price"].notna().astype("int64")

    keys = ["channel", "col_name", "d_year", "d_qoy", "i_category"]
    result = (
        foo.groupby(keys, dropna=False)[
            ["ext_sales_price", "sales_cnt", "priced"]
        ]
        .sum()
        .reset_index()
    )
    # SUM over a group whose values are all NULL is NULL, not zero.
    result["ext_sales_price"] = result["ext_sales_price"].where(
        result["priced"] > 0
    )

    result = result.sort_values(keys, na_position="first").head(100)
    result["sales_amt"] = _decimal_str(result["ext_sales_price"])
    return result[keys + ["sales_cnt", "sales_amt"]].reset_index(drop=True)
