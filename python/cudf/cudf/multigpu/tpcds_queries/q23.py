# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 23.Catalog and web sales of frequently sold items to the best store customers."""

from __future__ import annotations

import pandas as pd


def _frequent_ss_items(path, suffix):
    """One row per (item description, item, sold date) sold more than 4 times.

    Only ``item_sk`` is used downstream, but the duplicates are kept: the
    query joins the fact tables against this set, so an item that qualifies
    on several dates multiplies the sales it joins to.
    """
    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}", columns=["ss_sold_date_sk", "ss_item_sk"]
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_item_desc"]
    )

    date_dim = date_dim[date_dim["d_year"].isin([2000, 2001, 2002, 2003])][
        ["d_date_sk"]
    ]
    # d_date is unique per d_date_sk, so grouping by the key is the same
    # grouping as by the date itself.
    item = item.assign(itemdesc=item["i_item_desc"].str.slice(0, 30))[
        ["i_item_sk", "itemdesc"]
    ]

    df = store_sales.merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
    df = df.merge(item, left_on="ss_item_sk", right_on="i_item_sk")
    counts = df.groupby(
        ["itemdesc", "i_item_sk", "d_date_sk"], as_index=False, dropna=False
    ).agg(cnt=("ss_item_sk", "count"))
    return counts[counts["cnt"] > 4][["i_item_sk"]].rename(
        columns={"i_item_sk": "item_sk"}
    )


def _best_ss_customer(path, suffix):
    """Customers whose store sales exceed half of the largest customer total."""
    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_customer_sk",
            "ss_quantity",
            "ss_sales_price",
        ],
    )
    customer = pd.read_parquet(
        f"{path}/customer{suffix}", columns=["c_customer_sk"]
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )

    store_sales = store_sales.assign(
        csales=store_sales["ss_quantity"]
        * store_sales["ss_sales_price"].astype("float64")
    )
    joined = store_sales.merge(
        customer, left_on="ss_customer_sk", right_on="c_customer_sk"
    )

    years = date_dim[date_dim["d_year"].isin([2000, 2001, 2002, 2003])][["d_date_sk"]]
    in_years = joined.merge(years, left_on="ss_sold_date_sk", right_on="d_date_sk")
    tpcds_cmax = (
        in_years.groupby("c_customer_sk", as_index=False)["csales"].sum()["csales"].max()
    )

    totals = joined.groupby("c_customer_sk", as_index=False)["csales"].sum()
    return totals[totals["csales"] > (50 / 100.0) * tpcds_cmax][["c_customer_sk"]]


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    frequent = _frequent_ss_items(path, suffix)
    best = _best_ss_customer(path, suffix)

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    feb2000 = date_dim[(date_dim["d_year"] == 2000) & (date_dim["d_moy"] == 2)][
        ["d_date_sk"]
    ]
    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=["c_customer_sk", "c_first_name", "c_last_name"],
    )
    best_customers = customer.merge(best, on="c_customer_sk")

    frames = []
    for table, date_key, item_key, customer_key, quantity, price in (
        (
            "catalog_sales",
            "cs_sold_date_sk",
            "cs_item_sk",
            "cs_bill_customer_sk",
            "cs_quantity",
            "cs_list_price",
        ),
        (
            "web_sales",
            "ws_sold_date_sk",
            "ws_item_sk",
            "ws_bill_customer_sk",
            "ws_quantity",
            "ws_list_price",
        ),
    ):
        sales = pd.read_parquet(
            f"{path}/{table}{suffix}",
            columns=[date_key, item_key, customer_key, quantity, price],
        )
        # The list price is a DECIMAL and no GPU groupby can sum one, so the
        # amount is built in float64. SUM ignores a NULL term, hence the fill.
        sales = sales.assign(
            **{
                price: sales[price].astype("float64").fillna(0.0),
                quantity: sales[quantity].astype("float64").fillna(0.0),
            }
        )
        sales = sales.merge(feb2000, left_on=date_key, right_on="d_date_sk")
        sales = sales.merge(
            best_customers, left_on=customer_key, right_on="c_customer_sk"
        )
        sales = sales.merge(frequent, left_on=item_key, right_on="item_sk")
        sales = sales.assign(sales=sales[price] * sales[quantity])
        frames.append(
            sales.groupby(
                ["c_last_name", "c_first_name"], as_index=False, dropna=False
            )[["sales"]].sum()
        )

    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values(
        ["c_last_name", "c_first_name", "sales"], na_position="first"
    ).head(100)
    return out[["c_last_name", "c_first_name", "sales"]].reset_index(drop=True)
