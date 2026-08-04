# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 14. November 2001 sales, rolled up by channel and item family, for items sold through all three channels."""

from __future__ import annotations

import pandas as pd

_TRIPLE = ["i_brand_id", "i_class_id", "i_category_id"]
_KEYS = ["channel", "i_brand_id", "i_class_id", "i_category_id"]


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    item = pd.read_parquet(
        f"{base}/item{suffix}",
        columns=["i_item_sk", "i_brand_id", "i_class_id", "i_category_id"],
    )
    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=["ss_item_sk", "ss_sold_date_sk", "ss_quantity", "ss_list_price"],
    )
    catalog_sales = pd.read_parquet(
        f"{base}/catalog_sales{suffix}",
        columns=["cs_item_sk", "cs_sold_date_sk", "cs_quantity", "cs_list_price"],
    )
    web_sales = pd.read_parquet(
        f"{base}/web_sales{suffix}",
        columns=["ws_item_sk", "ws_sold_date_sk", "ws_quantity", "ws_list_price"],
    )

    span = date_dim[(date_dim["d_year"] >= 1999) & (date_dim["d_year"] <= 2001)][
        ["d_date_sk"]
    ]
    november = date_dim[(date_dim["d_year"] == 2001) & (date_dim["d_moy"] == 11)][
        ["d_date_sk"]
    ]

    channels = [
        ("store", store_sales, "ss_item_sk", "ss_sold_date_sk", "ss_quantity",
         "ss_list_price"),
        ("catalog", catalog_sales, "cs_item_sk", "cs_sold_date_sk", "cs_quantity",
         "cs_list_price"),
        ("web", web_sales, "ws_item_sk", "ws_sold_date_sk", "ws_quantity",
         "ws_list_price"),
    ]

    # cross_items: brand/class/category families sold through all three channels
    # in 1999-2001, and the items belonging to them.
    families = None
    for _, sales, item_col, date_col, _, _ in channels:
        seen = (
            sales[[item_col, date_col]]
            .merge(span, left_on=date_col, right_on="d_date_sk")
            .merge(item, left_on=item_col, right_on="i_item_sk")[_TRIPLE]
            .drop_duplicates()
        )
        families = seen if families is None else families.merge(seen, on=_TRIPLE)
    # The outer query joins the intersection back to item with =, which no NULL
    # brand/class/category can satisfy.
    cross_items = item.merge(families.dropna(), on=_TRIPLE)

    # avg_sales: mean of quantity * list_price over the three channels, 1999-2001
    total = 0.0
    count = 0
    for _, sales, _, date_col, quantity, price in channels:
        window = sales.merge(span, left_on=date_col, right_on="d_date_sk")
        product = window[quantity].astype("float64") * window[price].astype(
            "float64"
        )
        total += float(product.sum())
        count += int(product.count())
    average_sales = total / count

    parts = []
    for name, sales, item_col, date_col, quantity, price in channels:
        window = sales.merge(
            november, left_on=date_col, right_on="d_date_sk"
        ).merge(cross_items, left_on=item_col, right_on="i_item_sk")
        number_sales = (
            window.groupby(_TRIPLE, dropna=False)
            .size()
            .reset_index(name="number_sales")
        )
        priced = window[window[quantity].notna() & window[price].notna()]
        priced = priced.assign(
            sales=priced[quantity].astype("int64") * priced[price]
        )
        summed = (
            priced.groupby(_TRIPLE, dropna=False)["sales"].sum().reset_index()
        )
        part = summed.merge(number_sales, on=_TRIPLE, how="left")
        part = part[part["sales"] > average_sales]
        part.insert(0, "channel", name)
        parts.append(part)

    y = pd.concat(parts, ignore_index=True).assign(_all=0)

    levels = []
    for depth in range(4, -1, -1):
        keys = _KEYS[:depth] if depth else ["_all"]
        level = (
            y.groupby(keys, dropna=False)
            .agg(sum_sales=("sales", "sum"), sum_number_sales=("number_sales", "sum"))
            .reset_index()
        )
        for key in _KEYS:
            if key not in keys:
                level[key] = None if key == "channel" else float("nan")
        levels.append(level[_KEYS + ["sum_sales", "sum_number_sales"]])

    result = pd.concat(levels, ignore_index=True)
    result = result.sort_values(_KEYS, na_position="first", kind="stable")
    return result.head(100).reset_index(drop=True)
