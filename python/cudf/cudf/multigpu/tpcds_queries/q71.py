# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 71.Ranks brands by extended sales price at breakfast and dinner hours across all three channels in November 1999."""

from __future__ import annotations

import pandas as pd


def _cents(series):
    """A DECIMAL(*,2) column as an exact whole number of cents."""
    return (series.astype("float64") * 100).round()


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_moy", "d_year"]
    )
    date_dim = date_dim[(date_dim["d_moy"] == 11) & (date_dim["d_year"] == 1999)]
    date_dim = date_dim[["d_date_sk"]]

    parts = []
    for table, price, date_sk, item_sk, time_sk in (
        ("web_sales", "ws_ext_sales_price", "ws_sold_date_sk",
         "ws_item_sk", "ws_sold_time_sk"),
        ("catalog_sales", "cs_ext_sales_price", "cs_sold_date_sk",
         "cs_item_sk", "cs_sold_time_sk"),
        ("store_sales", "ss_ext_sales_price", "ss_sold_date_sk",
         "ss_item_sk", "ss_sold_time_sk"),
    ):
        sales = pd.read_parquet(
            f"{path}/{table}{suffix}",
            columns=[price, date_sk, item_sk, time_sk],
        )
        sales = sales.merge(date_dim, left_on=date_sk, right_on="d_date_sk")
        # Renaming a projection of the joined frame, rather than building a
        # DataFrame from a dict of columns: the dict form has to align three
        # separate indexes, which is a whole-frame index union.
        sales = sales.assign(**{price: _cents(sales[price])})
        parts.append(
            sales[[price, item_sk, time_sk]].rename(
                columns={
                    price: "ext_price",
                    item_sk: "sold_item_sk",
                    time_sk: "time_sk",
                }
            )
        )
        del sales
    tmp = pd.concat(parts, ignore_index=True)

    item = pd.read_parquet(
        f"{path}/item{suffix}",
        columns=["i_item_sk", "i_brand_id", "i_brand", "i_manager_id"],
    )
    item = item[item["i_manager_id"] == 1][
        ["i_item_sk", "i_brand_id", "i_brand"]
    ]

    time_dim = pd.read_parquet(
        f"{path}/time_dim{suffix}",
        columns=["t_time_sk", "t_hour", "t_minute", "t_meal_time"],
    )
    time_dim = time_dim[time_dim["t_meal_time"].isin(["breakfast", "dinner"])]
    time_dim = time_dim[["t_time_sk", "t_hour", "t_minute"]]

    joined = tmp.merge(item, left_on="sold_item_sk", right_on="i_item_sk")
    joined = joined.merge(time_dim, left_on="time_sk", right_on="t_time_sk")
    joined["priced"] = joined["ext_price"].notna().astype("int64")

    keys = ["i_brand", "i_brand_id", "t_hour", "t_minute"]
    result = (
        joined.groupby(keys, dropna=False)[["ext_price", "priced"]]
        .sum()
        .reset_index()
    )
    # SUM over a group whose values are all NULL is NULL, not zero.
    result["ext_price"] = result["ext_price"].where(result["priced"] > 0)

    result = result.sort_values(
        ["ext_price", "i_brand_id", "t_hour"],
        ascending=[False, True, True],
        na_position="first",
    )
    # Back to currency only after the ordering has been decided on the exact
    # integer cents, so two equal DECIMAL sums cannot be split by rounding.
    result["ext_price"] = result["ext_price"] / 100.0
    result = result.rename(
        columns={"i_brand_id": "brand_id", "i_brand": "brand"}
    )
    return result[
        ["brand_id", "brand", "t_hour", "t_minute", "ext_price"]
    ].reset_index(drop=True)
