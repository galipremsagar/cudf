# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 75.Finds the book products whose net unit sales fell by more than ten percent from 2001 to 2002 across all three channels."""

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


ITEM_KEYS = ["i_brand_id", "i_class_id", "i_category_id", "i_manufact_id"]


def _channel(path, suffix, item, date_dim, table, returns_table, item_sk,
             date_sk, order_key, quantity, price, return_item_sk,
             return_order_key, return_quantity, return_amount):
    sales = pd.read_parquet(
        f"{path}/{table}{suffix}",
        columns=[item_sk, date_sk, order_key, quantity, price],
    )
    sales = sales.merge(item, left_on=item_sk, right_on="i_item_sk")
    sales = sales.merge(date_dim, left_on=date_sk, right_on="d_date_sk")

    returns = pd.read_parquet(
        f"{path}/{returns_table}{suffix}",
        columns=[
            return_item_sk,
            return_order_key,
            return_quantity,
            return_amount,
        ],
    )
    sales = sales.merge(
        returns,
        how="left",
        left_on=[order_key, item_sk],
        right_on=[return_order_key, return_item_sk],
    )

    detail = sales[["d_year"] + ITEM_KEYS].copy()
    detail["sales_cnt"] = sales[quantity].astype("float64") - sales[
        return_quantity
    ].astype("float64").fillna(0.0)
    detail["sales_amt"] = _cents(sales[price]) - _cents(
        sales[return_amount]
    ).fillna(0.0)
    return detail


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_category"] + ITEM_KEYS
    )
    item = item[item["i_category"] == "Books"][["i_item_sk"] + ITEM_KEYS]
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )

    detail = pd.concat(
        [
            _channel(
                path, suffix, item, date_dim,
                "catalog_sales", "catalog_returns",
                "cs_item_sk", "cs_sold_date_sk", "cs_order_number",
                "cs_quantity", "cs_ext_sales_price",
                "cr_item_sk", "cr_order_number",
                "cr_return_quantity", "cr_return_amount",
            ),
            _channel(
                path, suffix, item, date_dim,
                "store_sales", "store_returns",
                "ss_item_sk", "ss_sold_date_sk", "ss_ticket_number",
                "ss_quantity", "ss_ext_sales_price",
                "sr_item_sk", "sr_ticket_number",
                "sr_return_quantity", "sr_return_amt",
            ),
            _channel(
                path, suffix, item, date_dim,
                "web_sales", "web_returns",
                "ws_item_sk", "ws_sold_date_sk", "ws_order_number",
                "ws_quantity", "ws_ext_sales_price",
                "wr_item_sk", "wr_order_number",
                "wr_return_quantity", "wr_return_amt",
            ),
        ],
        ignore_index=True,
    )
    # UNION, not UNION ALL: the three legs are de-duplicated together.
    detail = detail.drop_duplicates()

    detail["cnt_seen"] = detail["sales_cnt"].notna().astype("int64")
    detail["amt_seen"] = detail["sales_amt"].notna().astype("int64")
    keys = ["d_year"] + ITEM_KEYS
    all_sales = (
        detail.groupby(keys, dropna=False)[
            ["sales_cnt", "sales_amt", "cnt_seen", "amt_seen"]
        ]
        .sum()
        .reset_index()
    )
    # SUM over a group whose values are all NULL is NULL, not zero.
    all_sales["sales_cnt"] = all_sales["sales_cnt"].where(
        all_sales["cnt_seen"] > 0
    )
    all_sales["sales_amt"] = all_sales["sales_amt"].where(
        all_sales["amt_seen"] > 0
    )
    all_sales = all_sales[keys + ["sales_cnt", "sales_amt"]]

    current = all_sales[all_sales["d_year"] == 2002].rename(
        columns={"sales_cnt": "curr_cnt", "sales_amt": "curr_amt"}
    )
    previous = all_sales[all_sales["d_year"] == 2001].rename(
        columns={
            "d_year": "prev_year",
            "sales_cnt": "prev_cnt",
            "sales_amt": "prev_amt",
        }
    )
    # The two years are lined up by an equality on the four item attributes,
    # every one of which is nullable in ``item``. GROUP BY keeps a NULL group
    # (all the brand-less Books items land in one row per year), but SQL
    # equality never matches NULL to NULL, whereas a pandas merge does -- which
    # would pair those two fat groups and invent a row. Drop them on both
    # sides, the way an inner join must.
    current = current.dropna(subset=ITEM_KEYS)
    previous = previous.dropna(subset=ITEM_KEYS)

    joined = current.merge(previous, on=ITEM_KEYS)
    ratio = joined["curr_cnt"].astype("float64") / joined[
        "prev_cnt"
    ].astype("float64")
    # Division by zero is NULL in SQL, so the comparison never qualifies.
    joined = joined[(joined["prev_cnt"] != 0) & (ratio < 0.9)]

    joined["sales_cnt_diff"] = joined["curr_cnt"] - joined["prev_cnt"]
    joined["sales_amt_diff"] = joined["curr_amt"] - joined["prev_amt"]
    result = joined.sort_values(
        ["sales_cnt_diff", "sales_amt_diff"], na_position="last"
    ).head(100)
    result["sales_amt_diff_str"] = _decimal_str(result["sales_amt_diff"])
    return result[
        ["prev_year", "d_year"]
        + ITEM_KEYS
        + ["prev_cnt", "curr_cnt", "sales_cnt_diff", "sales_amt_diff_str"]
    ].reset_index(drop=True)
