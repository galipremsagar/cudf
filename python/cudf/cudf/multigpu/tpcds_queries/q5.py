# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 5. Sales, returns and profit per channel and outlet, rolled up."""

from __future__ import annotations

from decimal import Decimal

import pandas as pd

_ZERO = Decimal("0.00")
_START = "2000-08-23"
_END = "2000-09-06"
_MEASURES = ["sales", "returns_", "profit"]


def _null_like(series):
    """A column of the same dtype as ``series`` holding only nulls."""
    return series.where(series.isna())


def _channel_rows(sales, returns, dates, dimension, sales_key, dim_key, id_column):
    """One row per outlet: summed sales, profit, returns and loss."""
    sales = sales.merge(
        dates, left_on="date_sk", right_on="d_date_sk", how="inner"
    )[["outlet_sk", "sales_price", "profit"]]
    sales = sales.assign(return_amt=_ZERO, net_loss=_ZERO)

    returns = returns.merge(
        dates, left_on="date_sk", right_on="d_date_sk", how="inner"
    )[["outlet_sk", "return_amt", "net_loss"]]
    returns = returns.assign(sales_price=_ZERO, profit=_ZERO)

    columns = ["outlet_sk", "sales_price", "profit", "return_amt", "net_loss"]
    both = pd.concat([sales[columns], returns[columns]], ignore_index=True)

    joined = both.merge(
        dimension, left_on="outlet_sk", right_on=dim_key, how="inner"
    )
    grouped = (
        joined.groupby(id_column, dropna=False)[
            ["sales_price", "profit", "return_amt", "net_loss"]
        ]
        .sum()
        .reset_index()
    )
    return grouped


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_date"]
    )
    d_date = pd.to_datetime(date_dim["d_date"])
    dates = date_dim[
        (d_date >= pd.Timestamp(_START)) & (d_date <= pd.Timestamp(_END))
    ][["d_date_sk"]]

    # ---- store channel -------------------------------------------------
    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_store_sk",
            "ss_sold_date_sk",
            "ss_ext_sales_price",
            "ss_net_profit",
        ],
    ).rename(
        columns={
            "ss_store_sk": "outlet_sk",
            "ss_sold_date_sk": "date_sk",
            "ss_ext_sales_price": "sales_price",
            "ss_net_profit": "profit",
        }
    )
    store_returns = pd.read_parquet(
        f"{path}/store_returns{suffix}",
        columns=[
            "sr_store_sk",
            "sr_returned_date_sk",
            "sr_return_amt",
            "sr_net_loss",
        ],
    ).rename(
        columns={
            "sr_store_sk": "outlet_sk",
            "sr_returned_date_sk": "date_sk",
            "sr_return_amt": "return_amt",
            "sr_net_loss": "net_loss",
        }
    )
    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_store_id"]
    )
    ssr = _channel_rows(
        store_sales, store_returns, dates, store, "outlet_sk", "s_store_sk",
        "s_store_id",
    )

    # ---- catalog channel -----------------------------------------------
    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=[
            "cs_catalog_page_sk",
            "cs_sold_date_sk",
            "cs_ext_sales_price",
            "cs_net_profit",
        ],
    ).rename(
        columns={
            "cs_catalog_page_sk": "outlet_sk",
            "cs_sold_date_sk": "date_sk",
            "cs_ext_sales_price": "sales_price",
            "cs_net_profit": "profit",
        }
    )
    catalog_returns = pd.read_parquet(
        f"{path}/catalog_returns{suffix}",
        columns=[
            "cr_catalog_page_sk",
            "cr_returned_date_sk",
            "cr_return_amount",
            "cr_net_loss",
        ],
    ).rename(
        columns={
            "cr_catalog_page_sk": "outlet_sk",
            "cr_returned_date_sk": "date_sk",
            "cr_return_amount": "return_amt",
            "cr_net_loss": "net_loss",
        }
    )
    catalog_page = pd.read_parquet(
        f"{path}/catalog_page{suffix}",
        columns=["cp_catalog_page_sk", "cp_catalog_page_id"],
    )
    csr = _channel_rows(
        catalog_sales, catalog_returns, dates, catalog_page, "outlet_sk",
        "cp_catalog_page_sk", "cp_catalog_page_id",
    )

    # ---- web channel ----------------------------------------------------
    web_sales = pd.read_parquet(
        f"{path}/web_sales{suffix}",
        columns=[
            "ws_web_site_sk",
            "ws_sold_date_sk",
            "ws_item_sk",
            "ws_order_number",
            "ws_ext_sales_price",
            "ws_net_profit",
        ],
    )
    web_returns = pd.read_parquet(
        f"{path}/web_returns{suffix}",
        columns=[
            "wr_returned_date_sk",
            "wr_item_sk",
            "wr_order_number",
            "wr_return_amt",
            "wr_net_loss",
        ],
    )
    web_returns = web_returns.merge(
        web_sales[["ws_item_sk", "ws_order_number", "ws_web_site_sk"]],
        left_on=["wr_item_sk", "wr_order_number"],
        right_on=["ws_item_sk", "ws_order_number"],
        how="left",
    ).rename(
        columns={
            "ws_web_site_sk": "outlet_sk",
            "wr_returned_date_sk": "date_sk",
            "wr_return_amt": "return_amt",
            "wr_net_loss": "net_loss",
        }
    )
    web_sales = web_sales.rename(
        columns={
            "ws_web_site_sk": "outlet_sk",
            "ws_sold_date_sk": "date_sk",
            "ws_ext_sales_price": "sales_price",
            "ws_net_profit": "profit",
        }
    )
    web_site = pd.read_parquet(
        f"{path}/web_site{suffix}", columns=["web_site_sk", "web_site_id"]
    )
    wsr = _channel_rows(
        web_sales, web_returns, dates, web_site, "outlet_sk", "web_site_sk",
        "web_site_id",
    )

    # ---- union the three channels ---------------------------------------
    def to_channel(frame, channel, prefix, id_column):
        return pd.DataFrame(
            {
                "channel": channel,
                "id": prefix + frame[id_column],
                "sales": frame["sales_price"],
                "returns_": frame["return_amt"],
                "profit": frame["profit"] - frame["net_loss"],
            }
        )

    combined = pd.concat(
        [
            to_channel(ssr, "store channel", "store", "s_store_id"),
            to_channel(csr, "catalog channel", "catalog_page", "cp_catalog_page_id"),
            to_channel(wsr, "web channel", "web_site", "web_site_id"),
        ],
        ignore_index=True,
    )

    def rollup(frame):
        return (
            frame.groupby(["channel", "id"], dropna=False)[_MEASURES]
            .sum()
            .reset_index()
        )

    detail = rollup(combined)
    per_channel = rollup(combined.assign(id=_null_like(combined["id"])))
    grand = rollup(
        combined.assign(
            channel=_null_like(combined["channel"]),
            id=_null_like(combined["id"]),
        )
    )

    result = pd.concat([detail, per_channel, grand], ignore_index=True)
    result = result.sort_values(
        ["channel", "id"], na_position="first", kind="stable"
    )
    return result[["channel", "id", *_MEASURES]].head(100).reset_index(drop=True)
