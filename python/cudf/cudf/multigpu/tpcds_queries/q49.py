# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 49.Items with the worst return rates in December 2001, ranked separately for the web, catalog and store channels."""

from __future__ import annotations

import pandas as pd


def _channel(
    run_config,
    channel,
    sales_table,
    returns_table,
    sales_columns,
    returns_columns,
    date_key,
    order_key,
    item_key,
    return_order_key,
    return_item_key,
    quantity,
    net_paid,
    net_profit,
    return_quantity,
    return_amount,
):
    date_dim = pd.read_parquet(
        f"{run_config.dataset_path}/date_dim{run_config.suffix}",
        columns=["d_date_sk", "d_year", "d_moy"],
    )
    date_dim = date_dim[(date_dim["d_year"] == 2001) & (date_dim["d_moy"] == 12)]

    sales = pd.read_parquet(
        f"{run_config.dataset_path}/{sales_table}{run_config.suffix}",
        columns=sales_columns,
    )
    returns = pd.read_parquet(
        f"{run_config.dataset_path}/{returns_table}{run_config.suffix}",
        columns=returns_columns,
    )

    sales = sales.assign(
        **{
            net_paid: sales[net_paid].astype("float64"),
            net_profit: sales[net_profit].astype("float64"),
        }
    )
    returns = returns.assign(
        **{return_amount: returns[return_amount].astype("float64")}
    )

    sales = sales[
        (sales[net_profit] > 1) & (sales[net_paid] > 0) & (sales[quantity] > 0)
    ]

    merged = sales.merge(
        returns,
        left_on=[order_key, item_key],
        right_on=[return_order_key, return_item_key],
        how="left",
    )
    # The outer join is followed by a predicate on the right side, which drops
    # every unmatched row.
    merged = merged[merged[return_amount] > 10000]
    merged = merged.merge(date_dim, left_on=date_key, right_on="d_date_sk")

    merged = merged.assign(
        **{
            return_quantity: merged[return_quantity].fillna(0),
            return_amount: merged[return_amount].fillna(0),
            quantity: merged[quantity].fillna(0),
            net_paid: merged[net_paid].fillna(0),
        }
    )

    grouped = (
        merged.groupby(item_key, dropna=False)[
            [return_quantity, quantity, return_amount, net_paid]
        ]
        .sum()
        .reset_index()
    )
    grouped["return_ratio"] = grouped[return_quantity] / grouped[quantity]
    grouped["currency_ratio"] = grouped[return_amount] / grouped[net_paid]
    grouped = grouped.rename(columns={item_key: "item"})

    grouped["return_rank"] = grouped["return_ratio"].rank(method="min")
    grouped["currency_rank"] = grouped["currency_ratio"].rank(method="min")

    grouped = grouped[
        (grouped["return_rank"] <= 10) | (grouped["currency_rank"] <= 10)
    ]
    grouped = grouped.assign(channel=channel)
    return grouped[["channel", "item", "return_ratio", "return_rank", "currency_rank"]]


def query(run_config):
    web = _channel(
        run_config,
        "web",
        "web_sales",
        "web_returns",
        [
            "ws_item_sk",
            "ws_order_number",
            "ws_sold_date_sk",
            "ws_quantity",
            "ws_net_paid",
            "ws_net_profit",
        ],
        ["wr_item_sk", "wr_order_number", "wr_return_quantity", "wr_return_amt"],
        date_key="ws_sold_date_sk",
        order_key="ws_order_number",
        item_key="ws_item_sk",
        return_order_key="wr_order_number",
        return_item_key="wr_item_sk",
        quantity="ws_quantity",
        net_paid="ws_net_paid",
        net_profit="ws_net_profit",
        return_quantity="wr_return_quantity",
        return_amount="wr_return_amt",
    )
    catalog = _channel(
        run_config,
        "catalog",
        "catalog_sales",
        "catalog_returns",
        [
            "cs_item_sk",
            "cs_order_number",
            "cs_sold_date_sk",
            "cs_quantity",
            "cs_net_paid",
            "cs_net_profit",
        ],
        ["cr_item_sk", "cr_order_number", "cr_return_quantity", "cr_return_amount"],
        date_key="cs_sold_date_sk",
        order_key="cs_order_number",
        item_key="cs_item_sk",
        return_order_key="cr_order_number",
        return_item_key="cr_item_sk",
        quantity="cs_quantity",
        net_paid="cs_net_paid",
        net_profit="cs_net_profit",
        return_quantity="cr_return_quantity",
        return_amount="cr_return_amount",
    )
    store = _channel(
        run_config,
        "store",
        "store_sales",
        "store_returns",
        [
            "ss_item_sk",
            "ss_ticket_number",
            "ss_sold_date_sk",
            "ss_quantity",
            "ss_net_paid",
            "ss_net_profit",
        ],
        ["sr_item_sk", "sr_ticket_number", "sr_return_quantity", "sr_return_amt"],
        date_key="ss_sold_date_sk",
        order_key="ss_ticket_number",
        item_key="ss_item_sk",
        return_order_key="sr_ticket_number",
        return_item_key="sr_item_sk",
        quantity="ss_quantity",
        net_paid="ss_net_paid",
        net_profit="ss_net_profit",
        return_quantity="sr_return_quantity",
        return_amount="sr_return_amt",
    )

    combined = pd.concat([web, catalog, store], ignore_index=True).drop_duplicates()

    result = (
        combined.sort_values(
            ["channel", "return_rank", "currency_rank", "item"],
            na_position="first",
        )
        .head(100)
        .reset_index(drop=True)
    )
    return result[
        ["channel", "item", "return_ratio", "return_rank", "currency_rank"]
    ]
