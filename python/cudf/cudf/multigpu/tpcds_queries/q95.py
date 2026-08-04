# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 95.Shipping cost and profit for split web orders that were returned."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    web_sales = pd.read_parquet(
        f"{path}/web_sales{suffix}",
        columns=[
            "ws_ship_date_sk",
            "ws_ship_addr_sk",
            "ws_web_site_sk",
            "ws_order_number",
            "ws_warehouse_sk",
            "ws_ext_ship_cost",
            "ws_net_profit",
        ],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_date"]
    )
    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}", columns=["ca_address_sk", "ca_state"]
    )
    web_site = pd.read_parquet(
        f"{path}/web_site{suffix}", columns=["web_site_sk", "web_company_name"]
    )
    web_returns = pd.read_parquet(
        f"{path}/web_returns{suffix}", columns=["wr_order_number"]
    )

    d_date = pd.to_datetime(date_dim["d_date"])
    date_dim = date_dim[
        (d_date >= pd.Timestamp("1999-02-01")) & (d_date <= pd.Timestamp("1999-04-02"))
    ][["d_date_sk"]]
    customer_address = customer_address[customer_address["ca_state"] == "IL"][
        ["ca_address_sk"]
    ]
    web_site = web_site[web_site["web_company_name"] == "pri"][["web_site_sk"]]

    # ws_wh: orders shipped from more than one warehouse.  The self-join is a
    # count of the distinct non-NULL warehouses per order -- ``<>`` is never
    # true when either side is NULL.
    pairs = web_sales[["ws_order_number", "ws_warehouse_sk"]].dropna()
    warehouses = (
        pairs.drop_duplicates()
        .groupby("ws_order_number", as_index=False)["ws_warehouse_sk"]
        .count()
    )
    warehouses.columns = ["ws_order_number", "n_warehouses"]
    ws_wh = warehouses[warehouses["n_warehouses"] > 1][["ws_order_number"]]

    # ...and, of those, the ones that also appear in web_returns.  Both IN
    # subqueries are semi-joins against key sets that are already distinct, so
    # an inner merge cannot duplicate a web_sales row.
    returned = web_returns[["wr_order_number"]].drop_duplicates()

    df = web_sales.merge(date_dim, left_on="ws_ship_date_sk", right_on="d_date_sk")
    df = df.merge(
        customer_address, left_on="ws_ship_addr_sk", right_on="ca_address_sk"
    )
    df = df.merge(web_site, left_on="ws_web_site_sk", right_on="web_site_sk")
    df = df.merge(ws_wh, on="ws_order_number")
    df = df.merge(returned, left_on="ws_order_number", right_on="wr_order_number")

    # COUNT(DISTINCT ...) as a distributed de-duplication rather than
    # Series.nunique(), which gathers every value onto one GPU.
    order_count = len(df[["ws_order_number"]].drop_duplicates())
    # libcudf has no decimal sum, and these money columns are well inside
    # float64's exactly-representable range.
    return pd.DataFrame(
        {
            "order count": [order_count],
            "total shipping cost": [df["ws_ext_ship_cost"].astype("float64").sum()],
            "total net profit": [df["ws_net_profit"].astype("float64").sum()],
        }
    )
