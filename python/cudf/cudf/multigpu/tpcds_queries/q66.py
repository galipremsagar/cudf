# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 66.Monthly web and catalog shipments per warehouse for two
carriers during a morning time window in 2001."""

from __future__ import annotations

import pandas as pd

WAREHOUSE_COLUMNS = [
    "w_warehouse_sk",
    "w_warehouse_name",
    "w_warehouse_sq_ft",
    "w_city",
    "w_county",
    "w_state",
    "w_country",
]

GROUP_KEYS = [
    "w_warehouse_name",
    "w_warehouse_sq_ft",
    "w_city",
    "w_county",
    "w_state",
    "w_country",
    "d_year",
]

MONTHS = [
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
]


def _amount(frame, value_column, quantity_column):
    """value * quantity, NULL (which SUM skips) rendered as a zero addend.

    The money columns are DECIMAL in the parquet; libcudf has neither an
    arithmetic-with-scalar nor a group-by sum for decimals, so everything here
    is float64. TPC-DS amounts have at most a few significant digits beyond
    the cent, well inside float64's exactly representable range.
    """
    known = frame[value_column].notna() & frame[quantity_column].notna()
    value = frame[value_column].astype("float64").fillna(0.0)
    quantity = frame[quantity_column].astype("float64").fillna(0.0)
    return (value * quantity).where(known, 0.0)


def _branch(sales, warehouse, date_dim, time_dim, ship_mode, columns):
    date_sk, time_sk, mode_sk, warehouse_sk, price, net, quantity = columns
    frame = (
        sales.merge(warehouse, left_on=warehouse_sk, right_on="w_warehouse_sk")
        .merge(date_dim, left_on=date_sk, right_on="d_date_sk")
        .merge(time_dim, left_on=time_sk, right_on="t_time_sk")
        .merge(ship_mode, left_on=mode_sk, right_on="sm_ship_mode_sk")
    )
    sales_amount = _amount(frame, price, quantity)
    net_amount = _amount(frame, net, quantity)
    monthly = {}
    for number, month in enumerate(MONTHS, start=1):
        in_month = frame["d_moy"] == number
        monthly[f"{month}_sales"] = sales_amount.where(in_month, 0.0)
        monthly[f"{month}_net"] = net_amount.where(in_month, 0.0)
    frame = frame.assign(**monthly)
    return (
        frame.groupby(GROUP_KEYS, dropna=False)[list(monthly)].sum().reset_index()
    )


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    warehouse = pd.read_parquet(
        f"{path}/warehouse{suffix}", columns=WAREHOUSE_COLUMNS
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    date_dim = date_dim[date_dim["d_year"] == 2001]
    time_dim = pd.read_parquet(
        f"{path}/time_dim{suffix}", columns=["t_time_sk", "t_time"]
    )
    time_dim = time_dim[
        (time_dim["t_time"] >= 30838) & (time_dim["t_time"] <= 30838 + 28800)
    ][["t_time_sk"]]
    ship_mode = pd.read_parquet(
        f"{path}/ship_mode{suffix}", columns=["sm_ship_mode_sk", "sm_carrier"]
    )
    ship_mode = ship_mode[ship_mode["sm_carrier"].isin(["DHL", "BARIAN"])][
        ["sm_ship_mode_sk"]
    ]

    web_sales = pd.read_parquet(
        f"{path}/web_sales{suffix}",
        columns=[
            "ws_sold_date_sk",
            "ws_sold_time_sk",
            "ws_ship_mode_sk",
            "ws_warehouse_sk",
            "ws_ext_sales_price",
            "ws_net_paid",
            "ws_quantity",
        ],
    )
    web = _branch(
        web_sales,
        warehouse,
        date_dim,
        time_dim,
        ship_mode,
        [
            "ws_sold_date_sk",
            "ws_sold_time_sk",
            "ws_ship_mode_sk",
            "ws_warehouse_sk",
            "ws_ext_sales_price",
            "ws_net_paid",
            "ws_quantity",
        ],
    )

    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=[
            "cs_sold_date_sk",
            "cs_sold_time_sk",
            "cs_ship_mode_sk",
            "cs_warehouse_sk",
            "cs_sales_price",
            "cs_net_paid_inc_tax",
            "cs_quantity",
        ],
    )
    catalog = _branch(
        catalog_sales,
        warehouse,
        date_dim,
        time_dim,
        ship_mode,
        [
            "cs_sold_date_sk",
            "cs_sold_time_sk",
            "cs_ship_mode_sk",
            "cs_warehouse_sk",
            "cs_sales_price",
            "cs_net_paid_inc_tax",
            "cs_quantity",
        ],
    )

    value_columns = [f"{month}_{kind}" for month in MONTHS for kind in ("sales", "net")]
    combined = pd.concat([web, catalog], ignore_index=True)
    combined = (
        combined.groupby(GROUP_KEYS, dropna=False)[value_columns].sum().reset_index()
    )

    per_sq_foot = {}
    for month in MONTHS:
        per_sq_foot[f"{month}_sales_per_sq_foot"] = combined[
            f"{month}_sales"
        ].astype("float64") / combined["w_warehouse_sq_ft"].astype("float64")
    combined = combined.assign(ship_carriers="DHL,BARIAN", **per_sq_foot)

    combined = combined.sort_values("w_warehouse_name", na_position="first").head(100)
    ordered = (
        [
            "w_warehouse_name",
            "w_warehouse_sq_ft",
            "w_city",
            "w_county",
            "w_state",
            "w_country",
            "ship_carriers",
            "d_year",
        ]
        + [f"{month}_sales" for month in MONTHS]
        + [f"{month}_sales_per_sq_foot" for month in MONTHS]
        + [f"{month}_net" for month in MONTHS]
    )
    return combined[ordered].reset_index(drop=True)
