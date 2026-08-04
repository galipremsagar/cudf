# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 99.Catalog shipping lag buckets by warehouse, ship mode, call center."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=[
            "cs_ship_date_sk",
            "cs_sold_date_sk",
            "cs_warehouse_sk",
            "cs_ship_mode_sk",
            "cs_call_center_sk",
        ],
    )
    warehouse = pd.read_parquet(
        f"{path}/warehouse{suffix}", columns=["w_warehouse_sk", "w_warehouse_name"]
    )
    ship_mode = pd.read_parquet(
        f"{path}/ship_mode{suffix}", columns=["sm_ship_mode_sk", "sm_type"]
    )
    call_center = pd.read_parquet(
        f"{path}/call_center{suffix}", columns=["cc_call_center_sk", "cc_name"]
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_month_seq"]
    )

    date_dim = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1200 + 11)
    ]
    warehouse = warehouse.assign(w_substr=warehouse["w_warehouse_name"].str[:20])

    df = catalog_sales.merge(
        date_dim[["d_date_sk"]], left_on="cs_ship_date_sk", right_on="d_date_sk"
    )
    df = df.merge(
        warehouse[["w_warehouse_sk", "w_substr"]],
        left_on="cs_warehouse_sk",
        right_on="w_warehouse_sk",
    )
    df = df.merge(ship_mode, left_on="cs_ship_mode_sk", right_on="sm_ship_mode_sk")
    df = df.merge(call_center, left_on="cs_call_center_sk", right_on="cc_call_center_sk")

    lag = df["cs_ship_date_sk"] - df["cs_sold_date_sk"]
    df = df.assign(
        d30=(lag <= 30).astype("int64"),
        d31_60=((lag > 30) & (lag <= 60)).astype("int64"),
        d61_90=((lag > 60) & (lag <= 90)).astype("int64"),
        d91_120=((lag > 90) & (lag <= 120)).astype("int64"),
        d120=(lag > 120).astype("int64"),
    )

    grouped = df.groupby(
        ["w_substr", "sm_type", "cc_name"], dropna=False, as_index=False
    )[["d30", "d31_60", "d61_90", "d91_120", "d120"]].sum()
    grouped = grouped.assign(cc_name_lower=grouped["cc_name"].str.lower())
    grouped = grouped.sort_values(
        ["w_substr", "sm_type", "cc_name_lower"], na_position="first"
    ).head(100)

    result = grouped[
        ["w_substr", "sm_type", "cc_name_lower", "d30", "d31_60", "d61_90",
         "d91_120", "d120"]
    ]
    result.columns = [
        "w_substr",
        "sm_type",
        "cc_name_lower",
        "30 days",
        "31-60 days",
        "61-90 days",
        "91-120 days",
        ">120 days",
    ]
    return result.reset_index(drop=True)
