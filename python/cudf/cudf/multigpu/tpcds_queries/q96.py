# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 96.Late-evening store sales to large households at one store."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=["ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk"],
    )
    household_demographics = pd.read_parquet(
        f"{path}/household_demographics{suffix}",
        columns=["hd_demo_sk", "hd_dep_count"],
    )
    time_dim = pd.read_parquet(
        f"{path}/time_dim{suffix}", columns=["t_time_sk", "t_hour", "t_minute"]
    )
    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_store_name"]
    )

    time_dim = time_dim[(time_dim["t_hour"] == 20) & (time_dim["t_minute"] >= 30)]
    household_demographics = household_demographics[
        household_demographics["hd_dep_count"] == 7
    ]
    store = store[store["s_store_name"] == "ese"]

    df = store_sales.merge(
        time_dim[["t_time_sk"]], left_on="ss_sold_time_sk", right_on="t_time_sk"
    )
    df = df.merge(
        household_demographics[["hd_demo_sk"]],
        left_on="ss_hdemo_sk",
        right_on="hd_demo_sk",
    )
    df = df.merge(store[["s_store_sk"]], left_on="ss_store_sk", right_on="s_store_sk")

    return pd.DataFrame({"count(*)": [len(df)]})
