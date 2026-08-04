# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 88. Half-hourly store ticket counts between 8:30 and 12:30 for one store and some household types."""

from __future__ import annotations

import pandas as pd

_HALF_HOURS = [
    ("h8_30_to_9", 8, True),
    ("h9_to_9_30", 9, False),
    ("h9_30_to_10", 9, True),
    ("h10_to_10_30", 10, False),
    ("h10_30_to_11", 10, True),
    ("h11_to_11_30", 11, False),
    ("h11_30_to_12", 11, True),
    ("h12_to_12_30", 12, False),
]


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=["ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk"],
    )
    household_demographics = pd.read_parquet(
        f"{base}/household_demographics{suffix}",
        columns=["hd_demo_sk", "hd_dep_count", "hd_vehicle_count"],
    )
    time_dim = pd.read_parquet(
        f"{base}/time_dim{suffix}", columns=["t_time_sk", "t_hour", "t_minute"]
    )
    store = pd.read_parquet(
        f"{base}/store{suffix}", columns=["s_store_sk", "s_store_name"]
    )

    households = household_demographics[
        (
            (household_demographics["hd_dep_count"] == 4)
            & (household_demographics["hd_vehicle_count"] <= 4 + 2)
        )
        | (
            (household_demographics["hd_dep_count"] == 2)
            & (household_demographics["hd_vehicle_count"] <= 2 + 2)
        )
        | (
            (household_demographics["hd_dep_count"] == 0)
            & (household_demographics["hd_vehicle_count"] <= 0 + 2)
        )
    ][["hd_demo_sk"]]

    stores = store[store["s_store_name"] == "ese"][["s_store_sk"]]
    times = time_dim[time_dim["t_hour"].isin([8, 9, 10, 11, 12])][
        ["t_time_sk", "t_hour", "t_minute"]
    ]

    frame = store_sales.merge(
        households, left_on="ss_hdemo_sk", right_on="hd_demo_sk"
    )
    frame = frame.merge(stores, left_on="ss_store_sk", right_on="s_store_sk")
    frame = frame.merge(times, left_on="ss_sold_time_sk", right_on="t_time_sk")

    hour = frame["t_hour"]
    minute = frame["t_minute"]
    counts = {}
    for name, at_hour, late_half in _HALF_HOURS:
        in_half = (minute >= 30) if late_half else (minute < 30)
        counts[name] = [int(((hour == at_hour) & in_half).sum())]
    return pd.DataFrame(counts)
