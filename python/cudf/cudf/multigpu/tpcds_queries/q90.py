# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 90. Ratio of morning to evening web page views for one household profile."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    web_sales = pd.read_parquet(
        f"{base}/web_sales{suffix}",
        columns=["ws_sold_time_sk", "ws_ship_hdemo_sk", "ws_web_page_sk"],
    )
    household_demographics = pd.read_parquet(
        f"{base}/household_demographics{suffix}",
        columns=["hd_demo_sk", "hd_dep_count"],
    )
    time_dim = pd.read_parquet(
        f"{base}/time_dim{suffix}", columns=["t_time_sk", "t_hour"]
    )
    web_page = pd.read_parquet(
        f"{base}/web_page{suffix}", columns=["wp_web_page_sk", "wp_char_count"]
    )

    households = household_demographics[
        household_demographics["hd_dep_count"] == 6
    ][["hd_demo_sk"]]
    pages = web_page[
        (web_page["wp_char_count"] >= 5000) & (web_page["wp_char_count"] <= 5200)
    ][["wp_web_page_sk"]]

    frame = web_sales.merge(
        households, left_on="ws_ship_hdemo_sk", right_on="hd_demo_sk"
    )
    frame = frame.merge(pages, left_on="ws_web_page_sk", right_on="wp_web_page_sk")
    frame = frame.merge(time_dim, left_on="ws_sold_time_sk", right_on="t_time_sk")

    hour = frame["t_hour"]
    amc = int(((hour >= 8) & (hour <= 8 + 1)).sum())
    pmc = int(((hour >= 19) & (hour <= 19 + 1)).sum())

    ratio = None if pmc == 0 else amc / pmc
    return pd.DataFrame({"am_pm_ratio": [ratio]}, dtype="float64")
