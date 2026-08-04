# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 84. Names of Edgewood customers in a given income band who appear in store returns."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    customer = pd.read_parquet(
        f"{base}/customer{suffix}",
        columns=[
            "c_customer_id",
            "c_current_cdemo_sk",
            "c_current_hdemo_sk",
            "c_current_addr_sk",
            "c_first_name",
            "c_last_name",
        ],
    )
    customer_address = pd.read_parquet(
        f"{base}/customer_address{suffix}", columns=["ca_address_sk", "ca_city"]
    )
    customer_demographics = pd.read_parquet(
        f"{base}/customer_demographics{suffix}", columns=["cd_demo_sk"]
    )
    household_demographics = pd.read_parquet(
        f"{base}/household_demographics{suffix}",
        columns=["hd_demo_sk", "hd_income_band_sk"],
    )
    income_band = pd.read_parquet(
        f"{base}/income_band{suffix}",
        columns=["ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"],
    )
    store_returns = pd.read_parquet(
        f"{base}/store_returns{suffix}", columns=["sr_cdemo_sk"]
    )

    address = customer_address[customer_address["ca_city"] == "Edgewood"][
        ["ca_address_sk"]
    ]
    bands = income_band[
        (income_band["ib_lower_bound"] >= 38128)
        & (income_band["ib_upper_bound"] <= 38128 + 50000)
    ][["ib_income_band_sk"]]
    households = household_demographics.merge(
        bands, left_on="hd_income_band_sk", right_on="ib_income_band_sk"
    )[["hd_demo_sk"]]

    result = customer.merge(
        address, left_on="c_current_addr_sk", right_on="ca_address_sk"
    )
    result = result.merge(households, left_on="c_current_hdemo_sk", right_on="hd_demo_sk")
    result = result.merge(
        customer_demographics, left_on="c_current_cdemo_sk", right_on="cd_demo_sk"
    )
    result = result.merge(store_returns, left_on="cd_demo_sk", right_on="sr_cdemo_sk")

    last = result["c_last_name"].fillna("")
    first = result["c_first_name"].fillna("")
    result = result.assign(customername=last + ", " + first)

    result = result[["c_customer_id", "customername"]]
    result = result.sort_values("c_customer_id", na_position="first")
    return result.head(100).reset_index(drop=True)
