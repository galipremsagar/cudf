# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 91.Catalog return losses by call center for a customer segment."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    call_center = pd.read_parquet(
        f"{path}/call_center{suffix}",
        columns=["cc_call_center_sk", "cc_call_center_id", "cc_name", "cc_manager"],
    )
    catalog_returns = pd.read_parquet(
        f"{path}/catalog_returns{suffix}",
        columns=[
            "cr_call_center_sk",
            "cr_returned_date_sk",
            "cr_returning_customer_sk",
            "cr_net_loss",
        ],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year", "d_moy"]
    )
    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=[
            "c_customer_sk",
            "c_current_cdemo_sk",
            "c_current_hdemo_sk",
            "c_current_addr_sk",
        ],
    )
    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}",
        columns=["ca_address_sk", "ca_gmt_offset"],
    )
    customer_demographics = pd.read_parquet(
        f"{path}/customer_demographics{suffix}",
        columns=["cd_demo_sk", "cd_marital_status", "cd_education_status"],
    )
    household_demographics = pd.read_parquet(
        f"{path}/household_demographics{suffix}",
        columns=["hd_demo_sk", "hd_buy_potential"],
    )

    date_dim = date_dim[(date_dim["d_year"] == 1998) & (date_dim["d_moy"] == 11)]
    customer_address = customer_address[
        customer_address["ca_gmt_offset"].astype("float64") == -7.0
    ]
    household_demographics = household_demographics[
        household_demographics["hd_buy_potential"]
        .str.startswith("Unknown")
        .fillna(False)
    ]
    demo = customer_demographics[
        (
            (customer_demographics["cd_marital_status"] == "M")
            & (customer_demographics["cd_education_status"] == "Unknown")
        )
        | (
            (customer_demographics["cd_marital_status"] == "W")
            & (customer_demographics["cd_education_status"] == "Advanced Degree")
        )
    ]

    df = catalog_returns.merge(
        date_dim[["d_date_sk"]], left_on="cr_returned_date_sk", right_on="d_date_sk"
    )
    df = df.merge(call_center, left_on="cr_call_center_sk", right_on="cc_call_center_sk")
    df = df.merge(customer, left_on="cr_returning_customer_sk", right_on="c_customer_sk")
    df = df.merge(demo, left_on="c_current_cdemo_sk", right_on="cd_demo_sk")
    df = df.merge(
        household_demographics[["hd_demo_sk"]],
        left_on="c_current_hdemo_sk",
        right_on="hd_demo_sk",
    )
    df = df.merge(
        customer_address[["ca_address_sk"]],
        left_on="c_current_addr_sk",
        right_on="ca_address_sk",
    )

    # libcudf has no group-by sum for decimals, so the money column becomes
    # float64 first. TPC-DS amounts are far inside float64's exact range.
    df = df.assign(cr_net_loss=df["cr_net_loss"].astype("float64"))

    grouped = df.groupby(
        [
            "cc_call_center_id",
            "cc_name",
            "cc_manager",
            "cd_marital_status",
            "cd_education_status",
        ],
        dropna=False,
        as_index=False,
    )["cr_net_loss"].sum()

    grouped = grouped.sort_values("cr_net_loss", ascending=False)
    result = grouped[["cc_call_center_id", "cc_name", "cc_manager", "cr_net_loss"]]
    result.columns = ["Call_Center", "Call_Center_Name", "Manager", "Returns_Loss"]
    return result.reset_index(drop=True)
