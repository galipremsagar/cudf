# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 81. Georgia customers whose 2000 catalog returns far exceed their state's average."""

from __future__ import annotations

import pandas as pd

_OUTPUT = [
    "c_customer_id",
    "c_salutation",
    "c_first_name",
    "c_last_name",
    "ca_street_number",
    "ca_street_name",
    "ca_street_type",
    "ca_suite_number",
    "ca_city",
    "ca_county",
    "ca_state",
    "ca_zip",
    "ca_country",
    "ca_gmt_offset",
    "ca_location_type",
    "ctr_total_return",
]


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    catalog_returns = pd.read_parquet(
        f"{base}/catalog_returns{suffix}",
        columns=[
            "cr_returned_date_sk",
            "cr_returning_customer_sk",
            "cr_returning_addr_sk",
            "cr_return_amt_inc_tax",
        ],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    customer_address = pd.read_parquet(
        f"{base}/customer_address{suffix}",
        columns=[
            "ca_address_sk",
            "ca_street_number",
            "ca_street_name",
            "ca_street_type",
            "ca_suite_number",
            "ca_city",
            "ca_county",
            "ca_state",
            "ca_zip",
            "ca_country",
            "ca_gmt_offset",
            "ca_location_type",
        ],
    )
    customer = pd.read_parquet(
        f"{base}/customer{suffix}",
        columns=[
            "c_customer_sk",
            "c_customer_id",
            "c_current_addr_sk",
            "c_salutation",
            "c_first_name",
            "c_last_name",
        ],
    )

    # customer_total_return
    dates = date_dim[date_dim["d_year"] == 2000][["d_date_sk"]]
    ctr = catalog_returns.merge(
        dates, left_on="cr_returned_date_sk", right_on="d_date_sk"
    )
    ctr = ctr.merge(
        customer_address[["ca_address_sk", "ca_state"]],
        left_on="cr_returning_addr_sk",
        right_on="ca_address_sk",
    )
    # min_count=1 so a customer whose returns are all NULL sums to NULL, as SQL does
    ctr = ctr.groupby(
        ["cr_returning_customer_sk", "ca_state"], as_index=False, dropna=False
    )["cr_return_amt_inc_tax"].sum(min_count=1)
    ctr = ctr.rename(
        columns={
            "cr_returning_customer_sk": "ctr_customer_sk",
            "ca_state": "ctr_state",
            "cr_return_amt_inc_tax": "ctr_total_return",
        }
    )
    ctr["ctr_amount"] = ctr["ctr_total_return"].astype("float64")

    # avg(ctr_total_return) * 1.2, per state
    threshold = ctr.groupby("ctr_state", as_index=False)["ctr_amount"].mean()
    threshold = threshold.rename(columns={"ctr_amount": "ctr_threshold"})
    threshold["ctr_threshold"] = threshold["ctr_threshold"] * 1.2

    ctr = ctr.merge(threshold, on="ctr_state", how="left")
    ctr = ctr[ctr["ctr_amount"] > ctr["ctr_threshold"]]

    georgia = customer_address[customer_address["ca_state"] == "GA"]
    result = customer.merge(
        georgia, left_on="c_current_addr_sk", right_on="ca_address_sk"
    )
    result = result.merge(ctr, left_on="c_customer_sk", right_on="ctr_customer_sk")

    result = result[_OUTPUT].sort_values(_OUTPUT, na_position="last")
    return result.head(100).reset_index(drop=True)
