# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 30.Georgia customers whose 2002 web returns far exceed their state's average."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    web_returns = pd.read_parquet(
        f"{path}/web_returns{suffix}",
        columns=[
            "wr_returned_date_sk",
            "wr_returning_customer_sk",
            "wr_returning_addr_sk",
            "wr_return_amt",
        ],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}", columns=["ca_address_sk", "ca_state"]
    )
    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=[
            "c_customer_sk",
            "c_customer_id",
            "c_current_addr_sk",
            "c_salutation",
            "c_first_name",
            "c_last_name",
            "c_preferred_cust_flag",
            "c_birth_day",
            "c_birth_month",
            "c_birth_year",
            "c_birth_country",
            "c_login",
            "c_email_address",
            "c_last_review_date_sk",
        ],
    )

    returns = web_returns.merge(
        date_dim[date_dim["d_year"] == 2002][["d_date_sk"]],
        left_on="wr_returned_date_sk",
        right_on="d_date_sk",
    )
    returns = returns.merge(
        customer_address, left_on="wr_returning_addr_sk", right_on="ca_address_sk"
    )
    # The returned amount stays decimal, as it is in SQL; the float twin is
    # only there to average and compare without decimal arithmetic element by
    # element.
    returns = returns.assign(
        ctr_total_return=returns["wr_return_amt"],
        ctr_value=returns["wr_return_amt"].astype("float64"),
    )
    ctr = returns.groupby(
        ["wr_returning_customer_sk", "ca_state"], as_index=False, dropna=False
    )[["ctr_total_return", "ctr_value"]].sum()
    ctr = ctr.rename(
        columns={"wr_returning_customer_sk": "ctr_customer_sk", "ca_state": "ctr_state"}
    )

    # The correlated subquery: the average over the returns of the same state.
    # A NULL state matches no state, so those rows drop out.
    state_average = ctr.groupby("ctr_state", as_index=False)["ctr_value"].mean()
    state_average = state_average.rename(columns={"ctr_value": "state_avg"})
    ctr = ctr.merge(state_average, on="ctr_state")
    ctr = ctr[ctr["ctr_value"] > ctr["state_avg"] * 1.2]

    georgia = customer_address[
        (customer_address["ca_state"] == "GA").fillna(False)
    ][["ca_address_sk"]]
    df = customer.merge(
        georgia, left_on="c_current_addr_sk", right_on="ca_address_sk"
    )
    df = df.merge(ctr, left_on="c_customer_sk", right_on="ctr_customer_sk")

    columns = [
        "c_customer_id",
        "c_salutation",
        "c_first_name",
        "c_last_name",
        "c_preferred_cust_flag",
        "c_birth_day",
        "c_birth_month",
        "c_birth_year",
        "c_birth_country",
        "c_login",
        "c_email_address",
        "c_last_review_date_sk",
        "ctr_total_return",
    ]
    out = df.sort_values([*columns[:-1], "ctr_value"], na_position="first").head(100)
    return out[columns].reset_index(drop=True)
