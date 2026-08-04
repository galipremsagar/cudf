# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 50.How long store customers took to return August 2001 purchases, bucketed by store."""

from __future__ import annotations

import pandas as pd

_STORE_COLUMNS = [
    "s_store_name",
    "s_company_id",
    "s_street_number",
    "s_street_name",
    "s_street_type",
    "s_suite_number",
    "s_city",
    "s_county",
    "s_state",
    "s_zip",
]


def query(run_config):
    store_sales = pd.read_parquet(
        f"{run_config.dataset_path}/store_sales{run_config.suffix}",
        columns=[
            "ss_ticket_number",
            "ss_item_sk",
            "ss_customer_sk",
            "ss_sold_date_sk",
            "ss_store_sk",
        ],
    )
    store_returns = pd.read_parquet(
        f"{run_config.dataset_path}/store_returns{run_config.suffix}",
        columns=[
            "sr_ticket_number",
            "sr_item_sk",
            "sr_customer_sk",
            "sr_returned_date_sk",
        ],
    )
    store = pd.read_parquet(
        f"{run_config.dataset_path}/store{run_config.suffix}",
        columns=["s_store_sk"] + _STORE_COLUMNS,
    )
    date_dim = pd.read_parquet(
        f"{run_config.dataset_path}/date_dim{run_config.suffix}",
        columns=["d_date_sk", "d_year", "d_moy"],
    )

    d1 = date_dim[["d_date_sk"]]
    d2 = date_dim[(date_dim["d_year"] == 2001) & (date_dim["d_moy"] == 8)][
        ["d_date_sk"]
    ].rename(columns={"d_date_sk": "d2_date_sk"})

    # SQL equality never matches NULL, but pandas merge treats NaN as a joinable
    # value and pairs every null-customer sale with every null-customer return.
    # At SF1 that invented 457 rows. Dropping the null keys before the merge is
    # what `ss_customer_sk = sr_customer_sk` actually means.
    store_sales = store_sales.dropna(subset=["ss_customer_sk"])
    store_returns = store_returns.dropna(subset=["sr_customer_sk"])

    merged = (
        store_sales.merge(
            store_returns,
            left_on=["ss_ticket_number", "ss_item_sk", "ss_customer_sk"],
            right_on=["sr_ticket_number", "sr_item_sk", "sr_customer_sk"],
        )
        .merge(d1, left_on="ss_sold_date_sk", right_on="d_date_sk")
        .merge(d2, left_on="sr_returned_date_sk", right_on="d2_date_sk")
        .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    )

    lag = merged["sr_returned_date_sk"] - merged["ss_sold_date_sk"]
    buckets = {
        "30 days": lag <= 30,
        "31-60 days": (lag > 30) & (lag <= 60),
        "61-90 days": (lag > 60) & (lag <= 90),
        "91-120 days": (lag > 90) & (lag <= 120),
        ">120 days": lag > 120,
    }
    for name, mask in buckets.items():
        merged[name] = mask.astype("int64")

    value_columns = list(buckets)
    grouped = (
        merged.groupby(_STORE_COLUMNS, dropna=False)[value_columns]
        .sum()
        .reset_index()
    )

    result = (
        grouped.sort_values(_STORE_COLUMNS).head(100).reset_index(drop=True)
    )
    return result[_STORE_COLUMNS + value_columns]
