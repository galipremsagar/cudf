# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 24.Store sales of peach items to customers shopping away from home."""

from __future__ import annotations

import pandas as pd

_SSALES_KEYS = [
    "c_last_name",
    "c_first_name",
    "s_store_name",
    "ca_state",
    "s_state",
    "i_color",
    "i_current_price",
    "i_manager_id",
    "i_units",
    "i_size",
]


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_ticket_number",
            "ss_item_sk",
            "ss_customer_sk",
            "ss_store_sk",
            "ss_net_paid",
        ],
    )
    store_returns = pd.read_parquet(
        f"{path}/store_returns{suffix}",
        columns=["sr_ticket_number", "sr_item_sk"],
    )
    store = pd.read_parquet(
        f"{path}/store{suffix}",
        columns=["s_store_sk", "s_store_name", "s_state", "s_zip", "s_market_id"],
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}",
        columns=[
            "i_item_sk",
            "i_color",
            "i_current_price",
            "i_manager_id",
            "i_units",
            "i_size",
        ],
    )
    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=[
            "c_customer_sk",
            "c_first_name",
            "c_last_name",
            "c_current_addr_sk",
            "c_birth_country",
        ],
    )
    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}",
        columns=["ca_address_sk", "ca_state", "ca_zip", "ca_country"],
    )

    store = store[store["s_market_id"] == 8][
        ["s_store_sk", "s_store_name", "s_state", "s_zip"]
    ]
    item = item.assign(i_current_price=item["i_current_price"].astype("float64"))

    df = store_sales.merge(
        store_returns,
        left_on=["ss_ticket_number", "ss_item_sk"],
        right_on=["sr_ticket_number", "sr_item_sk"],
    )
    df = df.merge(customer, left_on="ss_customer_sk", right_on="c_customer_sk")
    df = df.merge(item, left_on="ss_item_sk", right_on="i_item_sk")
    df = df.merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    df = df.merge(
        customer_address, left_on="c_current_addr_sk", right_on="ca_address_sk"
    )
    df = df[(df["s_zip"] == df["ca_zip"]).fillna(False)]

    birth_country = df["c_birth_country"]
    upper_country = df["ca_country"].str.upper()
    # SQL drops the row when either side is NULL, since the comparison is
    # then unknown rather than true.
    different = (
        (birth_country != upper_country).fillna(False)
        & birth_country.notna()
        & upper_country.notna()
    )
    df = df[different]

    # netpaid is a DECIMAL in SQL, but no GPU groupby sums a decimal, so the
    # money is carried as float64 throughout.
    df = df.assign(netpaid=df["ss_net_paid"].astype("float64"))
    ssales = df.groupby(_SSALES_KEYS, as_index=False, dropna=False)[
        ["netpaid"]
    ].sum()

    threshold = 0.05 * ssales["netpaid"].mean()
    peach = ssales[(ssales["i_color"] == "peach").fillna(False)]
    out = peach.groupby(
        ["c_last_name", "c_first_name", "s_store_name"], as_index=False, dropna=False
    )[["netpaid"]].sum()
    out = out[out["netpaid"] > threshold]
    out = out.sort_values(
        ["c_last_name", "c_first_name", "s_store_name"], na_position="last"
    )
    return out[
        ["c_last_name", "c_first_name", "s_store_name", "netpaid"]
    ].rename(columns={"netpaid": "paid"}).reset_index(drop=True)
