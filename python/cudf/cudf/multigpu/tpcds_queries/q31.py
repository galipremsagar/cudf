# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 31.Counties where web sales grew faster than store sales quarter over quarter in 2000."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_qoy", "d_year"]
    )
    date_dim = date_dim[
        (date_dim["d_year"] == 2000) & (date_dim["d_qoy"].isin([1, 2, 3]))
    ]

    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}", columns=["ca_address_sk", "ca_county"]
    )

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=["ss_sold_date_sk", "ss_addr_sk", "ss_ext_sales_price"],
    )
    store_sales["ss_ext_sales_price"] = store_sales["ss_ext_sales_price"].astype(
        "float64"
    )
    ss = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    ).merge(customer_address, left_on="ss_addr_sk", right_on="ca_address_sk")
    ss = (
        ss.groupby(["ca_county", "d_qoy"], as_index=False)["ss_ext_sales_price"]
        .sum()
        .rename(columns={"ss_ext_sales_price": "store_sales"})
    )

    web_sales = pd.read_parquet(
        f"{path}/web_sales{suffix}",
        columns=["ws_sold_date_sk", "ws_bill_addr_sk", "ws_ext_sales_price"],
    )
    web_sales["ws_ext_sales_price"] = web_sales["ws_ext_sales_price"].astype("float64")
    ws = web_sales.merge(
        date_dim, left_on="ws_sold_date_sk", right_on="d_date_sk"
    ).merge(customer_address, left_on="ws_bill_addr_sk", right_on="ca_address_sk")
    ws = (
        ws.groupby(["ca_county", "d_qoy"], as_index=False)["ws_ext_sales_price"]
        .sum()
        .rename(columns={"ws_ext_sales_price": "web_sales"})
    )

    def quarter(frame, value, quarter_number, name):
        part = frame[frame["d_qoy"] == quarter_number][["ca_county", value]]
        return part.rename(columns={value: name})

    joined = quarter(ss, "store_sales", 1, "ss1")
    for source, value, quarter_number, name in (
        (ss, "store_sales", 2, "ss2"),
        (ss, "store_sales", 3, "ss3"),
        (ws, "web_sales", 1, "ws1"),
        (ws, "web_sales", 2, "ws2"),
        (ws, "web_sales", 3, "ws3"),
    ):
        joined = joined.merge(
            quarter(source, value, quarter_number, name), on="ca_county"
        )

    # The four ratios become columns of the joined frame itself, so that every
    # later expression reads columns of one frame instead of being grafted onto
    # a narrower one.
    joined = joined.assign(
        d_year=2000,
        web_q1_q2_increase=(joined["ws2"] * 1.0000) / joined["ws1"],
        store_q1_q2_increase=(joined["ss2"] * 1.0000) / joined["ss1"],
        web_q2_q3_increase=(joined["ws3"] * 1.0000) / joined["ws2"],
        store_q2_q3_increase=(joined["ss3"] * 1.0000) / joined["ss2"],
    )

    # ``CASE WHEN x > 0 THEN ratio ELSE NULL END`` on either side of a ``>``
    # makes the comparison unknown when the denominator is not positive, which
    # drops the row -- so the four positivity tests are the CASEs.
    mask = (
        (joined["ws1"] > 0)
        & (joined["ss1"] > 0)
        & (joined["ws2"] > 0)
        & (joined["ss2"] > 0)
        & (joined["web_q1_q2_increase"] > joined["store_q1_q2_increase"])
        & (joined["web_q2_q3_increase"] > joined["store_q2_q3_increase"])
    )
    result = joined[mask]

    result = result[
        [
            "ca_county",
            "d_year",
            "web_q1_q2_increase",
            "store_q1_q2_increase",
            "web_q2_q3_increase",
            "store_q2_q3_increase",
        ]
    ]
    return result.sort_values("ca_county").reset_index(drop=True)
