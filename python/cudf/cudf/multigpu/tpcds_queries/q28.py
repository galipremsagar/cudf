# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 28.List price statistics for six quantity buckets of store sales."""

from __future__ import annotations

import pandas as pd

# (quantity low, quantity high, list price low, coupon low, wholesale cost low)
_BUCKETS = [
    (0, 5, 8, 459, 57),
    (6, 10, 90, 2323, 31),
    (11, 15, 142, 12214, 79),
    (16, 20, 135, 6071, 38),
    (21, 25, 122, 836, 17),
    (26, 30, 154, 7326, 7),
]


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_quantity",
            "ss_list_price",
            "ss_coupon_amt",
            "ss_wholesale_cost",
        ],
    )
    quantity = store_sales["ss_quantity"]
    list_price = store_sales["ss_list_price"].astype("float64")
    coupon = store_sales["ss_coupon_amt"].astype("float64")
    cost = store_sales["ss_wholesale_cost"].astype("float64")

    columns = {}
    for bucket, (q_lo, q_hi, lp_lo, coupon_lo, cost_lo) in enumerate(_BUCKETS, 1):
        # A NULL is not BETWEEN anything; NULL OR TRUE is still TRUE, so each
        # side of the OR is resolved before it is combined.
        selected = (
            ((quantity >= q_lo) & (quantity <= q_hi)).fillna(False)
            & (
                ((list_price >= lp_lo) & (list_price <= lp_lo + 10)).fillna(False)
                | ((coupon >= coupon_lo) & (coupon <= coupon_lo + 1000)).fillna(False)
                | ((cost >= cost_lo) & (cost <= cost_lo + 20)).fillna(False)
            )
        )
        prices = list_price[selected]
        columns[f"B{bucket}_LP"] = [prices.mean()]
        columns[f"B{bucket}_CNT"] = [prices.count()]
        columns[f"B{bucket}_CNTD"] = [prices.nunique()]

    return pd.DataFrame(columns)
