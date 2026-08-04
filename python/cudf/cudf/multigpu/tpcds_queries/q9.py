# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 9. Five store-sales quantity buckets, each reported as discount or net paid."""

from __future__ import annotations

import pandas as pd

_BUCKETS = [
    (1, 20, 74129),
    (21, 40, 122840),
    (41, 60, 56580),
    (61, 80, 10097),
    (81, 100, 165306),
]


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=["ss_quantity", "ss_ext_discount_amt", "ss_net_paid"],
    )
    reason = pd.read_parquet(f"{path}/reason{suffix}", columns=["r_reason_sk"])
    rows = len(reason[reason["r_reason_sk"] == 1])

    quantity = store_sales["ss_quantity"]
    values = {}
    for index, (low, high, threshold) in enumerate(_BUCKETS, start=1):
        bucket = store_sales[(quantity >= low) & (quantity <= high)]
        if len(bucket) > threshold:
            measure = bucket["ss_ext_discount_amt"]
        else:
            measure = bucket["ss_net_paid"]
        values[f"bucket{index}"] = float(measure.astype("float64").mean())

    return pd.DataFrame(values, index=pd.RangeIndex(rows))
