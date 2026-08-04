# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 32.Excess catalog discount amount for one manufacturer over a three month window."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_date"]
    )
    d_date = pd.to_datetime(date_dim["d_date"])
    date_dim = date_dim[
        (d_date >= pd.Timestamp("2000-01-27")) & (d_date <= pd.Timestamp("2000-04-26"))
    ][["d_date_sk"]]

    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_manufact_id"]
    )
    item = item[item["i_manufact_id"] == 977][["i_item_sk"]]

    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=["cs_sold_date_sk", "cs_item_sk", "cs_ext_discount_amt"],
    )
    # avg() is a double in SQL, so the threshold test is done in double; the
    # sum that is returned stays decimal, exactly as the SQL computes it.
    catalog_sales["discount"] = catalog_sales["cs_ext_discount_amt"].astype("float64")
    in_window = catalog_sales.merge(
        date_dim, left_on="cs_sold_date_sk", right_on="d_date_sk"
    )

    per_item = (
        in_window.groupby("cs_item_sk", as_index=False)["discount"]
        .mean()
        .rename(columns={"discount": "avg_discount"})
    )
    per_item["threshold"] = 1.3 * per_item["avg_discount"]

    candidates = in_window.merge(
        item, left_on="cs_item_sk", right_on="i_item_sk"
    ).merge(per_item[["cs_item_sk", "threshold"]], on="cs_item_sk")

    excess = candidates[candidates["discount"] > candidates["threshold"]][
        "cs_ext_discount_amt"
    ].sum()

    return pd.DataFrame({"excess discount amount": [excess]})
