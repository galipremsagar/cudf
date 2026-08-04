# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 22.Average inventory on hand rolled up over the item hierarchy."""

from __future__ import annotations

import pandas as pd

_KEYS = ["i_product_name", "i_brand", "i_class", "i_category"]


def _nulled(series):
    """A column of the same dtype as ``series`` holding only nulls."""
    return series.where(series.isna())


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    inventory = pd.read_parquet(
        f"{path}/inventory{suffix}",
        columns=["inv_date_sk", "inv_item_sk", "inv_quantity_on_hand"],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_month_seq"]
    )
    item = pd.read_parquet(f"{path}/item{suffix}", columns=["i_item_sk", *_KEYS])

    date_dim = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1200 + 11)
    ][["d_date_sk"]]

    df = inventory.merge(date_dim, left_on="inv_date_sk", right_on="d_date_sk")
    df = df.merge(item, left_on="inv_item_sk", right_on="i_item_sk")

    # Finest level of the rollup; the coarser levels are folded up from it,
    # which keeps sum/count exact and avoids rescanning the fact table.
    finest = df.groupby(_KEYS, as_index=False, dropna=False).agg(
        total=("inv_quantity_on_hand", "sum"),
        n=("inv_quantity_on_hand", "count"),
    )

    frames = []
    for depth in (4, 3, 2, 1):
        if depth == 4:
            level = finest
        else:
            level = finest.groupby(_KEYS[:depth], as_index=False, dropna=False)[
                ["total", "n"]
            ].sum()
            for column in _KEYS[depth:]:
                level[column] = _nulled(level[_KEYS[0]])
        frames.append(level[[*_KEYS, "total", "n"]])

    grand = finest.head(1).copy()
    for column in _KEYS:
        grand[column] = _nulled(grand[column])
    grand["total"] = finest["total"].sum()
    grand["n"] = finest["n"].sum()
    frames.append(grand[[*_KEYS, "total", "n"]])

    out = pd.concat(frames, ignore_index=True)
    out = out.assign(qoh=out["total"].astype("float64") / out["n"])
    out = out.sort_values(["qoh", *_KEYS], na_position="first").head(100)
    return out[[*_KEYS, "qoh"]].reset_index(drop=True)
