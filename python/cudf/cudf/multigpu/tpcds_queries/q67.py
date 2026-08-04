# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 67.The hundred best-selling rollup combinations of item, date
and store within each category over a year."""

from __future__ import annotations

import pandas as pd

KEYS = [
    "i_category",
    "i_class",
    "i_brand",
    "i_product_name",
    "d_year",
    "d_qoy",
    "d_moy",
    "s_store_id",
]


def _rank_min_desc(frame, partition, value, name):
    """``rank() OVER (PARTITION BY partition ORDER BY value DESC)``.

    ``groupby().rank()`` has no distributed implementation, so this is built
    from a global sort instead: sorting by the partition keys and then by the
    value descending makes each partition a contiguous, ordered run of rows,
    so a row's position minus its partition's first position is its 0-based
    rank.  Taking the *minimum* such position over the rows sharing a value is
    exactly what ``method="min"`` means, and that is a group-by min.
    """
    ordered = frame[partition + [value]].sort_values(
        partition + [value],
        ascending=[True] * len(partition) + [False],
        na_position="first",
    )
    ordered = ordered.reset_index(drop=True).reset_index()
    ordered = ordered.rename(columns={"index": "_row"})

    starts = (
        ordered.groupby(partition, as_index=False, dropna=False)["_row"]
        .min()
        .rename(columns={"_row": "_start"})
    )
    firsts = ordered.groupby(
        partition + [value], as_index=False, dropna=False
    )["_row"].min()
    firsts = firsts.merge(starts, on=partition)
    firsts[name] = firsts["_row"] - firsts["_start"] + 1
    return firsts[partition + [value, name]]


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_item_sk",
            "ss_store_sk",
            "ss_sales_price",
            "ss_quantity",
        ],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}",
        columns=["d_date_sk", "d_month_seq", "d_year", "d_qoy", "d_moy"],
    )
    date_dim = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1211)
    ][["d_date_sk", "d_year", "d_qoy", "d_moy"]]
    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_store_id"]
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}",
        columns=[
            "i_item_sk",
            "i_category",
            "i_class",
            "i_brand",
            "i_product_name",
        ],
    )

    df = (
        store_sales.merge(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
        .merge(item, left_on="ss_item_sk", right_on="i_item_sk")
        .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    )

    # coalesce(ss_sales_price*ss_quantity, 0). The price is DECIMAL in the
    # parquet and libcudf has no group-by sum for decimals, so it becomes
    # float64 -- TPC-DS money is far inside float64's exact range.
    price = df["ss_sales_price"].astype("float64")
    quantity = df["ss_quantity"].astype("float64")
    known = price.notna() & quantity.notna()
    df = df.assign(
        sumsales=(price.fillna(0.0) * quantity.fillna(0.0)).where(known, 0.0)
    )

    # GROUP BY rollup(...): the full key, then one column dropped at a time.
    # A level is built by nulling out the dropped key rather than dropping the
    # column, so every level keeps the same schema and the same null-carrying
    # dtypes -- which is what lets them be concatenated and grouped again.
    finest = df.groupby(KEYS, as_index=False, dropna=False)["sumsales"].sum()
    levels = [finest]
    current = finest
    for key in reversed(KEYS):
        # where(isna()) is how an all-null column of the right dtype is built.
        current = current.assign(**{key: current[key].where(current[key].isna())})
        current = current.groupby(KEYS, as_index=False, dropna=False)[
            "sumsales"
        ].sum()
        levels.append(current)
    rollup = pd.concat(levels, ignore_index=True)

    ranks = _rank_min_desc(rollup, ["i_category"], "sumsales", "rk")
    # Only the top hundred per category survive, so the join back carries a few
    # hundred rows instead of the whole rollup.
    ranks = ranks[ranks["rk"] <= 100]
    rollup = rollup.merge(ranks, on=["i_category", "sumsales"])

    rollup = rollup.sort_values(
        KEYS + ["sumsales", "rk"], na_position="first"
    ).head(100)
    return rollup[KEYS + ["sumsales", "rk"]].reset_index(drop=True)
