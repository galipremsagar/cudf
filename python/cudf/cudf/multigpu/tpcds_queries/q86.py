# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 86. Web sales rolled up over category and class, ranked inside each parent level."""

from __future__ import annotations

import pandas as pd


def _rank_min_desc(frame, partition, value, name):
    """``rank() OVER (PARTITION BY partition ORDER BY value DESC)``.

    ``groupby().rank()`` has no distributed implementation, so this is built
    from a global sort instead: sorting by the partition keys and then by the
    value descending makes each partition a contiguous, ordered run of rows,
    so a row's position minus its partition's first position is its 0-based
    rank.  Taking the *minimum* such position over the rows sharing a value is
    exactly what ``method="min"`` means, and it is a group-by min.
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
    return frame.merge(firsts[partition + [value, name]], on=partition + [value])


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    web_sales = pd.read_parquet(
        f"{base}/web_sales{suffix}",
        columns=["ws_sold_date_sk", "ws_item_sk", "ws_net_paid"],
    )
    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}", columns=["d_date_sk", "d_month_seq"]
    )
    item = pd.read_parquet(
        f"{base}/item{suffix}", columns=["i_item_sk", "i_category", "i_class"]
    )

    dates = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1200 + 11)
    ][["d_date_sk"]]

    frame = web_sales.merge(dates, left_on="ws_sold_date_sk", right_on="d_date_sk")
    frame = frame.merge(item, left_on="ws_item_sk", right_on="i_item_sk")
    # libcudf has no group-by sum for decimals; TPC-DS money is far inside
    # float64's exactly representable range.
    frame = frame.assign(ws_net_paid=frame["ws_net_paid"].astype("float64"))

    def rolled_up(keys):
        # The count rides along in place of ``sum(min_count=1)``: a group whose
        # values are all NULL sums to NULL in SQL, not to zero.
        out = frame.groupby(keys, as_index=False, dropna=False).agg(
            total_sum=("ws_net_paid", "sum"), _kept=("ws_net_paid", "count")
        )
        out["total_sum"] = out["total_sum"].where(out["_kept"] > 0)
        return out

    # rollup(i_category, i_class): the two grouping levels plus the grand total
    level0 = rolled_up(["i_category", "i_class"])
    level0["lochierarchy"] = 0
    level0 = level0[["i_category", "i_class", "total_sum", "lochierarchy"]]

    level1 = rolled_up(["i_category"])
    # where(isna()) is how an all-null column of the right dtype is built.
    level1["i_class"] = level1["i_category"].where(level1["i_category"].isna())
    level1["lochierarchy"] = 1
    level1 = level1[["i_category", "i_class", "total_sum", "lochierarchy"]]

    level2 = level1.head(1)
    level2 = level2.assign(
        i_category=level2["i_category"].where(level2["i_category"].isna()),
        total_sum=frame["ws_net_paid"].sum(),
        lochierarchy=2,
    )

    rolled = pd.concat([level0, level1, level2], ignore_index=True)

    # case when grouping(i_class) = 0 then i_category end
    rolled["parent"] = rolled["i_category"].where(rolled["lochierarchy"] == 0)
    rolled = _rank_min_desc(
        rolled, ["lochierarchy", "parent"], "total_sum", "rank_within_parent"
    )

    rolled = rolled.sort_values(
        ["lochierarchy", "parent", "rank_within_parent"],
        ascending=[False, True, True],
        na_position="first",
    )
    result = rolled[
        ["total_sum", "i_category", "i_class", "lochierarchy", "rank_within_parent"]
    ]
    return result.head(100).reset_index(drop=True)
