# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 70.Store net profit rolled up by state and county over a year,
ranked within each level of the hierarchy."""

from __future__ import annotations

import pandas as pd


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
    return frame.merge(firsts[partition + [value, name]], on=partition + [value])


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=["ss_sold_date_sk", "ss_store_sk", "ss_net_profit"],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_month_seq"]
    )
    date_dim = date_dim[
        (date_dim["d_month_seq"] >= 1200) & (date_dim["d_month_seq"] <= 1211)
    ][["d_date_sk"]]
    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_state", "s_county"]
    )

    df = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    ).merge(store, left_on="ss_store_sk", right_on="s_store_sk")
    # libcudf has no group-by sum for decimals; TPC-DS money is far inside
    # float64's exactly representable range.
    df = df.assign(ss_net_profit=df["ss_net_profit"].astype("float64"))

    # The IN subquery ranks each state inside a partition of its own, so every
    # state that sold anything is ranked 1 and passes "ranking <= 5"; the set it
    # returns is exactly the set of non-NULL states already present here.
    #
    # What it is not is a no-op. "s_state IN (...)" is UNKNOWN, never true, when
    # s_state is NULL, so a store with no state is excluded from the outer query
    # however many such stores the subquery itself saw. One store carries a NULL
    # s_state from SF10 on; leaving its rows in adds a NULL state to the rollup,
    # which invents both a (NULL, NULL) detail group and a NULL state subtotal.
    # Membership in a set the row's own state defines reduces to "is not NULL".
    df = df[df["s_state"].notna()]

    def rolled_up(keys):
        # The count rides along in place of ``sum(min_count=1)``: a group whose
        # values are all NULL sums to NULL in SQL, not to zero.
        out = df.groupby(keys, as_index=False, dropna=False).agg(
            total_sum=("ss_net_profit", "sum"), _kept=("ss_net_profit", "count")
        )
        out["total_sum"] = out["total_sum"].where(out["_kept"] > 0)
        return out

    # rollup(s_state, s_county): both keys, then the state alone, then neither
    detail = rolled_up(["s_state", "s_county"])
    detail["lochierarchy"] = 0
    detail = detail[["total_sum", "s_state", "s_county", "lochierarchy"]]

    states = rolled_up(["s_state"])
    # where(isna()) is how an all-null column of the right dtype is built.
    states["s_county"] = states["s_state"].where(states["s_state"].isna())
    states["lochierarchy"] = 1
    states = states[["total_sum", "s_state", "s_county", "lochierarchy"]]

    grand = states.head(1)
    grand = grand.assign(
        s_state=grand["s_state"].where(grand["s_state"].isna()),
        total_sum=df["ss_net_profit"].sum(),
        lochierarchy=2,
    )

    rollup = pd.concat([detail, states, grand], ignore_index=True)

    # case when grouping(s_state) + grouping(s_county) = 0 then s_state end
    rollup["parent"] = rollup["s_state"].where(rollup["lochierarchy"] == 0)
    rollup = _rank_min_desc(
        rollup, ["lochierarchy", "parent"], "total_sum", "rank_within_parent"
    )

    columns = [
        "total_sum",
        "s_state",
        "s_county",
        "lochierarchy",
        "rank_within_parent",
    ]
    rollup = rollup.sort_values(
        ["lochierarchy", "parent", "rank_within_parent"],
        ascending=[False, True, True],
        na_position="last",
    ).head(100)
    return rollup[columns].reset_index(drop=True)
