# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 36.Gross margin by category and class in Tennessee stores, rolled up and ranked."""

from __future__ import annotations

import pandas as pd

#: Stands in for a NULL partition key. A window puts every NULL in one
#: partition, but a NULL join key matches nothing, so the self-join that
#: computes the rank has to see a real value there.
_NULL_KEY = "\x00mg_null"


def _rank_min(frame, partition, value, out):
    """``rank() OVER (PARTITION BY partition ORDER BY value)``, as a join.

    A row's rank under ``method='min'`` is one more than the number of
    strictly smaller values in its partition, and a self-join on the partition
    key counts those directly -- no groupby ``.rank()`` needed.

    ``out`` identifies a row uniquely, so aggregating the pairs back by it
    rebuilds exactly the rows of ``frame``.
    """
    others = frame[partition + [value]].rename(columns={value: "__mg_other"})
    pairs = frame.merge(others, on=partition)
    pairs = pairs.assign(
        __mg_less=(pairs["__mg_other"] < pairs[value]).astype("int64")
    )
    counted = pairs.groupby(out, as_index=False, dropna=False).agg(
        **{value: (value, "max"), "__mg_less": ("__mg_less", "sum")}
    )
    return counted.assign(rank_within_parent=counted["__mg_less"] + 1)


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    date_dim = date_dim[date_dim["d_year"] == 2001][["d_date_sk"]]

    store = pd.read_parquet(f"{path}/store{suffix}", columns=["s_store_sk", "s_state"])
    store = store[store["s_state"] == "TN"][["s_store_sk"]]

    item = pd.read_parquet(
        f"{path}/item{suffix}", columns=["i_item_sk", "i_category", "i_class"]
    )

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_sold_date_sk",
            "ss_item_sk",
            "ss_store_sk",
            "ss_net_profit",
            "ss_ext_sales_price",
        ],
    )
    # Both money columns are DECIMALs, which no GPU groupby can sum.
    store_sales = store_sales.assign(
        ss_net_profit=store_sales["ss_net_profit"].astype("float64"),
        ss_ext_sales_price=store_sales["ss_ext_sales_price"].astype("float64"),
    )

    joined = store_sales.merge(
        date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk"
    )
    joined = joined.merge(item, left_on="ss_item_sk", right_on="i_item_sk")
    joined = joined.merge(store, left_on="ss_store_sk", right_on="s_store_sk")

    results = (
        joined.groupby(["i_category", "i_class"], dropna=False)
        .agg(
            ss_net_profit=("ss_net_profit", "sum"),
            ss_ext_sales_price=("ss_ext_sales_price", "sum"),
        )
        .reset_index()
    )
    results = results.assign(
        gross_margin=(results["ss_net_profit"] * 1.0000)
        / results["ss_ext_sales_price"],
        cat_key=results["i_category"].fillna(_NULL_KEY),
        one=1,
    )

    # lochierarchy 0: one row per class, ranked inside its own category.
    level0 = _rank_min(
        results[["cat_key", "i_category", "i_class", "gross_margin"]],
        ["cat_key"],
        "gross_margin",
        ["i_category", "i_class"],
    )
    level0 = level0.assign(lochierarchy=0, sort_key=level0["i_category"])

    # lochierarchy 1: one row per category. ``t_class`` is 1 there, so the CASE
    # in the PARTITION BY is NULL on every row and they form one partition.
    by_category = results.groupby("i_category", as_index=False, dropna=False).agg(
        ss_net_profit=("ss_net_profit", "sum"),
        ss_ext_sales_price=("ss_ext_sales_price", "sum"),
    )
    by_category = by_category.assign(
        gross_margin=(by_category["ss_net_profit"] * 1.0000)
        / by_category["ss_ext_sales_price"],
        one=1,
    )
    level1 = _rank_min(
        by_category[["one", "i_category", "gross_margin"]],
        ["one"],
        "gross_margin",
        ["i_category"],
    )
    level1 = level1.assign(lochierarchy=1, sort_key="")

    # lochierarchy 2: the grand total, alone in its partition and so rank 1.
    total = results.groupby("one", as_index=False).agg(
        ss_net_profit=("ss_net_profit", "sum"),
        ss_ext_sales_price=("ss_ext_sales_price", "sum"),
    )
    level2 = total.assign(
        gross_margin=(total["ss_net_profit"] * 1.0000) / total["ss_ext_sales_price"],
        lochierarchy=2,
        rank_within_parent=1,
        sort_key="",
    )

    tail = ["gross_margin", "lochierarchy", "rank_within_parent", "sort_key"]
    rollup = pd.concat(
        [
            level0[["i_category", "i_class"] + tail],
            level1[["i_category"] + tail],
            level2[tail],
        ],
        ignore_index=True,
    )
    # ORDER BY lochierarchy DESC, CASE WHEN lochierarchy = 0 THEN i_category
    # END, rank_within_parent -- the CASE is ``sort_key``, held constant on the
    # two rolled-up levels so that the rank alone orders them.
    rollup = rollup.sort_values(
        ["lochierarchy", "sort_key", "rank_within_parent"],
        ascending=[False, True, True],
        na_position="first",
    ).head(100)

    result = rollup[
        ["gross_margin", "i_category", "i_class", "lochierarchy", "rank_within_parent"]
    ]
    return result.reset_index(drop=True)
