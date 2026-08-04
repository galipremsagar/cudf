# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 2. Year-over-year ratio of web plus catalog sales for each day of the week."""

from __future__ import annotations

import pandas as pd

_DAYS = [
    ("Sunday", "sun_sales"),
    ("Monday", "mon_sales"),
    ("Tuesday", "tue_sales"),
    ("Wednesday", "wed_sales"),
    ("Thursday", "thu_sales"),
    ("Friday", "fri_sales"),
    ("Saturday", "sat_sales"),
]


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    web_sales = pd.read_parquet(
        f"{path}/web_sales{suffix}",
        columns=["ws_sold_date_sk", "ws_ext_sales_price"],
    )
    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=["cs_sold_date_sk", "cs_ext_sales_price"],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}",
        columns=["d_date_sk", "d_week_seq", "d_day_name", "d_year"],
    )

    web_sales = web_sales.rename(
        columns={"ws_sold_date_sk": "sold_date_sk", "ws_ext_sales_price": "sales_price"}
    )
    catalog_sales = catalog_sales.rename(
        columns={"cs_sold_date_sk": "sold_date_sk", "cs_ext_sales_price": "sales_price"}
    )
    wscs = pd.concat([web_sales, catalog_sales], ignore_index=True)
    wscs = wscs.assign(sales_price=wscs["sales_price"].astype("float64"))

    joined = wscs.merge(
        date_dim[["d_date_sk", "d_week_seq", "d_day_name"]],
        left_on="sold_date_sk",
        right_on="d_date_sk",
        how="inner",
    )

    # ``sum(CASE WHEN d_day_name = 'Sunday' THEN sales_price END)`` is the sum
    # over one (week, day) cell, and NULL when the cell is empty.  Summing per
    # (week, day) once and then spreading the days across columns with left
    # merges gives exactly that -- a missing cell becomes a missing row and so
    # a NaN -- without materialising seven masked copies of the fact table.
    per_cell = (
        joined.groupby(["d_week_seq", "d_day_name"], dropna=False)["sales_price"]
        .sum()
        .reset_index()
    )

    columns = [name for _, name in _DAYS]
    wswscs = per_cell[["d_week_seq"]].drop_duplicates()
    for day, name in _DAYS:
        cell = per_cell[per_cell["d_day_name"] == day][
            ["d_week_seq", "sales_price"]
        ].rename(columns={"sales_price": name})
        wswscs = wswscs.merge(cell, on="d_week_seq", how="left")

    first = date_dim[date_dim["d_year"] == 2001][["d_week_seq"]].merge(
        wswscs, on="d_week_seq", how="inner"
    )
    first = first.rename(
        columns={"d_week_seq": "d_week_seq1", **{c: f"{c}1" for c in columns}}
    )

    second = date_dim[date_dim["d_year"] == 2002][["d_week_seq"]].merge(
        wswscs, on="d_week_seq", how="inner"
    )
    second = second.rename(
        columns={"d_week_seq": "d_week_seq2", **{c: f"{c}2" for c in columns}}
    )
    second = second.assign(join_key=second["d_week_seq2"] - 53)

    merged = first.merge(
        second, left_on="d_week_seq1", right_on="join_key", how="inner"
    )

    ratios = {
        f"r{index}": (merged[f"{name}1"] / merged[f"{name}2"]).round(2)
        for index, (_, name) in enumerate(_DAYS, start=1)
    }
    out = merged.assign(**ratios)[["d_week_seq1", *ratios]]

    out = out.sort_values("d_week_seq1", na_position="first", kind="stable")
    return out.reset_index(drop=True)
