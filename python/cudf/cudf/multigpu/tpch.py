# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-H over a multi-GPU DataFrame.

The 22 queries are written once against a plain DataFrame API and run on three
interchangeable backends -- ``multigpu`` (chunked across every GPU),
``cudf`` (one GPU), and ``pandas`` (host) -- so results can be cross-checked
against each other rather than eyeballed.

    # generate data first:
    #   tpchgen-cli parquet -s 100 --output-dir=/path/tpch/sf100

    python -m cudf.multigpu.tpch --path /path/tpch/sf100
    python -m cudf.multigpu.tpch --path /path/tpch/sf1 --verify
    python -m cudf.multigpu.tpch --path /path/tpch/sf1 --queries 1,6,9

Every query is intentionally expressed the way a user would write it, not
rewritten to dodge missing functionality: a query that fails is a missing
multi-GPU API, which is the point of running these.
"""

from __future__ import annotations

import argparse
import time
import traceback
from typing import Any, Callable, Sequence

TABLES = (
    "lineitem",
    "orders",
    "customer",
    "part",
    "partsupp",
    "supplier",
    "nation",
    "region",
)

#: TPC-H stores money/quantities as DECIMAL. cuDF's decimal arithmetic does not
#: cover the mixed decimal/integer expressions these queries use, and every
#: published DataFrame TPC-H implementation converts to double, so we do too.
_DECIMAL_TO_FLOAT = True


# ----------------------------------------------------------------------
# backends
# ----------------------------------------------------------------------
def load_tables(
    path: str,
    backend: str = "multigpu",
    npartitions: int | None = None,
    tables: Sequence[str] = TABLES,
) -> dict[str, Any]:
    """Read the TPC-H tables with the requested backend."""
    import os

    frames: dict[str, Any] = {}
    for name in tables:
        source = os.path.join(path, f"{name}.parquet")
        if not os.path.exists(source):
            source = os.path.join(path, name)
        frames[name] = _read(source, backend, npartitions)
    return frames


def _read(source: str, backend: str, npartitions: int | None):
    if backend == "multigpu":
        import cudf.multigpu as mgpu

        frame = mgpu.read_parquet(source, npartitions=npartitions)
    elif backend == "cudf":
        import cudf

        frame = cudf.read_parquet(source)
    elif backend == "pandas":
        import pandas as pd

        frame = pd.read_parquet(source)
    else:
        raise ValueError(f"unknown backend {backend!r}")
    return _normalize(frame, backend)


def _normalize(frame, backend: str):
    """Cast DECIMAL columns to float64 (see ``_DECIMAL_TO_FLOAT``)."""
    if not _DECIMAL_TO_FLOAT:
        return frame
    decimal_columns = [
        name
        for name, dtype in frame.dtypes.items()
        if "decimal" in str(dtype).lower()
    ]
    for name in decimal_columns:
        frame[name] = frame[name].astype("float64")
    return frame


# ----------------------------------------------------------------------
# helpers shared by the queries
# ----------------------------------------------------------------------
def _date(frame, text: str):
    """A date literal comparable with the tables' datetime64[s] columns."""
    import numpy as np

    return np.datetime64(text, "s")


def _sort_head(frame, by, ascending=True, n: int | None = None):
    out = frame.sort_values(by, ascending=ascending)
    return out.head(n) if n is not None else out


# ----------------------------------------------------------------------
# the queries
# ----------------------------------------------------------------------
def q1(t):
    """Pricing summary report."""
    line = t["lineitem"]
    line = line[line["l_shipdate"] <= _date(line, "1998-09-02")]
    line = line.assign(
        disc_price=line["l_extendedprice"] * (1 - line["l_discount"]),
    )
    line = line.assign(
        charge=line["disc_price"] * (1 + line["l_tax"]),
    )
    grouped = line.groupby(["l_returnflag", "l_linestatus"], as_index=False).agg(
        {
            "l_quantity": ["sum", "mean"],
            "l_extendedprice": ["sum", "mean"],
            "disc_price": "sum",
            "charge": "sum",
            "l_discount": "mean",
            "l_orderkey": "count",
        }
    )
    return grouped.sort_values(["l_returnflag", "l_linestatus"])


def q2(t):
    """Minimum cost supplier."""
    part, partsupp = t["part"], t["partsupp"]
    supplier, nation, region = t["supplier"], t["nation"], t["region"]

    europe = region[region["r_name"] == "EUROPE"]
    nations = nation.merge(
        europe, left_on="n_regionkey", right_on="r_regionkey"
    )
    suppliers = supplier.merge(
        nations, left_on="s_nationkey", right_on="n_nationkey"
    )
    candidates = partsupp.merge(
        suppliers, left_on="ps_suppkey", right_on="s_suppkey"
    )

    parts = part[
        (part["p_size"] == 15) & (part["p_type"].str.endswith("BRASS"))
    ]
    joined = candidates.merge(parts, left_on="ps_partkey", right_on="p_partkey")

    cheapest = joined.groupby("p_partkey", as_index=False).agg(
        {"ps_supplycost": "min"}
    )
    cheapest = cheapest.rename(columns={"ps_supplycost": "min_supplycost"})
    result = joined.merge(cheapest, on="p_partkey")
    result = result[result["ps_supplycost"] == result["min_supplycost"]]
    result = result[
        [
            "s_acctbal",
            "s_name",
            "n_name",
            "p_partkey",
            "p_mfgr",
            "s_address",
            "s_phone",
            "s_comment",
        ]
    ]
    return _sort_head(
        result,
        ["s_acctbal", "n_name", "s_name", "p_partkey"],
        ascending=[False, True, True, True],
        n=100,
    )


def q3(t):
    """Shipping priority."""
    customer, orders, lineitem = t["customer"], t["orders"], t["lineitem"]
    cutoff = _date(orders, "1995-03-15")

    customer = customer[customer["c_mktsegment"] == "BUILDING"]
    orders = orders[orders["o_orderdate"] < cutoff]
    lineitem = lineitem[lineitem["l_shipdate"] > cutoff]

    joined = customer.merge(orders, left_on="c_custkey", right_on="o_custkey")
    joined = joined.merge(lineitem, left_on="o_orderkey", right_on="l_orderkey")
    joined = joined.assign(
        revenue=joined["l_extendedprice"] * (1 - joined["l_discount"])
    )
    grouped = joined.groupby(
        ["l_orderkey", "o_orderdate", "o_shippriority"], as_index=False
    ).agg({"revenue": "sum"})
    return _sort_head(
        grouped, ["revenue", "o_orderdate"], ascending=[False, True], n=10
    )


def q4(t):
    """Order priority checking."""
    orders, lineitem = t["orders"], t["lineitem"]
    low = _date(orders, "1993-07-01")
    high = _date(orders, "1993-10-01")

    orders = orders[
        (orders["o_orderdate"] >= low) & (orders["o_orderdate"] < high)
    ]
    late = lineitem[lineitem["l_commitdate"] < lineitem["l_receiptdate"]]
    late_keys = late[["l_orderkey"]].drop_duplicates()

    joined = orders.merge(late_keys, left_on="o_orderkey", right_on="l_orderkey")
    grouped = joined.groupby("o_orderpriority", as_index=False).agg(
        {"o_orderkey": "count"}
    )
    grouped = grouped.rename(columns={"o_orderkey": "order_count"})
    return grouped.sort_values("o_orderpriority")


def q5(t):
    """Local supplier volume."""
    low = _date(t["orders"], "1994-01-01")
    high = _date(t["orders"], "1995-01-01")

    region = t["region"][t["region"]["r_name"] == "ASIA"]
    nations = t["nation"].merge(
        region, left_on="n_regionkey", right_on="r_regionkey"
    )
    customer = t["customer"].merge(
        nations, left_on="c_nationkey", right_on="n_nationkey"
    )
    orders = t["orders"]
    orders = orders[
        (orders["o_orderdate"] >= low) & (orders["o_orderdate"] < high)
    ]
    joined = customer.merge(orders, left_on="c_custkey", right_on="o_custkey")
    joined = joined.merge(
        t["lineitem"], left_on="o_orderkey", right_on="l_orderkey"
    )
    joined = joined.merge(
        t["supplier"],
        left_on=["l_suppkey", "n_nationkey"],
        right_on=["s_suppkey", "s_nationkey"],
    )
    joined = joined.assign(
        revenue=joined["l_extendedprice"] * (1 - joined["l_discount"])
    )
    grouped = joined.groupby("n_name", as_index=False).agg({"revenue": "sum"})
    return grouped.sort_values("revenue", ascending=False)


def q6(t):
    """Forecasting revenue change."""
    line = t["lineitem"]
    low = _date(line, "1994-01-01")
    high = _date(line, "1995-01-01")
    selected = line[
        (line["l_shipdate"] >= low)
        & (line["l_shipdate"] < high)
        & (line["l_discount"] >= 0.05)
        & (line["l_discount"] <= 0.07)
        & (line["l_quantity"] < 24)
    ]
    revenue = (selected["l_extendedprice"] * selected["l_discount"]).sum()
    return _scalar_frame(t, "revenue", revenue)


def q7(t):
    """Volume shipping."""
    nation = t["nation"]
    pair = nation[nation["n_name"].isin(["FRANCE", "GERMANY"])]

    suppliers = t["supplier"].merge(
        pair.rename(columns={"n_name": "supp_nation", "n_nationkey": "s_nk"}),
        left_on="s_nationkey",
        right_on="s_nk",
    )
    customers = t["customer"].merge(
        pair.rename(columns={"n_name": "cust_nation", "n_nationkey": "c_nk"}),
        left_on="c_nationkey",
        right_on="c_nk",
    )

    line = t["lineitem"]
    low = _date(line, "1995-01-01")
    high = _date(line, "1996-12-31")
    line = line[(line["l_shipdate"] >= low) & (line["l_shipdate"] <= high)]

    joined = line.merge(suppliers, left_on="l_suppkey", right_on="s_suppkey")
    joined = joined.merge(
        t["orders"], left_on="l_orderkey", right_on="o_orderkey"
    )
    joined = joined.merge(customers, left_on="o_custkey", right_on="c_custkey")
    joined = joined[joined["supp_nation"] != joined["cust_nation"]]

    joined = joined.assign(
        l_year=joined["l_shipdate"].dt.year,
        volume=joined["l_extendedprice"] * (1 - joined["l_discount"]),
    )
    grouped = joined.groupby(
        ["supp_nation", "cust_nation", "l_year"], as_index=False
    ).agg({"volume": "sum"})
    return grouped.sort_values(["supp_nation", "cust_nation", "l_year"])


def q8(t):
    """National market share."""
    region = t["region"][t["region"]["r_name"] == "AMERICA"]
    nations = t["nation"].merge(
        region, left_on="n_regionkey", right_on="r_regionkey"
    )
    customer = t["customer"].merge(
        nations, left_on="c_nationkey", right_on="n_nationkey"
    )

    orders = t["orders"]
    low = _date(orders, "1995-01-01")
    high = _date(orders, "1996-12-31")
    orders = orders[
        (orders["o_orderdate"] >= low) & (orders["o_orderdate"] <= high)
    ]

    part = t["part"][t["part"]["p_type"] == "ECONOMY ANODIZED STEEL"]

    joined = customer.merge(orders, left_on="c_custkey", right_on="o_custkey")
    joined = joined.merge(
        t["lineitem"], left_on="o_orderkey", right_on="l_orderkey"
    )
    joined = joined.merge(part, left_on="l_partkey", right_on="p_partkey")
    joined = joined.merge(
        t["supplier"], left_on="l_suppkey", right_on="s_suppkey"
    )
    supp_nation = t["nation"].rename(
        columns={"n_name": "supp_nation", "n_nationkey": "supp_nationkey"}
    )
    joined = joined.merge(
        supp_nation, left_on="s_nationkey", right_on="supp_nationkey"
    )

    joined = joined.assign(
        o_year=joined["o_orderdate"].dt.year,
        volume=joined["l_extendedprice"] * (1 - joined["l_discount"]),
    )
    joined = joined.assign(
        brazil_volume=joined["volume"].where(
            joined["supp_nation"] == "BRAZIL", 0.0
        )
    )
    grouped = joined.groupby("o_year", as_index=False).agg(
        {"brazil_volume": "sum", "volume": "sum"}
    )
    grouped = grouped.assign(
        mkt_share=grouped["brazil_volume"] / grouped["volume"]
    )
    return grouped[["o_year", "mkt_share"]].sort_values("o_year")


def q9(t):
    """Product type profit measure."""
    part = t["part"][t["part"]["p_name"].str.contains("green")]
    joined = t["lineitem"].merge(
        part, left_on="l_partkey", right_on="p_partkey"
    )
    joined = joined.merge(
        t["supplier"], left_on="l_suppkey", right_on="s_suppkey"
    )
    joined = joined.merge(
        t["partsupp"],
        left_on=["l_suppkey", "l_partkey"],
        right_on=["ps_suppkey", "ps_partkey"],
    )
    joined = joined.merge(
        t["orders"], left_on="l_orderkey", right_on="o_orderkey"
    )
    joined = joined.merge(
        t["nation"], left_on="s_nationkey", right_on="n_nationkey"
    )
    joined = joined.assign(
        o_year=joined["o_orderdate"].dt.year,
        amount=joined["l_extendedprice"] * (1 - joined["l_discount"])
        - joined["ps_supplycost"] * joined["l_quantity"],
    )
    grouped = joined.groupby(["n_name", "o_year"], as_index=False).agg(
        {"amount": "sum"}
    )
    return grouped.sort_values(["n_name", "o_year"], ascending=[True, False])


def q10(t):
    """Returned item reporting."""
    orders = t["orders"]
    low = _date(orders, "1993-10-01")
    high = _date(orders, "1994-01-01")
    orders = orders[
        (orders["o_orderdate"] >= low) & (orders["o_orderdate"] < high)
    ]
    line = t["lineitem"][t["lineitem"]["l_returnflag"] == "R"]

    joined = t["customer"].merge(
        orders, left_on="c_custkey", right_on="o_custkey"
    )
    joined = joined.merge(line, left_on="o_orderkey", right_on="l_orderkey")
    joined = joined.merge(
        t["nation"], left_on="c_nationkey", right_on="n_nationkey"
    )
    joined = joined.assign(
        revenue=joined["l_extendedprice"] * (1 - joined["l_discount"])
    )
    grouped = joined.groupby(
        [
            "c_custkey",
            "c_name",
            "c_acctbal",
            "c_phone",
            "n_name",
            "c_address",
            "c_comment",
        ],
        as_index=False,
    ).agg({"revenue": "sum"})
    return _sort_head(grouped, "revenue", ascending=False, n=20)


def q11(t):
    """Important stock identification."""
    nation = t["nation"][t["nation"]["n_name"] == "GERMANY"]
    suppliers = t["supplier"].merge(
        nation, left_on="s_nationkey", right_on="n_nationkey"
    )
    joined = t["partsupp"].merge(
        suppliers, left_on="ps_suppkey", right_on="s_suppkey"
    )
    joined = joined.assign(
        value=joined["ps_supplycost"] * joined["ps_availqty"]
    )
    total = joined["value"].sum()

    grouped = joined.groupby("ps_partkey", as_index=False).agg({"value": "sum"})
    grouped = grouped[grouped["value"] > total * 0.0001]
    return grouped.sort_values("value", ascending=False)


def q12(t):
    """Shipping modes and order priority."""
    line = t["lineitem"]
    low = _date(line, "1994-01-01")
    high = _date(line, "1995-01-01")
    line = line[
        line["l_shipmode"].isin(["MAIL", "SHIP"])
        & (line["l_commitdate"] < line["l_receiptdate"])
        & (line["l_shipdate"] < line["l_commitdate"])
        & (line["l_receiptdate"] >= low)
        & (line["l_receiptdate"] < high)
    ]
    joined = line.merge(t["orders"], left_on="l_orderkey", right_on="o_orderkey")
    urgent = joined["o_orderpriority"].isin(["1-URGENT", "2-HIGH"])
    joined = joined.assign(
        high_line_count=urgent.astype("int64"),
        low_line_count=(~urgent).astype("int64"),
    )
    grouped = joined.groupby("l_shipmode", as_index=False).agg(
        {"high_line_count": "sum", "low_line_count": "sum"}
    )
    return grouped.sort_values("l_shipmode")


def q13(t):
    """Customer distribution."""
    orders = t["orders"]
    orders = orders[
        ~orders["o_comment"].str.contains("special.*requests", regex=True)
    ]
    joined = t["customer"].merge(
        orders, left_on="c_custkey", right_on="o_custkey", how="left"
    )
    per_customer = joined.groupby("c_custkey", as_index=False).agg(
        {"o_orderkey": "count"}
    )
    per_customer = per_customer.rename(columns={"o_orderkey": "c_count"})
    distribution = per_customer.groupby("c_count", as_index=False).agg(
        {"c_custkey": "count"}
    )
    distribution = distribution.rename(columns={"c_custkey": "custdist"})
    return distribution.sort_values(
        ["custdist", "c_count"], ascending=[False, False]
    )


def q14(t):
    """Promotion effect."""
    line = t["lineitem"]
    low = _date(line, "1995-09-01")
    high = _date(line, "1995-10-01")
    line = line[(line["l_shipdate"] >= low) & (line["l_shipdate"] < high)]
    joined = line.merge(t["part"], left_on="l_partkey", right_on="p_partkey")
    joined = joined.assign(
        revenue=joined["l_extendedprice"] * (1 - joined["l_discount"])
    )
    promo = joined["p_type"].str.startswith("PROMO")
    joined = joined.assign(
        promo_revenue=joined["revenue"].where(promo, 0.0)
    )
    numerator = joined["promo_revenue"].sum()
    denominator = joined["revenue"].sum()
    return _scalar_frame(t, "promo_revenue", 100.0 * numerator / denominator)


def q15(t):
    """Top supplier."""
    line = t["lineitem"]
    low = _date(line, "1996-01-01")
    high = _date(line, "1996-04-01")
    line = line[(line["l_shipdate"] >= low) & (line["l_shipdate"] < high)]
    line = line.assign(
        total_revenue=line["l_extendedprice"] * (1 - line["l_discount"])
    )
    revenue = line.groupby("l_suppkey", as_index=False).agg(
        {"total_revenue": "sum"}
    )
    best = revenue["total_revenue"].max()
    revenue = revenue[revenue["total_revenue"] == best]
    joined = t["supplier"].merge(
        revenue, left_on="s_suppkey", right_on="l_suppkey"
    )
    result = joined[
        ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]
    ]
    return result.sort_values("s_suppkey")


def q16(t):
    """Parts/supplier relationship."""
    part = t["part"]
    part = part[
        (part["p_brand"] != "Brand#45")
        & (~part["p_type"].str.startswith("MEDIUM POLISHED"))
        & (part["p_size"].isin([49, 14, 23, 45, 19, 3, 36, 9]))
    ]
    complaints = t["supplier"]
    complaints = complaints[
        complaints["s_comment"].str.contains("Customer.*Complaints", regex=True)
    ]
    bad_keys = complaints[["s_suppkey"]].drop_duplicates()

    joined = t["partsupp"].merge(part, left_on="ps_partkey", right_on="p_partkey")
    keep = ~joined["ps_suppkey"].isin(bad_keys["s_suppkey"])
    joined = joined[keep]

    distinct = joined[
        ["p_brand", "p_type", "p_size", "ps_suppkey"]
    ].drop_duplicates()
    grouped = distinct.groupby(
        ["p_brand", "p_type", "p_size"], as_index=False
    ).agg({"ps_suppkey": "count"})
    grouped = grouped.rename(columns={"ps_suppkey": "supplier_cnt"})
    return grouped.sort_values(
        ["supplier_cnt", "p_brand", "p_type", "p_size"],
        ascending=[False, True, True, True],
    )


def q17(t):
    """Small-quantity-order revenue."""
    part = t["part"]
    part = part[(part["p_brand"] == "Brand#23") & (part["p_container"] == "MED BOX")]
    joined = t["lineitem"].merge(
        part, left_on="l_partkey", right_on="p_partkey"
    )
    averages = joined.groupby("p_partkey", as_index=False).agg(
        {"l_quantity": "mean"}
    )
    averages = averages.rename(columns={"l_quantity": "avg_quantity"})
    joined = joined.merge(averages, on="p_partkey")
    joined = joined[joined["l_quantity"] < 0.2 * joined["avg_quantity"]]
    return _scalar_frame(t, "avg_yearly", joined["l_extendedprice"].sum() / 7.0)


def q18(t):
    """Large volume customer."""
    line = t["lineitem"]
    totals = line.groupby("l_orderkey", as_index=False).agg({"l_quantity": "sum"})
    totals = totals.rename(columns={"l_quantity": "order_quantity"})
    totals = totals[totals["order_quantity"] > 300]

    joined = t["orders"].merge(
        totals, left_on="o_orderkey", right_on="l_orderkey"
    )
    joined = joined.merge(
        t["customer"], left_on="o_custkey", right_on="c_custkey"
    )
    joined = joined.merge(line, left_on="o_orderkey", right_on="l_orderkey")
    grouped = joined.groupby(
        ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"],
        as_index=False,
    ).agg({"l_quantity": "sum"})
    return _sort_head(
        grouped, ["o_totalprice", "o_orderdate"], ascending=[False, True], n=100
    )


def q19(t):
    """Discounted revenue."""
    line, part = t["lineitem"], t["part"]
    joined = line.merge(part, left_on="l_partkey", right_on="p_partkey")
    joined = joined[joined["l_shipinstruct"] == "DELIVER IN PERSON"]
    joined = joined[joined["l_shipmode"].isin(["AIR", "AIR REG"])]

    first = (
        (joined["p_brand"] == "Brand#12")
        & joined["p_container"].isin(
            ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
        )
        & (joined["l_quantity"] >= 1)
        & (joined["l_quantity"] <= 11)
        & (joined["p_size"] >= 1)
        & (joined["p_size"] <= 5)
    )
    second = (
        (joined["p_brand"] == "Brand#23")
        & joined["p_container"].isin(
            ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
        )
        & (joined["l_quantity"] >= 10)
        & (joined["l_quantity"] <= 20)
        & (joined["p_size"] >= 1)
        & (joined["p_size"] <= 10)
    )
    third = (
        (joined["p_brand"] == "Brand#34")
        & joined["p_container"].isin(
            ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]
        )
        & (joined["l_quantity"] >= 20)
        & (joined["l_quantity"] <= 30)
        & (joined["p_size"] >= 1)
        & (joined["p_size"] <= 15)
    )
    joined = joined[first | second | third]
    revenue = (joined["l_extendedprice"] * (1 - joined["l_discount"])).sum()
    return _scalar_frame(t, "revenue", revenue)


def q20(t):
    """Potential part promotion."""
    line = t["lineitem"]
    low = _date(line, "1994-01-01")
    high = _date(line, "1995-01-01")
    line = line[(line["l_shipdate"] >= low) & (line["l_shipdate"] < high)]
    quantities = line.groupby(["l_partkey", "l_suppkey"], as_index=False).agg(
        {"l_quantity": "sum"}
    )
    quantities = quantities.assign(
        threshold=0.5 * quantities["l_quantity"]
    )

    parts = t["part"][t["part"]["p_name"].str.startswith("forest")]
    part_keys = parts[["p_partkey"]].drop_duplicates()

    partsupp = t["partsupp"].merge(
        part_keys, left_on="ps_partkey", right_on="p_partkey"
    )
    partsupp = partsupp.merge(
        quantities,
        left_on=["ps_partkey", "ps_suppkey"],
        right_on=["l_partkey", "l_suppkey"],
    )
    partsupp = partsupp[partsupp["ps_availqty"] > partsupp["threshold"]]
    good_suppliers = partsupp[["ps_suppkey"]].drop_duplicates()

    nation = t["nation"][t["nation"]["n_name"] == "CANADA"]
    suppliers = t["supplier"].merge(
        nation, left_on="s_nationkey", right_on="n_nationkey"
    )
    result = suppliers.merge(
        good_suppliers, left_on="s_suppkey", right_on="ps_suppkey"
    )
    return result[["s_name", "s_address"]].sort_values("s_name")


def q21(t):
    """Suppliers who kept orders waiting."""
    nation = t["nation"][t["nation"]["n_name"] == "SAUDI ARABIA"]
    suppliers = t["supplier"].merge(
        nation, left_on="s_nationkey", right_on="n_nationkey"
    )
    orders = t["orders"][t["orders"]["o_orderstatus"] == "F"]
    line = t["lineitem"]

    late = line[line["l_receiptdate"] > line["l_commitdate"]]

    # orders with more than one distinct supplier
    supplier_counts = line[["l_orderkey", "l_suppkey"]].drop_duplicates()
    supplier_counts = supplier_counts.groupby("l_orderkey", as_index=False).agg(
        {"l_suppkey": "count"}
    )
    supplier_counts = supplier_counts.rename(
        columns={"l_suppkey": "n_suppliers"}
    )
    multi = supplier_counts[supplier_counts["n_suppliers"] > 1]

    # orders where only one supplier was late
    late_counts = late[["l_orderkey", "l_suppkey"]].drop_duplicates()
    late_counts = late_counts.groupby("l_orderkey", as_index=False).agg(
        {"l_suppkey": "count"}
    )
    late_counts = late_counts.rename(columns={"l_suppkey": "n_late"})
    single_late = late_counts[late_counts["n_late"] == 1]

    candidates = late.merge(multi, on="l_orderkey")
    candidates = candidates.merge(single_late, on="l_orderkey")
    candidates = candidates.merge(
        orders, left_on="l_orderkey", right_on="o_orderkey"
    )
    candidates = candidates.merge(
        suppliers, left_on="l_suppkey", right_on="s_suppkey"
    )
    grouped = candidates.groupby("s_name", as_index=False).agg(
        {"l_orderkey": "count"}
    )
    grouped = grouped.rename(columns={"l_orderkey": "numwait"})
    return _sort_head(
        grouped, ["numwait", "s_name"], ascending=[False, True], n=100
    )


def q22(t):
    """Global sales opportunity."""
    codes = ["13", "31", "23", "29", "30", "18", "17"]
    customer = t["customer"]
    customer = customer.assign(cntrycode=customer["c_phone"].str.slice(0, 2))
    customer = customer[customer["cntrycode"].isin(codes)]

    positive = customer[customer["c_acctbal"] > 0.00]
    average = positive["c_acctbal"].mean()

    candidates = customer[customer["c_acctbal"] > average]
    ordered = t["orders"][["o_custkey"]].drop_duplicates()
    ordered = ordered.assign(has_orders=1)
    candidates = candidates.merge(
        ordered, left_on="c_custkey", right_on="o_custkey", how="left"
    )
    candidates = candidates[candidates["has_orders"].isna()]

    grouped = candidates.groupby("cntrycode", as_index=False).agg(
        {"c_custkey": "count", "c_acctbal": "sum"}
    )
    grouped = grouped.rename(
        columns={"c_custkey": "numcust", "c_acctbal": "totacctbal"}
    )
    return grouped.sort_values("cntrycode")


def _scalar_frame(t, name: str, value):
    """Wrap a scalar answer so every query returns a frame."""
    import pandas as pd

    return pd.DataFrame({name: [float(value)]})


QUERIES: dict[int, Callable] = {
    1: q1, 2: q2, 3: q3, 4: q4, 5: q5, 6: q6, 7: q7, 8: q8,
    9: q9, 10: q10, 11: q11, 12: q12, 13: q13, 14: q14, 15: q15,
    16: q16, 17: q17, 18: q18, 19: q19, 20: q20, 21: q21, 22: q22,
}


# ----------------------------------------------------------------------
# runner
# ----------------------------------------------------------------------
def to_host(result):
    """Normalize any backend's result to a pandas DataFrame."""
    import pandas as pd

    if isinstance(result, pd.DataFrame):
        return result.reset_index(drop=True)
    if hasattr(result, "to_pandas"):
        frame = result.to_pandas()
        if isinstance(frame, pd.Series):
            frame = frame.to_frame()
        return frame.reset_index(drop=True)
    return pd.DataFrame(result)


def run_query(number: int, tables: dict) -> tuple[Any, float]:
    start = time.perf_counter()
    result = QUERIES[number](tables)
    host = to_host(result)
    return host, time.perf_counter() - start


def compare(got, expected, tol: float = 1e-4) -> tuple[bool, str]:
    """Compare two query results allowing float noise and column order."""
    import numpy as np
    import pandas as pd

    if got.shape != expected.shape:
        return False, f"shape {got.shape} != {expected.shape}"
    got = got.sort_index(axis=1)
    expected = expected.sort_index(axis=1)
    if list(got.columns) != list(expected.columns):
        return False, f"columns {list(got.columns)} != {list(expected.columns)}"
    order = list(got.columns)
    got = got.sort_values(order).reset_index(drop=True)
    expected = expected.sort_values(order).reset_index(drop=True)
    for column in order:
        left, right = got[column], expected[column]
        if pd.api.types.is_numeric_dtype(right):
            if not np.allclose(
                left.to_numpy(dtype="float64"),
                right.to_numpy(dtype="float64"),
                rtol=tol,
                atol=tol,
                equal_nan=True,
            ):
                return False, f"column {column!r} differs"
        elif not left.astype(str).equals(right.astype(str)):
            return False, f"column {column!r} differs"
    return True, ""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True, help="directory of parquet tables")
    parser.add_argument("--backend", default="multigpu",
                        choices=["multigpu", "cudf", "pandas"])
    parser.add_argument("--queries", default="all",
                        help="comma separated query numbers, or 'all'")
    parser.add_argument("--npartitions", type=int, default=None)
    parser.add_argument("--devices", default=None)
    parser.add_argument("--verify", action="store_true",
                        help="also run on a reference backend and compare")
    parser.add_argument("--reference", default="pandas",
                        choices=["cudf", "pandas"])
    parser.add_argument("--pool-fraction", type=float, default=0.90)
    parser.add_argument("--initial-pool-fraction", type=float, default=0.05)
    parser.add_argument("--traceback", action="store_true")
    args = parser.parse_args()

    numbers = (
        sorted(QUERIES)
        if args.queries == "all"
        else [int(x) for x in args.queries.split(",")]
    )

    if args.backend == "multigpu":
        import cudf.multigpu as mgpu

        devices = (
            [int(x) for x in args.devices.split(",")] if args.devices else None
        )
        runtime = mgpu.init(
            devices=devices,
            max_pool_fraction=args.pool_fraction,
            initial_pool_fraction=args.initial_pool_fraction,
        )
        print(f"multi-GPU runtime: {runtime.n_devices} devices "
              f"{list(runtime.devices)}")

    print(f"loading {args.path} with backend={args.backend} ...")
    start = time.perf_counter()
    tables = load_tables(args.path, args.backend, args.npartitions)
    print(f"loaded in {time.perf_counter() - start:.2f}s")
    if args.backend == "multigpu":
        usage: dict[int, int] = {}
        for frame in tables.values():
            for device, nbytes in frame.memory_usage_per_device().items():
                usage[device] = usage.get(device, 0) + nbytes
        total = sum(usage.values()) / (1 << 30)
        print(f"  resident: {total:.2f} GiB over {len(usage)} GPUs -> "
              + ", ".join(f"GPU{d}={v / (1 << 30):.2f}G"
                          for d, v in sorted(usage.items())))

    reference_tables = None
    if args.verify:
        print(f"loading reference backend={args.reference} ...")
        reference_tables = load_tables(args.path, args.reference, None)

    passed = failed = errored = 0
    timings: dict[int, float] = {}
    print(f"\n{'query':>6}  {'status':<8}{'time':>9}  detail")
    print("-" * 72)
    for number in numbers:
        try:
            result, elapsed = run_query(number, tables)
            timings[number] = elapsed
        except Exception as exc:
            errored += 1
            print(f"{number:>6}  {'ERROR':<8}{'-':>9}  "
                  f"{type(exc).__name__}: {str(exc)[:90]}")
            if args.traceback:
                traceback.print_exc()
            continue

        if reference_tables is None:
            passed += 1
            print(f"{number:>6}  {'ok':<8}{elapsed:>8.2f}s  {len(result)} rows")
            continue

        try:
            expected, _ = run_query(number, reference_tables)
        except Exception as exc:
            print(f"{number:>6}  {'noref':<8}{elapsed:>8.2f}s  "
                  f"reference failed: {type(exc).__name__}: {str(exc)[:60]}")
            continue
        same, reason = compare(result, expected)
        if same:
            passed += 1
            print(f"{number:>6}  {'PASS':<8}{elapsed:>8.2f}s  {len(result)} rows")
        else:
            failed += 1
            print(f"{number:>6}  {'FAIL':<8}{elapsed:>8.2f}s  {reason}")

    print("-" * 72)
    total_time = sum(timings.values())
    print(f"{passed} ok, {failed} wrong, {errored} errored; "
          f"total query time {total_time:.2f}s")


if __name__ == "__main__":
    main()
