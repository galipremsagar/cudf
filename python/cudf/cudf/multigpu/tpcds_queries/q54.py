# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 54. Revenue segments of customers who bought maternity wear off-store."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    base = run_config.dataset_path
    suffix = run_config.suffix

    date_dim = pd.read_parquet(
        f"{base}/date_dim{suffix}",
        columns=["d_date_sk", "d_month_seq", "d_year", "d_moy"],
    )
    item = pd.read_parquet(
        f"{base}/item{suffix}", columns=["i_item_sk", "i_category", "i_class"]
    )
    customer = pd.read_parquet(
        f"{base}/customer{suffix}", columns=["c_customer_sk", "c_current_addr_sk"]
    )
    catalog_sales = pd.read_parquet(
        f"{base}/catalog_sales{suffix}",
        columns=["cs_sold_date_sk", "cs_bill_customer_sk", "cs_item_sk"],
    )
    web_sales = pd.read_parquet(
        f"{base}/web_sales{suffix}",
        columns=["ws_sold_date_sk", "ws_bill_customer_sk", "ws_item_sk"],
    )

    december = date_dim[(date_dim["d_year"] == 1998) & (date_dim["d_moy"] == 12)]
    # The scalar sub-query is "select distinct d_month_seq", which only has an
    # answer because December 1998 sits in exactly one month sequence; a
    # reduction picks that one value without materializing anything.
    month_seq = int(december["d_month_seq"].min())

    cs = catalog_sales.rename(
        columns={
            "cs_sold_date_sk": "sold_date_sk",
            "cs_bill_customer_sk": "customer_sk",
            "cs_item_sk": "item_sk",
        }
    )
    ws = web_sales.rename(
        columns={
            "ws_sold_date_sk": "sold_date_sk",
            "ws_bill_customer_sk": "customer_sk",
            "ws_item_sk": "item_sk",
        }
    )
    sales = pd.concat([cs, ws], ignore_index=True)

    maternity = item[
        (item["i_category"] == "Women") & (item["i_class"] == "maternity")
    ][["i_item_sk"]]

    my_customers = (
        sales.merge(maternity, left_on="item_sk", right_on="i_item_sk")
        .merge(december[["d_date_sk"]], left_on="sold_date_sk", right_on="d_date_sk")
        .merge(customer, left_on="customer_sk", right_on="c_customer_sk")
    )[["c_customer_sk", "c_current_addr_sk"]].drop_duplicates()

    customer_address = pd.read_parquet(
        f"{base}/customer_address{suffix}",
        columns=["ca_address_sk", "ca_county", "ca_state"],
    )
    store = pd.read_parquet(
        f"{base}/store{suffix}", columns=["s_county", "s_state"]
    )
    store_sales = pd.read_parquet(
        f"{base}/store_sales{suffix}",
        columns=["ss_sold_date_sk", "ss_customer_sk", "ss_ext_sales_price"],
    )

    window = date_dim[
        (date_dim["d_month_seq"] >= month_seq + 1)
        & (date_dim["d_month_seq"] <= month_seq + 3)
    ][["d_date_sk"]]

    located = my_customers.merge(
        customer_address, left_on="c_current_addr_sk", right_on="ca_address_sk"
    ).merge(
        store, left_on=["ca_county", "ca_state"], right_on=["s_county", "s_state"]
    )[["c_customer_sk"]]

    recent = store_sales.merge(
        window, left_on="ss_sold_date_sk", right_on="d_date_sk"
    )

    sold = located.merge(recent, left_on="c_customer_sk", right_on="ss_customer_sk")
    # libcudf cannot group-by-sum a fixed-point column, so the money moves to
    # float64 before the aggregation rather than after it.
    sold = sold.assign(_price=sold["ss_ext_sales_price"].astype("float64"))
    revenue = sold.groupby("c_customer_sk", as_index=False)["_price"].sum()

    # cast(round(revenue/50) as int): revenue is non-negative here, so adding a
    # half and truncating is the same rounding SQL does.
    revenue = revenue.assign(
        segment=(revenue["_price"] / 50 + 0.5).astype("int64")
    )

    counted = revenue.groupby("segment", as_index=False).size()
    counted = counted.rename(columns={"size": "num_customers"})
    counted = counted.assign(segment_base=counted["segment"] * 50)

    result = counted.sort_values(
        ["segment", "num_customers", "segment_base"], na_position="first"
    ).head(100)
    return result[["segment", "num_customers", "segment_base"]].reset_index(drop=True)
