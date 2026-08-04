# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 64.Items in selected colours whose store/customer-address pair
sold in two consecutive years, comparing the two years' totals."""

from __future__ import annotations

import pandas as pd

GROUP_KEYS = [
    "i_product_name",
    "i_item_sk",
    "s_store_name",
    "s_zip",
    "b_street_number",
    "b_street_name",
    "b_city",
    "b_zip",
    "c_street_number",
    "c_street_name",
    "c_city",
    "c_zip",
    "syear",
    "fsyear",
    "s2year",
]


def query(run_config):
    path = run_config.dataset_path
    suffix = run_config.suffix

    catalog_sales = pd.read_parquet(
        f"{path}/catalog_sales{suffix}",
        columns=["cs_item_sk", "cs_order_number", "cs_ext_list_price"],
    )
    catalog_returns = pd.read_parquet(
        f"{path}/catalog_returns{suffix}",
        columns=[
            "cr_item_sk",
            "cr_order_number",
            "cr_refunded_cash",
            "cr_reversed_charge",
            "cr_store_credit",
        ],
    )
    returned = catalog_sales.merge(
        catalog_returns,
        left_on=["cs_item_sk", "cs_order_number"],
        right_on=["cr_item_sk", "cr_order_number"],
    )
    # The money columns move to float64 here: libcudf has no group-by sum for
    # fixed-point columns, and TPC-DS amounts are far inside float64's exactly
    # representable range.  cr_refunded_cash+cr_reversed_charge+cr_store_credit
    # is NULL, and so is skipped by SUM, when any part of it is NULL -- which is
    # exactly what NaN propagation through the addition gives.
    refund = (
        returned["cr_refunded_cash"].astype("float64")
        + returned["cr_reversed_charge"].astype("float64")
        + returned["cr_store_credit"].astype("float64")
    )
    returned = returned.assign(
        _sale=returned["cs_ext_list_price"].astype("float64"),
        _refund=refund,
        _refund_known=refund.notna(),
    )
    cs_ui = (
        returned.groupby("cs_item_sk", dropna=False)
        .agg(
            sale=("_sale", "sum"),
            refund=("_refund", "sum"),
            known=("_refund_known", "sum"),
        )
        .reset_index()
    )
    # A SUM over an all-NULL group is NULL, and the HAVING comparison against a
    # NULL is unknown, so those items drop out.
    cs_ui = cs_ui[(cs_ui["known"] > 0) & (cs_ui["sale"] > 2 * cs_ui["refund"])][
        ["cs_item_sk"]
    ]

    store_sales = pd.read_parquet(
        f"{path}/store_sales{suffix}",
        columns=[
            "ss_store_sk",
            "ss_sold_date_sk",
            "ss_customer_sk",
            "ss_cdemo_sk",
            "ss_hdemo_sk",
            "ss_addr_sk",
            "ss_item_sk",
            "ss_ticket_number",
            "ss_promo_sk",
            "ss_wholesale_cost",
            "ss_list_price",
            "ss_coupon_amt",
        ],
    )
    # The three money columns are aggregated later; float64 is what libcudf can
    # group-by-sum, and the cast is cheaper here than after the joins.
    store_sales = store_sales.assign(
        ss_wholesale_cost=store_sales["ss_wholesale_cost"].astype("float64"),
        ss_list_price=store_sales["ss_list_price"].astype("float64"),
        ss_coupon_amt=store_sales["ss_coupon_amt"].astype("float64"),
    )
    store_returns = pd.read_parquet(
        f"{path}/store_returns{suffix}",
        columns=["sr_item_sk", "sr_ticket_number"],
    )
    date_dim = pd.read_parquet(
        f"{path}/date_dim{suffix}", columns=["d_date_sk", "d_year"]
    )
    store = pd.read_parquet(
        f"{path}/store{suffix}", columns=["s_store_sk", "s_store_name", "s_zip"]
    )
    customer = pd.read_parquet(
        f"{path}/customer{suffix}",
        columns=[
            "c_customer_sk",
            "c_current_cdemo_sk",
            "c_current_hdemo_sk",
            "c_current_addr_sk",
            "c_first_sales_date_sk",
            "c_first_shipto_date_sk",
        ],
    )
    customer_demographics = pd.read_parquet(
        f"{path}/customer_demographics{suffix}",
        columns=["cd_demo_sk", "cd_marital_status"],
    )
    promotion = pd.read_parquet(f"{path}/promotion{suffix}", columns=["p_promo_sk"])
    household_demographics = pd.read_parquet(
        f"{path}/household_demographics{suffix}",
        columns=["hd_demo_sk", "hd_income_band_sk"],
    )
    income_band = pd.read_parquet(
        f"{path}/income_band{suffix}", columns=["ib_income_band_sk"]
    )
    customer_address = pd.read_parquet(
        f"{path}/customer_address{suffix}",
        columns=[
            "ca_address_sk",
            "ca_street_number",
            "ca_street_name",
            "ca_city",
            "ca_zip",
        ],
    )
    item = pd.read_parquet(
        f"{path}/item{suffix}",
        columns=["i_item_sk", "i_product_name", "i_color", "i_current_price"],
    )

    # The two BETWEENs intersect to [65, 74]; the bounds are whole dollars, so
    # comparing in float64 keeps the same rows a decimal comparison would.
    price = item["i_current_price"].astype("float64")
    item = item[
        item["i_color"].isin(
            ["purple", "burlywood", "indian", "spring", "floral", "medium"]
        )
        & (price >= 65)
        & (price <= 74)
    ][["i_item_sk", "i_product_name"]]

    banded = household_demographics.merge(
        income_band, left_on="hd_income_band_sk", right_on="ib_income_band_sk"
    )[["hd_demo_sk"]]

    address_b = customer_address.rename(
        columns={
            "ca_address_sk": "b_address_sk",
            "ca_street_number": "b_street_number",
            "ca_street_name": "b_street_name",
            "ca_city": "b_city",
            "ca_zip": "b_zip",
        }
    )
    address_c = customer_address.rename(
        columns={
            "ca_address_sk": "c_address_sk",
            "ca_street_number": "c_street_number",
            "ca_street_name": "c_street_name",
            "ca_city": "c_city",
            "ca_zip": "c_zip",
        }
    )

    customers = (
        customer.merge(
            customer_demographics.rename(
                columns={
                    "cd_demo_sk": "cd2_demo_sk",
                    "cd_marital_status": "cd2_marital_status",
                }
            ),
            left_on="c_current_cdemo_sk",
            right_on="cd2_demo_sk",
        )
        .merge(
            banded.rename(columns={"hd_demo_sk": "hd2_demo_sk"}),
            left_on="c_current_hdemo_sk",
            right_on="hd2_demo_sk",
        )
        .merge(address_c, left_on="c_current_addr_sk", right_on="c_address_sk")
        .merge(
            date_dim.rename(columns={"d_date_sk": "d2_date_sk", "d_year": "fsyear"}),
            left_on="c_first_sales_date_sk",
            right_on="d2_date_sk",
        )
        .merge(
            date_dim.rename(columns={"d_date_sk": "d3_date_sk", "d_year": "s2year"}),
            left_on="c_first_shipto_date_sk",
            right_on="d3_date_sk",
        )
    )

    df = (
        store_sales.merge(
            store_returns,
            left_on=["ss_item_sk", "ss_ticket_number"],
            right_on=["sr_item_sk", "sr_ticket_number"],
        )
        .merge(cs_ui, left_on="ss_item_sk", right_on="cs_item_sk")
        .merge(
            date_dim.rename(columns={"d_date_sk": "d1_date_sk", "d_year": "syear"}),
            left_on="ss_sold_date_sk",
            right_on="d1_date_sk",
        )
        .merge(store, left_on="ss_store_sk", right_on="s_store_sk")
        .merge(
            customer_demographics.rename(
                columns={
                    "cd_demo_sk": "cd1_demo_sk",
                    "cd_marital_status": "cd1_marital_status",
                }
            ),
            left_on="ss_cdemo_sk",
            right_on="cd1_demo_sk",
        )
        .merge(
            banded.rename(columns={"hd_demo_sk": "hd1_demo_sk"}),
            left_on="ss_hdemo_sk",
            right_on="hd1_demo_sk",
        )
        .merge(address_b, left_on="ss_addr_sk", right_on="b_address_sk")
        .merge(promotion, left_on="ss_promo_sk", right_on="p_promo_sk")
        .merge(item, left_on="ss_item_sk", right_on="i_item_sk")
        .merge(customers, left_on="ss_customer_sk", right_on="c_customer_sk")
    )

    # <> is unknown, hence false, when either marital status is NULL.
    df = df[
        df["cd1_marital_status"].notna()
        & df["cd2_marital_status"].notna()
        & (df["cd1_marital_status"] != df["cd2_marital_status"])
    ]

    cross_sales = (
        df.groupby(GROUP_KEYS, dropna=False)
        .agg(
            cnt=("ss_item_sk", "size"),
            s1=("ss_wholesale_cost", "sum"),
            s2=("ss_list_price", "sum"),
            s3=("ss_coupon_amt", "sum"),
        )
        .reset_index()
    )

    left = cross_sales[cross_sales["syear"] == 1999]
    right = cross_sales[cross_sales["syear"] == 2000][
        ["i_item_sk", "s_store_name", "s_zip", "cnt", "s1", "s2", "s3", "syear"]
    ].rename(
        columns={
            "cnt": "cnt2",
            "s1": "s12",
            "s2": "s22",
            "s3": "s32",
            "syear": "syear2",
        }
    )

    joined = left.merge(right, on=["i_item_sk", "s_store_name", "s_zip"])
    joined = joined[joined["cnt2"] <= joined["cnt"]]

    joined = joined.sort_values(
        ["i_product_name", "s_store_name", "cnt2", "s1", "s12"],
        na_position="last",
    )

    result = joined[
        [
            "i_product_name",
            "s_store_name",
            "s_zip",
            "b_street_number",
            "b_street_name",
            "b_city",
            "b_zip",
            "c_street_number",
            "c_street_name",
            "c_city",
            "c_zip",
            "syear",
            "cnt",
            "s1",
            "s2",
            "s3",
            "s12",
            "s22",
            "s32",
            "syear2",
            "cnt2",
        ]
    ]
    return result.reset_index(drop=True)
