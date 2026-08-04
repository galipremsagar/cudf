# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS query 41.Product names from a manufacturer id range whose manufacturer also makes an item matching one of several colour/unit/size profiles."""

from __future__ import annotations

import pandas as pd


def query(run_config):
    item = pd.read_parquet(
        f"{run_config.dataset_path}/item{run_config.suffix}",
        columns=[
            "i_manufact_id",
            "i_manufact",
            "i_category",
            "i_color",
            "i_units",
            "i_size",
            "i_product_name",
        ],
    )

    category = item["i_category"]
    color = item["i_color"]
    units = item["i_units"]
    size = item["i_size"]

    def profile(cat, colors, unit_list, sizes):
        return (
            (category == cat)
            & color.isin(colors)
            & units.isin(unit_list)
            & size.isin(sizes)
        )

    matched = (
        profile("Women", ["powder", "khaki"], ["Ounce", "Oz"],
                ["medium", "extra large"])
        | profile("Women", ["brown", "honeydew"], ["Bunch", "Ton"],
                  ["N/A", "small"])
        | profile("Men", ["floral", "deep"], ["N/A", "Dozen"], ["petite"])
        | profile("Men", ["light", "cornflower"], ["Box", "Pound"],
                  ["medium", "extra large"])
        | profile("Women", ["midnight", "snow"], ["Pallet", "Gross"],
                  ["medium", "extra large"])
        | profile("Women", ["cyan", "papaya"], ["Cup", "Dram"],
                  ["N/A", "small"])
        | profile("Men", ["orange", "frosted"], ["Each", "Tbl"], ["petite"])
        | profile("Men", ["forest", "ghost"], ["Lb", "Bundle"],
                  ["medium", "extra large"])
    )

    # The correlated subquery counts, for each candidate's i_manufact, the
    # items matching any profile above; "> 0" is therefore membership in the
    # set of manufacturers that have at least one such item. A NULL
    # i_manufact never satisfies the equality, so nulls are dropped.
    #
    # The membership test is spelled as an inner join against the distinct
    # manufacturers rather than isin(): the right-hand side is a column of a
    # partitioned frame, and only a join makes every partition see all of it.
    manufacts = item[matched][["i_manufact"]].dropna().drop_duplicates()

    candidates = item[
        (item["i_manufact_id"] >= 738) & (item["i_manufact_id"] <= 738 + 40)
    ][["i_manufact", "i_product_name"]]

    result = (
        candidates.merge(manufacts, on="i_manufact")[["i_product_name"]]
        .drop_duplicates()
        .sort_values("i_product_name")
        .head(100)
        .reset_index(drop=True)
    )
    return result
