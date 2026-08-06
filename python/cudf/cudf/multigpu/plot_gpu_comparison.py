# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""One GPU against eight, on TPC-H and TPC-DS.

    python -m cudf.multigpu.plot_gpu_comparison --out gpu_comparison

A dumbbell per scale factor: one dot for each configuration, joined by the gap
between them. That form is chosen because the quantity of interest *is* the
gap -- grouped bars would make the reader compare bar lengths across a log
axis, which is exactly what people misread.

Times are like-for-like: each pair covers only the queries **both**
configurations completed, so a scale factor where one GPU gave up on three
queries does not get credit for the time it saved by not running them. The
subset size is printed on the chart.

The axis is log because the range runs from 2.6 s to 19.5 hours. A log axis
flatters differences, so every multiplier is written on the chart rather than
left to be estimated from the gap -- including the one case where multi-GPU is
*slower*, which is the chart's most important honesty check.

Scale factors with no single-GPU run are drawn as a lone dot rather than a
dumbbell, so an absent baseline never reads as a fast one.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

TPCH = Path("/raid/pgali/tpch")
TPCDS = Path("/raid/pgali/tpcds")

#: (benchmark, {scale: (one_gpu_log, eight_gpu_log)}); None means not run
SERIES = {
    "TPC-H (22 queries)": {
        1: (TPCH / "1gpu_sf1.log", TPCH / "final_sf1_pool.log"),
        100: (TPCH / "1gpu_sf100.log", TPCH / "final_sf100_pool.log"),
        300: (TPCH / "1gpu_sf300.log", TPCH / "final_sf300_pool.log"),
        500: (None, TPCH / "final_sf500_managed.log"),
        1000: (None, TPCH / "final_sf1000_managed.log"),
    },
    "TPC-DS (99 queries)": {
        1: (TPCDS / "final_sf1_cudf.log", TPCDS / "final_sf1_multigpu.log"),
        10: (TPCDS / "final_sf10_cudf.log", TPCDS / "final_sf10_multigpu.log"),
        100: (TPCDS / "final_sf100_cudf_MERGED.json",
              TPCDS / "final_sf100_multigpu.log"),
        300: (None, TPCDS / "final_sf300_multigpu.log"),
        500: (None, TPCDS / "final_sf500_multigpu.log"),
        1000: (None, TPCDS / "final_sf1000_multigpu.log"),
    },
}

THEMES = {
    "light": {
        "surface": "#fcfcfb", "text": "#0b0b0b", "muted": "#52514e",
        "grid": "#e6e5e2", "one": "#eb6834", "eight": "#2a78d6",
    },
    "dark": {
        "surface": "#1a1a19", "text": "#ffffff", "muted": "#c3c2b7",
        "grid": "#333331", "one": "#f0834f", "eight": "#5598e7",
    },
}


def _load(path):
    """-> {query: seconds} for queries that completed, or None."""
    if path is None or not os.path.exists(path):
        return None
    if str(path).endswith(".json"):
        data = json.load(open(path))
        return {int(k): v["seconds"] for k, v in data["per_query"].items()
                if v["status"] == "ok"}
    out = {}
    for line in open(path):
        m = re.match(r"^ *(\d+) +ok +([\d.]+)s", line)
        if m:
            out[int(m.group(1))] = float(m.group(2))
    return out or None


def collect():
    """-> {benchmark: [(scale, one, eight, n_common), ...]}"""
    data = {}
    for name, scales in SERIES.items():
        rows = []
        for scale, (one_path, eight_path) in sorted(scales.items()):
            one, eight = _load(one_path), _load(eight_path)
            if eight is None:
                continue
            if one is None:
                rows.append((scale, None, sum(eight.values()), len(eight)))
                continue
            # only the queries both finished, so neither is credited for work
            # it skipped
            common = set(one) & set(eight)
            rows.append((scale,
                         sum(one[q] for q in common),
                         sum(eight[q] for q in common),
                         len(common)))
        data[name] = rows
    return data


def _fmt(sec: float) -> str:
    if sec >= 3600:
        return f"{sec / 3600:.1f} h"
    if sec >= 90:
        return f"{sec / 60:.0f} min"
    if sec >= 10:
        return f"{sec:.0f} s"
    return f"{sec:.1f} s"


def build(data, theme_name="light"):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    t = THEMES[theme_name]
    fig, axes = plt.subplots(1, 2, figsize=(14.2, 6.4), dpi=200)
    fig.patch.set_facecolor(t["surface"])

    for ax, (name, rows) in zip(axes, data.items()):
        ax.set_facecolor(t["surface"])
        ys = list(range(len(rows)))

        for y, (scale, one, eight, n) in zip(ys, rows):
            if one is not None:
                ax.plot([eight, one], [y, y], color=t["grid"], linewidth=2.5,
                        solid_capstyle="round", zorder=1)
                ax.plot([one], [y], "o", color=t["one"], markersize=11,
                        markeredgecolor=t["surface"], markeredgewidth=2,
                        zorder=3)
                ratio = one / eight
                # the multiplier is stated, never left to the log axis
                label = (f"{ratio:.0f}x faster" if ratio >= 2
                         else f"{1 / ratio:.1f}x SLOWER")
                ax.annotate(label, ((one * eight) ** 0.5, y), xytext=(0, 9),
                            textcoords="offset points", ha="center",
                            fontsize=10, fontweight="semibold",
                            color=t["text"])
                ax.annotate(_fmt(one), (one, y), xytext=(11, 0),
                            textcoords="offset points", va="center",
                            fontsize=9.5, color=t["muted"])
            else:
                ax.annotate("no single-GPU run", (eight, y), xytext=(13, 0),
                            textcoords="offset points", va="center",
                            fontsize=9, color=t["muted"], style="italic")
            ax.plot([eight], [y], "o", color=t["eight"], markersize=11,
                    markeredgecolor=t["surface"], markeredgewidth=2, zorder=3)
            ax.annotate(_fmt(eight), (eight, y), xytext=(0, -17),
                        textcoords="offset points", ha="center",
                        fontsize=9.5, color=t["muted"])

        ax.set_xscale("log")
        ax.set_yticks(ys)
        ax.set_yticklabels(
            [f"SF{s}\n{n} queries" for s, _o, _e, n in rows],
            fontsize=10, color=t["text"])
        ax.invert_yaxis()
        ax.set_ylim(len(rows) - 0.4, -0.75)
        ax.set_xlim(1.2, 2.4e5)
        ax.grid(axis="x", color=t["grid"], linewidth=0.8, zorder=0)
        ax.set_axisbelow(True)
        for side in ("top", "right", "left"):
            ax.spines[side].set_visible(False)
        ax.spines["bottom"].set_color(t["grid"])
        ax.tick_params(length=0, pad=6, labelsize=9.5, labelcolor=t["muted"])
        ax.set_xlabel("wall-clock for the whole suite  (log scale)",
                      fontsize=10, color=t["muted"], labelpad=8)
        ax.set_title(name, fontsize=13.5, color=t["text"],
                     fontweight="semibold", pad=14, loc="left")

    legend = fig.legend(
        handles=[
            Line2D([], [], marker="o", linestyle="none", color=t["one"],
                   markersize=11, label="1 GPU (stock cudf.pandas)"),
            Line2D([], [], marker="o", linestyle="none", color=t["eight"],
                   markersize=11, label="8 GPUs (cudf.multigpu)"),
        ],
        loc="lower center", ncol=2, frameon=False, fontsize=11,
        bbox_to_anchor=(0.5, 0.072), handletextpad=0.6, columnspacing=2.4)
    for text in legend.get_texts():
        text.set_color(t["text"])

    fig.suptitle("One GPU against eight, same queries, same machine",
                 x=0.011, y=0.973, ha="left", fontsize=16, color=t["text"],
                 fontweight="semibold")
    fig.text(0.011, 0.918,
             "Each pair covers only the queries BOTH configurations finished, "
             "so neither is credited for work it skipped.",
             ha="left", fontsize=10.5, color=t["muted"])
    fig.text(0.011, 0.018,
             "8x RTX PRO 6000, 97 GiB each. Single-GPU totals include queries "
             "cudf.pandas silently ran on the CPU -- 65 of 99 at TPC-DS SF100. "
             "Above SF300 no single-GPU run was attempted.",
             ha="left", fontsize=9, color=t["muted"])
    fig.subplots_adjust(left=0.085, right=0.975, top=0.845, bottom=0.185,
                        wspace=0.30)
    return fig


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="gpu_comparison")
    args = parser.parse_args()
    data = collect()

    for name, rows in data.items():
        print(f"\n{name}")
        for scale, one, eight, n in rows:
            if one is None:
                print(f"  SF{scale:<5} {n:>3} queries | 1 GPU: not run"
                      f"        | 8 GPUs: {_fmt(eight):>8}")
            else:
                print(f"  SF{scale:<5} {n:>3} queries | 1 GPU: {_fmt(one):>8}"
                      f"      | 8 GPUs: {_fmt(eight):>8}"
                      f"  ({one / eight:.1f}x)")

    for theme in ("light", "dark"):
        fig = build(data, theme)
        path = f"{args.out}{'' if theme == 'light' else '_dark'}.png"
        fig.savefig(path, facecolor=fig.get_facecolor())
        print("wrote", path)


if __name__ == "__main__":
    main()
