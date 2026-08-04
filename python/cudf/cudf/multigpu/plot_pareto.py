# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Cost/performance curve: what does each additional GPU actually buy?

    python -m cudf.multigpu.plot_pareto --out pareto_tpch

Reads the sweep logs written by ``tpch_pdsh`` at 1/2/4/8 GPUs and plots wall
time against the number of GPUs spent -- the two axes anyone actually trades
off. A point is only comparable to its neighbours if it did the same work, so
each scale factor is scored on the subset of queries that *every* device count
completed entirely on the GPUs, and the subset size is stated on the chart.

The reading is Pareto: for a given GPU budget, the lowest point is the best
achievable time. A configuration sitting above and to the right of another is
dominated -- it costs more and delivers less.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

SWEEP = Path("/raid/pgali/tpch/sweep")
STOCK = {  # stock single-GPU cudf.pandas, same harness
    1: Path("/raid/pgali/tpch/1gpu_sf1.log"),
    100: Path("/raid/pgali/tpch/1gpu_sf100.log"),
    300: Path("/raid/pgali/tpch/1gpu_sf300.log"),
}
SCALES = [1, 100, 300]
COUNTS = [1, 2, 4, 8]

THEMES = {
    "light": {
        "surface": "#fcfcfb", "text": "#0b0b0b", "muted": "#52514e",
        "grid": "#e6e5e2",
        "series": {1: "#2a78d6", 100: "#eb6834", 300: "#1baf7a"},
    },
    "dark": {
        "surface": "#1a1a19", "text": "#ffffff", "muted": "#c3c2b7",
        "grid": "#333331",
        "series": {1: "#3987e5", 100: "#d95926", 300: "#199e70"},
    },
}


def parse(path: Path, gpu_only: bool = True):
    """-> {query: seconds}; ``None`` if the run never finished."""
    if not path.exists():
        return None
    text = path.read_text()
    if "on GPU:" not in text:
        return None
    out = {}
    for line in text.splitlines():
        m = re.match(r"^\s*(\d+)\s+ok\s+([\d.]+)s\s*([+-][\d.]+)G", line)
        if not m:
            continue
        if gpu_only and "ran on CPU" in line:
            continue
        out[int(m.group(1))] = float(m.group(2))
    return out


def collect():
    """-> {scale: {"common": [...], "multi": {n: secs}, "stock": secs|None}}"""
    data = {}
    for scale in SCALES:
        runs = {n: parse(SWEEP / f"sf{scale}_g{n}.log") for n in COUNTS}
        done = {n: r for n, r in runs.items() if r}
        if not done:
            continue
        common = set.intersection(*(set(r) for r in done.values()))
        if not common:
            continue
        # stock is scored on the same queries, CPU time included: that is what
        # a one-GPU budget actually costs you
        stock_all = parse(STOCK[scale], gpu_only=False) or {}
        stock = (sum(stock_all[q] for q in common)
                 if common <= set(stock_all) else None)
        data[scale] = {
            "common": sorted(common),
            "multi": {n: sum(r[q] for q in common) for n, r in done.items()},
            # capability, not speed: how many of the 22 that budget can run at
            # all. The common subset necessarily drops the queries that need
            # the most memory, i.e. exactly the ones extra GPUs are bought for,
            # so the counts have to travel with the times.
            "completed": {n: len(r) for n, r in done.items()},
            "stock_completed": len(
                [q for q, _ in (stock_all or {}).items()]),
            "missing": [n for n in COUNTS if runs.get(n) is None],
            "stock": stock,
        }
    return data


def build(data, theme_name="light"):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    t = THEMES[theme_name]
    fig, ax = plt.subplots(figsize=(10.5, 6.2), dpi=200)
    fig.patch.set_facecolor(t["surface"])
    ax.set_facecolor(t["surface"])

    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.grid(color=t["grid"], linewidth=0.8, linestyle="-", zorder=0)
    ax.set_axisbelow(True)

    for scale in SCALES:
        if scale not in data:
            continue
        entry = data[scale]
        color = t["series"][scale]
        xs = sorted(entry["multi"])
        ys = [entry["multi"][n] for n in xs]

        ax.plot(xs, ys, "-", color=color, linewidth=2.0, zorder=2)
        ax.plot(xs, ys, "o", color=color, markersize=10,
                markeredgecolor=t["surface"], markeredgewidth=2, zorder=3)

        # capability rides with the time: how many of the 22 this budget ran
        for n in xs:
            ax.annotate(f"{entry['completed'][n]}/22",
                        (n, entry["multi"][n]), xytext=(0, -17),
                        textcoords="offset points", ha="center",
                        color=t["muted"], fontsize=8.5)

        # stock cudf.pandas is a different implementation at the same cost of
        # one GPU, so it gets its own mark rather than joining the line
        if entry["stock"]:
            ax.plot([1], [entry["stock"]], "D", color=color, markersize=9,
                    markeredgecolor=t["surface"], markeredgewidth=2, zorder=3)

        # direct label at the right end -- required anyway by the relief rule
        # for the aqua slot in light mode
        best_n = min(entry["multi"], key=entry["multi"].get)
        ax.annotate(f"SF{scale}", (xs[-1], ys[-1]), xytext=(14, 0),
                    textcoords="offset points", va="center", ha="left",
                    color=t["text"], fontsize=12, fontweight="medium")
        ax.annotate(f"{len(entry['common'])} queries",
                    (xs[-1], ys[-1]), xytext=(14, -14),
                    textcoords="offset points", va="center", ha="left",
                    color=t["muted"], fontsize=9)

        # the knee: fewest GPUs that reach the best time
        ax.annotate(f"best: {best_n} GPU{'s' if best_n > 1 else ''}"
                    f"  ({entry['multi'][best_n]:.1f} s)",
                    (best_n, entry["multi"][best_n]), xytext=(0, 16),
                    textcoords="offset points", ha="center",
                    color=t["muted"], fontsize=9.5)

        for n in entry["missing"]:
            ax.annotate("x", (n, max(ys) * 1.9), ha="center", va="center",
                        color=color, fontsize=13, fontweight="bold")

    ax.set_xticks(COUNTS)
    ax.set_xticklabels([str(n) for n in COUNTS], fontsize=11, color=t["muted"])
    ax.minorticks_off()
    ax.set_xlim(0.82, 14)
    ax.set_xlabel("GPUs spent", fontsize=11, color=t["muted"], labelpad=10)
    ax.set_ylabel("wall-clock time  (log scale)", fontsize=11,
                  color=t["muted"], labelpad=10)
    ax.tick_params(axis="y", labelsize=10, labelcolor=t["muted"], length=0)
    ax.tick_params(axis="x", length=0, pad=6)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("bottom", "left"):
        ax.spines[side].set_color(t["grid"])
        ax.spines[side].set_linewidth(0.8)

    legend = ax.legend(
        handles=[
            Line2D([], [], marker="o", color=t["muted"], linewidth=2,
                   markersize=10, label="cudf.multigpu"),
            Line2D([], [], marker="D", linestyle="none", color=t["muted"],
                   markersize=9, label="stock cudf.pandas (1 GPU)"),
        ],
        loc="lower left", frameon=False, fontsize=10.5,
        handletextpad=0.6, borderaxespad=0.4,
    )
    for text in legend.get_texts():
        text.set_color(t["text"])

    fig.suptitle("What each extra GPU actually buys",
                 x=0.012, y=0.972, ha="left", fontsize=15.5,
                 color=t["text"], fontweight="semibold")
    fig.text(0.012, 0.912,
             "Lower and further left is better. A point above and right of "
             "another is dominated: it costs more and delivers less.",
             ha="left", fontsize=10.5, color=t["muted"])
    fig.text(0.012, 0.028,
             "Times are scored on the queries every device count completed "
             "entirely on the GPUs; n/22 is how many each budget ran at all. "
             "The subset necessarily omits the memory-hungry queries, so the "
             "curves understate what the extra GPUs buy.",
             ha="left", fontsize=9, color=t["muted"])
    fig.subplots_adjust(left=0.105, right=0.86, top=0.80, bottom=0.135)
    return fig


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="pareto_tpch")
    args = parser.parse_args()
    data = collect()
    for scale, entry in data.items():
        print(f"SF{scale}: subset={len(entry['common'])} "
              f"multi={ {n: round(v, 2) for n, v in entry['multi'].items()} } "
              f"stock={entry['stock'] and round(entry['stock'], 2)}")
    for theme in ("light", "dark"):
        fig = build(data, theme)
        suffix = "" if theme == "light" else "_dark"
        path = f"{args.out}{suffix}.png"
        fig.savefig(path, facecolor=fig.get_facecolor())
        print("wrote", path)


if __name__ == "__main__":
    main()
