# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""How far up the scale factors this machine goes, and what it costs.

    python -m cudf.multigpu.plot_scaling --out scaling_tpch

Two questions, two panels, because they are different questions:

**Left -- can it run at all?** Queries completed out of 22, against scale
factor. This is the POC's actual claim: a single GPU stops being able to answer
PDS-H well before the data stops fitting on eight. Capability is a count, so the
axis is linear and starts at zero.

**Right -- what does it cost?** Total wall-clock for the queries that ran. Time
spans 10 s to 45 min across the sweep, so the axis is log; a log axis flatters
differences, so the multiplier between the lines is written on the chart rather
than left to be estimated from the gap.

A point is plotted only where that configuration was actually measured. The
single-GPU series stops at SF300 because it was not run further -- by then it
needed 44 minutes and was answering only 8 of 22 queries on the GPU. That is a
limit of the measurement, not a measured limit, so the chart says which.

Colour: three series, one hue each, fixed order, validated for CVD separation
(OKLab dE >= 8 on every adjacent pair under protan/deutan/tritan). The green sits
at 2.74:1 on the light surface, below the 3:1 contrast floor, so every series is
also direct-labelled and the numbers are printed as a table -- identity and value
never depend on colour alone.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

TPCH = Path("/raid/pgali/tpch")

#: (label, {scale: logfile}). Only files that exist and finished get plotted.
SERIES = [
    ("1 GPU, stock cudf.pandas", {
        1: TPCH / "1gpu_sf1.log",
        100: TPCH / "1gpu_sf100.log",
        300: TPCH / "1gpu_sf300.log",
    }),
    ("8 GPUs, pool", {
        1: TPCH / "final_sf1_pool.log",
        100: TPCH / "final_sf100_pool.log",
        300: TPCH / "final_sf300_pool.log",
        500: TPCH / "final_sf500_pool.log",
        1000: TPCH / "final_sf1000_pool.log",
    }),
    ("8 GPUs, managed (UVM)", {
        500: TPCH / "final_sf500_managed.log",
        1000: TPCH / "final_sf1000_managed.log",
    }),
]
SCALES = [1, 100, 300, 500, 1000]

THEMES = {
    "light": {
        "surface": "#fcfcfb", "text": "#0b0b0b", "muted": "#52514e",
        "grid": "#e6e5e2",
        "series": ["#2a78d6", "#eb6834", "#1baf7a"],
    },
    "dark": {
        "surface": "#1a1a19", "text": "#ffffff", "muted": "#c3c2b7",
        "grid": "#333331",
        "series": ["#5598e7", "#f0834f", "#2fc78e"],
    },
}


def parse(path: Path):
    """-> (n_on_gpu, n_ran, total_seconds) or None if the run never finished."""
    if not path.exists():
        return None
    text = path.read_text()
    if "queries ran" not in text:
        return None
    total, ran, on_gpu = 0.0, 0, 0
    for line in text.splitlines():
        m = re.match(r"^\s*(\d+)\s+ok\s+([\d.]+)s\s*([+-][\d.]+)G", line)
        if not m:
            continue
        ran += 1
        total += float(m.group(2))
        if "ran on CPU" not in line:
            on_gpu += 1
    return on_gpu, ran, total


def collect():
    out = []
    for label, logs in SERIES:
        points = {}
        for scale, path in logs.items():
            got = parse(path)
            if got:
                points[scale] = got
        if points:
            out.append((label, points))
    return out


def _fmt(sec: float) -> str:
    if sec >= 3600:
        return f"{sec / 3600:.1f} h"
    if sec >= 90:
        return f"{sec / 60:.0f} min"
    return f"{sec:.0f} s"


def build(data, theme_name="light"):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    t = THEMES[theme_name]
    fig, (cap, cost) = plt.subplots(1, 2, figsize=(13.6, 6.0), dpi=200)
    fig.patch.set_facecolor(t["surface"])

    # Scale factors are plotted at even spacing rather than on a log axis.
    # The five sampled values are 1, 100, 300, 500, 1000, which on a log axis
    # crowd the last three into an unreadable smear while stranding SF1. The
    # comparison being made is between the series at each scale, not the
    # spacing between scales, so evenness costs nothing and is stated here.
    at = {s: i for i, s in enumerate(SCALES)}

    for ax in (cap, cost):
        ax.set_facecolor(t["surface"])
        ax.set_xticks(range(len(SCALES)))
        ax.set_xticklabels([f"SF{s}" for s in SCALES], fontsize=10.5,
                           color=t["muted"])
        ax.set_xlim(-0.35, len(SCALES) - 0.65)
        ax.grid(color=t["grid"], linewidth=0.8, zorder=0)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("bottom", "left"):
            ax.spines[side].set_color(t["grid"])
            ax.spines[side].set_linewidth(0.8)
        ax.tick_params(length=0, pad=6, labelsize=10, labelcolor=t["muted"])

    for i, (label, points) in enumerate(data):
        color = t["series"][i % len(t["series"])]
        xs = sorted(points)

        cap.plot([at[x] for x in xs], [points[x][0] for x in xs], "-o",
                 color=color, linewidth=2.0, markersize=9,
                 markeredgecolor=t["surface"], markeredgewidth=2, zorder=3)
        cap.annotate(label, (at[xs[-1]], points[xs[-1]][0]), xytext=(10, 0),
                     textcoords="offset points", va="center", fontsize=9.5,
                     color=t["text"], fontweight="medium")

        # A run that answered fewer queries did less work, so its total is not
        # on the same footing as a complete one. Plotting them on one line made
        # the pool appear to get *faster* from SF500 to SF1000, when what
        # actually happened is that seven queries ran out of memory. Complete
        # runs carry the line; incomplete ones are drawn detached and open, and
        # labelled with what they managed.
        whole = [x for x in xs if points[x][1] == 22]
        partial = [x for x in xs if points[x][1] != 22]
        if whole:
            cost.plot([at[x] for x in whole], [points[x][2] for x in whole],
                      "-o", color=color, linewidth=2.0, markersize=9,
                      markeredgecolor=t["surface"], markeredgewidth=2, zorder=3)
            cost.annotate(_fmt(points[whole[-1]][2]),
                          (at[whole[-1]], points[whole[-1]][2]), xytext=(10, 0),
                          textcoords="offset points", va="center", fontsize=9.5,
                          color=t["text"], fontweight="medium")
        for x in partial:
            cost.plot([at[x]], [points[x][2]], "o", color=t["surface"],
                      markersize=9, markeredgecolor=color, markeredgewidth=2,
                      zorder=3)
            cost.annotate(f"only {points[x][1]}/22", (at[x], points[x][2]),
                          xytext=(0, -18), textcoords="offset points",
                          ha="center", fontsize=8.5, color=t["muted"],
                          style="italic")

    by_label = dict(data)
    stock = by_label.get("1 GPU, stock cudf.pandas", {})
    pool = by_label.get("8 GPUs, pool", {})
    # a log axis makes a 45x gap look like a short hop; say the number
    for scale in sorted(set(stock) & set(pool)):
        if stock[scale][1] != 22 or pool[scale][1] != 22:
            continue
        ratio = stock[scale][2] / pool[scale][2]
        if ratio < 2:
            continue
        cost.annotate(f"{ratio:.0f}x faster",
                      (at[scale], (stock[scale][2] * pool[scale][2]) ** 0.5),
                      ha="center", va="center", fontsize=10.5,
                      color=t["text"], fontweight="semibold",
                      bbox=dict(boxstyle="round,pad=0.3", fc=t["surface"],
                                ec=t["grid"], lw=0.8))

    if stock:
        last = max(stock)
        cap.annotate("not run past SF300: already 44 min\nwith 8 of 22 on the GPU",
                     (at[last], stock[last][0]), xytext=(14, -44),
                     textcoords="offset points", fontsize=9,
                     color=t["muted"], style="italic",
                     arrowprops=dict(arrowstyle="->", color=t["grid"], lw=1.0))

    cap.set_ylim(0, 24.5)
    cap.set_yticks([0, 5, 10, 15, 20, 22])
    cap.axhline(22, color=t["grid"], linewidth=1.0, linestyle="--", zorder=1)
    cap.annotate("all 22", (-0.3, 22), xytext=(0, 5),
                 textcoords="offset points", fontsize=9, color=t["muted"])
    cap.set_ylabel("queries completed on GPU, of 22", fontsize=10.5,
                   color=t["muted"], labelpad=8)
    cap.set_title("Can it run at all?", fontsize=13, color=t["text"],
                  fontweight="semibold", pad=12, loc="left")

    cost.set_yscale("log")
    cost.set_ylabel("total wall-clock  (log scale)", fontsize=10.5,
                    color=t["muted"], labelpad=8)
    cost.set_title("What does it cost?", fontsize=13, color=t["text"],
                   fontweight="semibold", pad=12, loc="left")

    fig.suptitle("PDS-H across scale factors on 8x RTX PRO 6000 (776 GiB total)",
                 x=0.012, y=0.975, ha="left", fontsize=15.5, color=t["text"],
                 fontweight="semibold")
    fig.text(0.012, 0.915,
             "Row-partitioned multi-GPU cuDF. Every run is --strict: a query "
             "that silently fell back to pandas is a failure, not a result.",
             ha="left", fontsize=10.5, color=t["muted"])
    fig.text(0.012, 0.022,
             "Scale factors are evenly spaced, not to scale. Right panel: "
             "only runs that answered all 22 queries are joined by a line -- "
             "an open marker did less work, so its total is not comparable.",
             ha="left", fontsize=9, color=t["muted"])
    fig.subplots_adjust(left=0.075, right=0.80, top=0.80, bottom=0.135,
                        wspace=0.42)
    return fig


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="scaling_tpch")
    args = parser.parse_args()
    data = collect()

    # the table view the contrast WARN obligates
    print(f"{'':<28}" + "".join(f"{'SF' + str(s):>16}" for s in SCALES))
    for label, points in data:
        cells = []
        for s in SCALES:
            if s in points:
                on_gpu, _ran, total = points[s]
                cells.append(f"{on_gpu:>2}/22 {_fmt(total):>9}")
            else:
                cells.append(f"{'-':>16}")
        print(f"{label:<28}" + "".join(f"{c:>16}" for c in cells))

    for theme in ("light", "dark"):
        fig = build(data, theme)
        suffix = "" if theme == "light" else "_dark"
        path = f"{args.out}{suffix}.png"
        fig.savefig(path, facecolor=fig.get_facecolor())
        print("wrote", path)


if __name__ == "__main__":
    main()
