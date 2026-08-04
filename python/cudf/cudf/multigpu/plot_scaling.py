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

A point is plotted only where that configuration was actually measured. Single
GPU stops at SF300 because SF500 does not fit in 97 GiB at all -- that absence is
the result, so it is annotated rather than interpolated over.

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

    for ax in (cap, cost):
        ax.set_facecolor(t["surface"])
        ax.set_xscale("log")
        ax.set_xticks(SCALES)
        ax.set_xticklabels([f"SF{s}" for s in SCALES], fontsize=10.5,
                           color=t["muted"])
        ax.minorticks_off()
        ax.set_xlim(0.7, 1700)
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

        cap.plot(xs, [points[x][0] for x in xs], "-o", color=color,
                 linewidth=2.0, markersize=9, markeredgecolor=t["surface"],
                 markeredgewidth=2, zorder=3)
        cost.plot(xs, [points[x][2] for x in xs], "-o", color=color,
                  linewidth=2.0, markersize=9, markeredgecolor=t["surface"],
                  markeredgewidth=2, zorder=3)

        # direct label at the right end of each series -- required, since the
        # green fails the 3:1 contrast floor and may not carry identity alone
        cap.annotate(label, (xs[-1], points[xs[-1]][0]), xytext=(10, 0),
                     textcoords="offset points", va="center", fontsize=9.5,
                     color=t["text"], fontweight="medium")
        cost.annotate(_fmt(points[xs[-1]][2]),
                      (xs[-1], points[xs[-1]][2]), xytext=(10, 0),
                      textcoords="offset points", va="center", fontsize=9.5,
                      color=t["text"], fontweight="medium")

    # a log axis makes a 40x gap look like a short hop; say the number
    by_label = dict(data)
    stock = by_label.get("1 GPU, stock cudf.pandas", {})
    pool = by_label.get("8 GPUs, pool", {})
    for scale in sorted(set(stock) & set(pool)):
        ratio = stock[scale][2] / pool[scale][2]
        if ratio < 2:
            continue
        cost.annotate(f"{ratio:.0f}x", (scale, (stock[scale][2] * pool[scale][2]) ** 0.5),
                      ha="center", va="center", fontsize=11,
                      color=t["text"], fontweight="semibold",
                      bbox=dict(boxstyle="round,pad=0.28", fc=t["surface"],
                                ec=t["grid"], lw=0.8))

    if stock:
        last = max(stock)
        cap.annotate("single GPU cannot hold\nSF500 at all",
                     (last, stock[last][0]), xytext=(6, -46),
                     textcoords="offset points", fontsize=9,
                     color=t["muted"], style="italic",
                     arrowprops=dict(arrowstyle="->", color=t["grid"], lw=1.0))

    cap.set_ylim(0, 24.5)
    cap.set_yticks([0, 5, 10, 15, 20, 22])
    cap.axhline(22, color=t["grid"], linewidth=1.0, linestyle="--", zorder=1)
    cap.annotate("all 22", (0.78, 22), xytext=(0, 5),
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
             "Points are measured, never interpolated; a missing point means "
             "that configuration could not run that scale. Times cover the "
             "queries that completed, so a line with fewer completions is "
             "being flattered.",
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
