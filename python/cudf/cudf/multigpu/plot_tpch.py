# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Plot the PDS-H single-GPU vs multi-GPU comparison.

    python -m cudf.multigpu.plot_tpch --out benchmark_tpch

A dumbbell rather than grouped bars: the values span three orders of magnitude,
so the axis has to be logarithmic, and a bar on a log axis is dishonest (a bar
encodes magnitude by its length from zero, and a log axis has no zero). Dots
encode *position*, which stays truthful, and the connector between them becomes
the thing the chart is actually about -- its length is the speedup and its
direction says which configuration won.
"""

from __future__ import annotations

import argparse

# ── measured on 8x RTX PRO 6000 (97 GiB each), cudf 26.10 ────────────────────
# 1-GPU and 8-GPU runs both go through cudf.multigpu.tpch_pdsh so the timing
# points and the host-RSS fallback detector are identical.
RESULTS = [
    # label, 1-GPU seconds, 8-GPU seconds, 1-GPU queries that ran on CPU, n
    ("SF1",   2.62, 9.77, 0, 22),
    ("SF100", 1138.51, 28.32, 5, 22),
    ("SF300", 2626.93, 30.83, 11, 19),
]

THEMES = {
    "light": {
        "surface": "#fcfcfb",
        "text": "#0b0b0b",
        "muted": "#52514e",
        "grid": "#e6e5e2",
        "connector": "#b8b7b2",
        "one_gpu": "#2a78d6",   # categorical slot 1
        "many_gpu": "#eb6834",  # categorical slot 2
    },
    "dark": {
        "surface": "#1a1a19",
        "text": "#ffffff",
        "muted": "#c3c2b7",
        "grid": "#333331",
        "connector": "#4f4e4a",
        "one_gpu": "#3987e5",
        "many_gpu": "#d95926",
    },
}


def _fmt_seconds(value: float) -> str:
    if value >= 600:
        return f"{value / 60:.0f} min"
    if value >= 100:
        return f"{value:.0f} s"
    if value >= 10:
        return f"{value:.1f} s"
    return f"{value:.2f} s"


def build(theme_name: str = "light"):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    t = THEMES[theme_name]
    fig, ax = plt.subplots(figsize=(11.0, 5.0), dpi=200)
    fig.patch.set_facecolor(t["surface"])
    ax.set_facecolor(t["surface"])

    # first row at the top
    ypos = {row[0]: len(RESULTS) - 1 - i for i, row in enumerate(RESULTS)}

    # recessive solid hairline grid at the decades, drawn under everything
    ax.set_xscale("log")
    ax.grid(axis="x", color=t["grid"], linewidth=0.8, linestyle="-", zorder=0)
    ax.set_axisbelow(True)

    for label, one, many, on_cpu, nq in RESULTS:
        y = ypos[label]
        lo_val, hi_val = min(one, many), max(one, many)

        # the connector is the subject of the chart: its length is the ratio
        # and its direction says which configuration won. Recessive, so the
        # two dots keep identity.
        ax.plot([lo_val, hi_val], [y, y], color=t["connector"], linewidth=3.0,
                solid_capstyle="round", zorder=1)
        # 2px surface ring so the markers stay separate from the connector
        ax.plot(one, y, "o", markersize=13, color=t["one_gpu"],
                markeredgecolor=t["surface"], markeredgewidth=2, zorder=3)
        ax.plot(many, y, "o", markersize=13, color=t["many_gpu"],
                markeredgecolor=t["surface"], markeredgewidth=2, zorder=3)

        # values sit outside the pair, horizontally: six labels total, and the
        # values are the point. Placing them outside leaves the space between
        # the dots free for the two notes.
        ax.annotate(_fmt_seconds(lo_val), (lo_val, y), xytext=(-13, 0),
                    textcoords="offset points", ha="right", va="center",
                    color=t["text"], fontsize=11)
        ax.annotate(_fmt_seconds(hi_val), (hi_val, y), xytext=(13, 0),
                    textcoords="offset points", ha="left", va="center",
                    color=t["text"], fontsize=11)

        mid = (lo_val * hi_val) ** 0.5  # geometric mid == visual mid on a log axis
        ratio = one / many
        note = (f"{ratio:.0f}x faster on 8 GPUs" if ratio >= 2
                else f"{1 / ratio:.1f}x slower on 8 GPUs")
        ax.annotate(note, (mid, y), xytext=(0, 13), textcoords="offset points",
                    ha="center", color=t["muted"], fontsize=10)
        if on_cpu:
            ax.annotate(f"{on_cpu} of {nq} queries silently ran on the CPU",
                        (mid, y), xytext=(0, -22), textcoords="offset points",
                        ha="center", color=t["muted"], fontsize=9.5,
                        style="italic")

    ax.set_yticks([ypos[row[0]] for row in RESULTS])
    ax.set_yticklabels([f"{row[0]}\n{_scale_note(row[0])}" for row in RESULTS],
                       fontsize=12, color=t["text"])
    ax.set_ylim(-0.75, len(RESULTS) - 0.25)

    ax.set_xlim(0.55, 20000)
    ax.set_xticks([1, 10, 100, 1000, 10000])
    ax.set_xticklabels(["1 s", "10 s", "100 s", "1,000 s", "10,000 s"],
                       fontsize=10, color=t["muted"])
    ax.set_xlabel("total wall-clock time for the PDS-H query set  (log scale)",
                  fontsize=10.5, color=t["muted"], labelpad=10)

    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(t["grid"])
    ax.spines["bottom"].set_linewidth(0.8)
    ax.tick_params(axis="both", length=0, pad=8)
    ax.tick_params(axis="y", labelcolor=t["text"])

    legend = ax.legend(
        handles=[
            Line2D([], [], marker="o", linestyle="none", markersize=11,
                   color=t["one_gpu"], label="1 GPU (stock cudf.pandas)"),
            Line2D([], [], marker="o", linestyle="none", markersize=11,
                   color=t["many_gpu"], label="8 GPUs (cudf.multigpu)"),
        ],
        loc="upper right", frameon=False, fontsize=10.5,
        handletextpad=0.5, borderaxespad=0.2, ncol=1,
    )
    for text in legend.get_texts():
        text.set_color(t["text"])

    fig.suptitle(
        "One GPU is faster until the data stops fitting",
        x=0.012, y=0.975, ha="left", fontsize=15.5,
        color=t["text"], fontweight="semibold",
    )
    fig.text(
        0.012, 0.905,
        "PDS-H (TPC-H) on 8 x RTX PRO 6000, 97 GiB each. Both configurations "
        "measured through the same runner.",
        ha="left", fontsize=10.5, color=t["muted"],
    )
    fig.text(
        0.012, 0.028,
        "SF300 compares the 19 queries that completed in both configurations; "
        "q8, q9 and q10 finish in neither.",
        ha="left", fontsize=9, color=t["muted"],
    )
    fig.subplots_adjust(left=0.10, right=0.945, top=0.78, bottom=0.19)
    return fig


def _scale_note(label: str) -> str:
    return {
        "SF1": "1 GB",
        "SF100": "100 GB",
        "SF300": "300 GB",
    }[label]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="benchmark_tpch")
    args = parser.parse_args()
    for theme in ("light", "dark"):
        fig = build(theme)
        suffix = "" if theme == "light" else "_dark"
        path = f"{args.out}{suffix}.png"
        fig.savefig(path, facecolor=fig.get_facecolor())
        print("wrote", path)


if __name__ == "__main__":
    main()
