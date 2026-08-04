# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Where the PDS-H time goes, query by query, at each scale factor.

    python -m cudf.multigpu.plot_composition --out composition_tpch

Every one of the 22 queries is its own segment -- nothing is folded into an
"other" bucket -- and a query keeps the same colour in every bar, so it can be
traced across configurations and scale factors. Queries that errored are named
under the bar they errored in, since a query that did not run contributes no
time and so cannot honestly be given any area.

Bars are in **seconds**, not shares, so the runtime difference is the thing you
see. Stacking is additive, so the axis must be linear, and a linear axis cannot
hold 2.6 s and 2627 s at once -- hence one panel per scale factor, each with its
own axis. Panel maxima are printed so the scales are not mistaken for equal.

On colour: 22 classes is past what any palette can keep separable -- the
accessible ceiling is about eight. Rather than invent 22 hues that only look
distinct, the queries share one ordered blue ramp (q1 lightest, q22 darkest),
which is honest about being a sequence, and identity is carried by the labels on
the segments and the reference strip, never by colour alone.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

LOGS = {
    ("SF1", "1 GPU"): "/raid/pgali/tpch/1gpu_sf1.log",
    ("SF1", "8 GPUs"): "/raid/pgali/tpch/mgpu_sf1_rss.log",
    ("SF100", "1 GPU"): "/raid/pgali/tpch/1gpu_sf100.log",
    ("SF100", "8 GPUs"): "/raid/pgali/tpch/mgpu_sf100_rss.log",
    ("SF300", "1 GPU"): "/raid/pgali/tpch/1gpu_sf300.log",
    ("SF300", "8 GPUs"): "/raid/pgali/tpch/mgpu_sf300_rss.log",
}
SCALES = ["SF1", "SF100", "SF300"]
CONFIGS = ["1 GPU", "8 GPUs"]
QUERIES = list(range(1, 23))

# documented blue ramp; the ordinal range starts at step 250 so the lightest
# segment still clears 2:1 on the light surface
RAMP_LIGHT = ["#86b6ef", "#6da7ec", "#5598e7", "#3987e5", "#2a78d6",
              "#256abf", "#1c5cab", "#184f95", "#104281", "#0d366b"]
RAMP_DARK = ["#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec",
             "#5598e7", "#3987e5", "#2a78d6", "#256abf", "#184f95"]

THEMES = {
    "light": {
        "surface": "#fcfcfb", "text": "#0b0b0b", "muted": "#52514e",
        "grid": "#e6e5e2", "ramp": RAMP_LIGHT, "on_label": "#fcfcfb",
    },
    "dark": {
        "surface": "#1a1a19", "text": "#ffffff", "muted": "#c3c2b7",
        "grid": "#333331", "ramp": RAMP_DARK, "on_label": "#0b0b0b",
    },
}


def parse(path: str):
    """-> ({query: seconds}, {query: ran_on_cpu}, [errored queries])"""
    seconds, on_cpu, errored = {}, {}, []
    for line in Path(path).read_text().splitlines():
        m = re.match(r"^\s*(\d+)\s+ok\s+([\d.]+)s\s*([+-][\d.]+)G", line)
        if m:
            q = int(m.group(1))
            seconds[q] = float(m.group(2))
            on_cpu[q] = "ran on CPU" in line
            continue
        m = re.match(r"^\s*(\d+)\s+ERROR", line)
        if m:
            errored.append(int(m.group(1)))
    return seconds, on_cpu, sorted(errored)


def _ramp(hexes, n):
    """n colours interpolated through the documented ramp, in sRGB."""
    import matplotlib.colors as mcolors

    cmap = mcolors.LinearSegmentedColormap.from_list("q", hexes)
    return [mcolors.to_hex(cmap(i / (n - 1))) for i in range(n)]


def _fmt(value: float) -> str:
    if value >= 600:
        return f"{value / 60:.0f} min"
    if value >= 100:
        return f"{value:.0f} s"
    if value >= 10:
        return f"{value:.1f} s"
    return f"{value:.2f} s"


def build(theme_name="light"):
    import textwrap

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    t = THEMES[theme_name]
    colors = dict(zip(QUERIES, _ramp(t["ramp"], len(QUERIES))))

    fig, axes = plt.subplots(2, 3, figsize=(13.5, 9.6), dpi=200)
    fig.patch.set_facecolor(t["surface"])

    def stack(ax, scale, configs, panel_max, label_floor, notes=True):
        for i, config in enumerate(configs):
            seconds, on_cpu, errored = parse(LOGS[(scale, config)])
            total = sum(seconds.values())
            bottom = 0.0
            for q in QUERIES:
                value = seconds.get(q, 0.0)
                if value <= 0:
                    continue
                ax.bar(i, value, bottom=bottom, width=0.62, color=colors[q],
                       edgecolor=t["surface"], linewidth=1.6, zorder=2)
                if value / panel_max >= label_floor:
                    ax.text(i, bottom + value / 2, f"q{q}", ha="center",
                            va="center", color=t["on_label"], fontsize=8.5,
                            fontweight="semibold", zorder=3)
                bottom += value
            ax.text(i, total + panel_max * 0.03, _fmt(total), ha="center",
                    va="bottom", color=t["text"], fontsize=13,
                    fontweight="semibold")
            if not notes:
                continue
            lines = []
            fell = sorted(q for q, cpu in on_cpu.items() if cpu)
            if fell:
                lines += textwrap.wrap(
                    "on CPU: " + ", ".join(f"q{q}" for q in fell), 20)
            if errored:
                lines.append("errored: " + ", ".join(f"q{q}" for q in errored))
            if lines:
                ax.annotate("\n".join(lines), (i, 0), xytext=(0, -32),
                            textcoords="offset points", ha="center", va="top",
                            color=t["muted"], fontsize=8.5, style="italic")

    def dress(ax, configs, panel_max, title=None):
        ax.set_facecolor(t["surface"])
        ax.set_xticks(range(len(configs)))
        ax.set_xticklabels(configs, fontsize=11.5, color=t["text"])
        ax.set_xlim(-0.62, len(configs) - 0.38)
        ax.set_ylim(0, panel_max * 1.14)
        if title:
            ax.set_title(title, fontsize=14, color=t["text"],
                         fontweight="semibold", pad=12)
        ax.grid(axis="y", color=t["grid"], linewidth=0.8, linestyle="-",
                zorder=0)
        ax.set_axisbelow(True)
        for side in ("top", "right", "left"):
            ax.spines[side].set_visible(False)
        ax.spines["bottom"].set_color(t["grid"])
        ax.spines["bottom"].set_linewidth(0.8)
        ax.tick_params(axis="both", length=0, pad=6, labelsize=9.5,
                       labelcolor=t["muted"])
        ax.set_ylabel("seconds", fontsize=10, color=t["muted"], labelpad=6)

    for col, scale in enumerate(SCALES):
        totals = {c: sum(parse(LOGS[(scale, c)])[0].values()) for c in CONFIGS}

        top = axes[0][col]
        stack(top, scale, CONFIGS, max(totals.values()), 0.045)
        dress(top, CONFIGS, max(totals.values()), title=scale)

        # the 8-GPU bar is a sliver at the shared scale -- that IS the runtime
        # story -- so it gets its own panel underneath to show composition
        bottom_ax = axes[1][col]
        stack(bottom_ax, scale, ["8 GPUs"], totals["8 GPUs"], 0.035,
              notes=False)
        dress(bottom_ax, ["8 GPUs"], totals["8 GPUs"])
        ratio = totals["1 GPU"] / totals["8 GPUs"]
        note = (f"{ratio:.0f}x shorter" if ratio >= 1
                else f"{1 / ratio:.1f}x TALLER")
        bottom_ax.set_title(f"8 GPUs on its own scale  ({note} than above)",
                            fontsize=10.5, color=t["muted"], pad=10)

    handles = [Patch(facecolor=colors[q], label=f"q{q}") for q in QUERIES]
    legend = fig.legend(handles=handles, loc="lower center", ncol=22,
                        frameon=False, fontsize=8.5, handlelength=0.9,
                        handleheight=0.9, columnspacing=0.55,
                        handletextpad=0.28, bbox_to_anchor=(0.5, 0.038))
    for text in legend.get_texts():
        text.set_color(t["text"])

    fig.text(0.008, 0.972, "PDS-H runtime by query, 1 GPU vs 8 GPUs",
             ha="left", va="top", fontsize=16, color=t["text"],
             fontweight="semibold")
    fig.text(0.008, 0.941,
             "Top row: both configurations on one axis, so the runtime "
             "difference is the picture. Bottom row: the 8-GPU bar on its own "
             "axis, so its composition is readable.",
             ha="left", va="top", fontsize=10.5, color=t["muted"])
    fig.text(0.008, 0.014,
             "All 22 queries individually; each panel has its own axis. "
             "q8/q9/q10 error at SF300 -- they exhaust memory in both "
             "configurations, so they contribute no time and are named "
             "instead.",
             ha="left", fontsize=9, color=t["muted"])
    fig.subplots_adjust(left=0.062, right=0.988, top=0.885, bottom=0.10,
                        wspace=0.24, hspace=0.52)
    return fig


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="composition_tpch")
    args = parser.parse_args()

    print(f"{'query':>6}" + "".join(
        f"{s + ' ' + c:>16}" for s in SCALES for c in CONFIGS))
    cols = [parse(LOGS[(s, c)]) for s in SCALES for c in CONFIGS]
    for q in QUERIES:
        cells = []
        for seconds, on_cpu, errored in cols:
            if q in errored:
                cells.append(f"{'errored':>16}")
            elif q in seconds:
                mark = "*" if on_cpu.get(q) else " "
                cells.append(f"{seconds[q]:>15.2f}{mark}")
            else:
                cells.append(f"{'-':>16}")
        print(f"{'q' + str(q):>6}" + "".join(cells))
    print("  (* = silently ran on the CPU)")

    for theme in ("light", "dark"):
        fig = build(theme)
        suffix = "" if theme == "light" else "_dark"
        path = f"{args.out}{suffix}.png"
        fig.savefig(path, facecolor=fig.get_facecolor())
        print("wrote", path)


if __name__ == "__main__":
    main()
