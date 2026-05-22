#!/usr/bin/env python3
"""Generuje NOWE wykresy v2 dla sprawozdania ZTDB:

(C) crud_pair_{op}_{scale}.png
    6 sluпков per scenariusz (3 indeksowane DB x {bez idx, z idx}).
    Redis pominiety (brak indeksow).
    Generuje 4 op x 3 skale = 12 plikow.

(D) index_impact_pct_{DB}_{op}.png
    Per scenariusz 6 sluпков (3 skale x {bez idx, z idx}) z etykietami
    procentowej zmiany (delta% = (t_idx - t_no)/t_no * 100, ujemne = szybciej).
    Generuje 3 DB x 4 op = 12 plikow.

Uruchomienie:
    python scripts/_gen_charts_v2.py
"""
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_CSV = os.path.join(ROOT, "results", "results_all.csv")
CHARTS_DIR = os.path.join(ROOT, "results", "charts")
os.makedirs(CHARTS_DIR, exist_ok=True)

SCALES = [500_000, 1_000_000, 10_000_000]
INDEXED_DBS = ["PostgreSQL", "MySQL", "MongoDB"]  # Redis pominiety
OPERATIONS = ["CREATE", "READ", "UPDATE", "DELETE"]

DB_COLORS = {
    "PostgreSQL": "#4e79a7",
    "MySQL": "#f28e2b",
    "MongoDB": "#59a14f",
}
NO_IDX_HATCH = ""
IDX_HATCH = "///"
NO_IDX_ALPHA = 0.55
IDX_ALPHA = 0.95

SCALE_COLORS = {
    500_000: "#a0cbe8",
    1_000_000: "#4e79a7",
    10_000_000: "#1f3a5f",
}

plt.rcParams.update({
    "figure.facecolor": "#ffffff",
    "axes.facecolor": "#f8f9fa",
    "font.size": 9,
})


def load_df() -> pd.DataFrame:
    df = pd.read_csv(RESULTS_CSV)
    df["Indexed"] = df["Indexed"].astype(str).str.lower().isin(["true", "1", "yes"])
    df["ms"] = df["Average_Time_Seconds"] * 1000.0
    return df


def fmt_scale(s: int) -> str:
    if s >= 1_000_000:
        return f"{s // 1_000_000}M" if s % 1_000_000 == 0 else f"{s/1_000_000:.1f}M"
    return f"{s // 1_000}k"


def get(df: pd.DataFrame, db: str, scale: int, op: str, scen: str, idx: bool) -> float:
    m = (
        (df["Database"] == db)
        & (df["Scale"] == scale)
        & (df["Operation_Type"] == op)
        & (df["Scenario_Name"] == scen)
        & (df["Indexed"] == idx)
    )
    vals = df.loc[m, "ms"].values
    return float(vals[0]) if len(vals) else float("nan")


# ─────────────────────────────────────────────────────────────────────
# (C) Wykresy per skala: 6 sluпков per scenariusz (3 DB x bez/z idx)
# ─────────────────────────────────────────────────────────────────────
def chart_crud_pair(df: pd.DataFrame, op: str, scale: int):
    df_op = df[(df["Operation_Type"] == op) & (df["Scale"] == scale)]
    if df_op.empty:
        print(f"[skip] crud_pair {op} {scale}: brak danych")
        return
    scenarios = sorted(df_op["Scenario_Name"].unique())
    n_sc = len(scenarios)

    fig, ax = plt.subplots(figsize=(13, 6))

    # 6 sluпков per scenariusz: dla kazdej DB para (bez idx, z idx)
    n_bars_per_sc = len(INDEXED_DBS) * 2
    total_width = 0.85
    bar_w = total_width / n_bars_per_sc

    x = list(range(n_sc))
    for j, db in enumerate(INDEXED_DBS):
        no_vals = [get(df_op, db, scale, op, s, False) for s in scenarios]
        ix_vals = [get(df_op, db, scale, op, s, True) for s in scenarios]
        # Pozycje slup'ow: dla DB j -> dwa slupki obok siebie
        offset_no = (j * 2 - n_bars_per_sc / 2 + 0.5) * bar_w
        offset_ix = (j * 2 + 1 - n_bars_per_sc / 2 + 0.5) * bar_w

        ax.bar(
            [xi + offset_no for xi in x], no_vals, bar_w,
            color=DB_COLORS[db], alpha=NO_IDX_ALPHA,
            edgecolor="black", linewidth=0.4,
            label=f"{db} bez idx",
        )
        ax.bar(
            [xi + offset_ix for xi in x], ix_vals, bar_w,
            color=DB_COLORS[db], alpha=IDX_ALPHA,
            hatch=IDX_HATCH, edgecolor="black", linewidth=0.4,
            label=f"{db} z idx",
        )

        # Etykiety wartosci [ms] (male) tylko gdy znaczace
        for xi, vn, vi in zip(x, no_vals, ix_vals):
            for v, off in [(vn, offset_no), (vi, offset_ix)]:
                if v == v and v > 0:  # not NaN
                    ax.text(
                        xi + off, v, f"{v:.1f}",
                        ha="center", va="bottom",
                        fontsize=5.5, rotation=90,
                    )

    ax.set_title(
        f"Porownanie {op} - skala {scale:,} - 3 indeksowane bazy (bez/z indeksami)",
        fontsize=11, fontweight="bold",
    )
    ax.set_ylabel("Czas [ms]")
    ax.set_xticks(x)
    ax.set_xticklabels(
        [s.replace("_", "\n") for s in scenarios],
        fontsize=7, rotation=20, ha="right",
    )
    ax.legend(fontsize=7, ncol=3, loc="upper right")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    out = os.path.join(CHARTS_DIR, f"crud_pair_{op}_{scale}.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"OK  {out}")


# ─────────────────────────────────────────────────────────────────────
# (D) Wykresy per DB: 6 sluпков per scenariusz (3 skale x bez/z idx)
#     z etykietami ±% nad kazda para
# ─────────────────────────────────────────────────────────────────────
def chart_index_impact_pct(df: pd.DataFrame, db: str, op: str):
    df_db = df[(df["Database"] == db) & (df["Operation_Type"] == op)]
    if df_db.empty:
        print(f"[skip] index_impact_pct {db} {op}: brak danych")
        return
    scenarios = sorted(df_db["Scenario_Name"].unique())
    n_sc = len(scenarios)

    fig, ax = plt.subplots(figsize=(13, 6))

    n_bars_per_sc = len(SCALES) * 2
    total_width = 0.85
    bar_w = total_width / n_bars_per_sc

    x = list(range(n_sc))
    # Dla kazdej skali: para (bez, z) obok siebie
    for j, scale in enumerate(SCALES):
        no_vals = [get(df_db, db, scale, op, s, False) for s in scenarios]
        ix_vals = [get(df_db, db, scale, op, s, True) for s in scenarios]
        col = SCALE_COLORS[scale]

        offset_no = (j * 2 - n_bars_per_sc / 2 + 0.5) * bar_w
        offset_ix = (j * 2 + 1 - n_bars_per_sc / 2 + 0.5) * bar_w

        ax.bar(
            [xi + offset_no for xi in x], no_vals, bar_w,
            color=col, alpha=NO_IDX_ALPHA,
            edgecolor="black", linewidth=0.4,
            label=f"{fmt_scale(scale)} bez idx",
        )
        ax.bar(
            [xi + offset_ix for xi in x], ix_vals, bar_w,
            color=col, alpha=IDX_ALPHA,
            hatch=IDX_HATCH, edgecolor="black", linewidth=0.4,
            label=f"{fmt_scale(scale)} z idx",
        )

        # Etykieta delta% nad para
        for xi, vn, vi in zip(x, no_vals, ix_vals):
            if vn and vn == vn and vi == vi and vn > 0:
                pct = (vi - vn) / vn * 100.0
                top = max(vn, vi)
                ax.text(
                    xi + (offset_no + offset_ix) / 2,
                    top,
                    f"{pct:+.0f}%",
                    ha="center", va="bottom",
                    fontsize=6.5,
                    color="#b30000" if pct > 5 else ("#006400" if pct < -5 else "#555"),
                    fontweight="bold" if abs(pct) >= 50 else "normal",
                )

    ax.set_title(
        f"Wplyw indeksow na {op} - {db} - 3 skale (delta% nad para)",
        fontsize=11, fontweight="bold",
    )
    ax.set_ylabel("Czas [ms]")
    ax.set_xticks(x)
    ax.set_xticklabels(
        [s.replace("_", "\n") for s in scenarios],
        fontsize=7, rotation=20, ha="right",
    )
    ax.legend(fontsize=7, ncol=3, loc="upper right")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    out = os.path.join(CHARTS_DIR, f"index_impact_pct_{db}_{op}.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"OK  {out}")


def main():
    if not os.path.isfile(RESULTS_CSV):
        sys.exit(f"Brak pliku: {RESULTS_CSV}")
    df = load_df()

    # (C) 4 op x 3 skale = 12 plikow
    for op in OPERATIONS:
        for scale in SCALES:
            chart_crud_pair(df, op, scale)

    # (D) 3 DB x 4 op = 12 plikow
    for db in INDEXED_DBS:
        for op in OPERATIONS:
            chart_index_impact_pct(df, db, op)


if __name__ == "__main__":
    main()
