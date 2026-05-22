#!/usr/bin/env python3
"""Generuje tabele LaTeX dla sprawozdania ZTDB (v2).

Dla kazdej operacji (CREATE/READ/UPDATE/DELETE) generuje dwie tabele:
  - tab_{op}_no_idx.tex  - wyniki BEZ indeksow
  - tab_{op}_idx.tex     - wyniki Z indeksami (Redis = ---)

Format kazdej tabeli (6 kolumn):
  Scenariusz | Skala | PostgreSQL | MySQL | MongoDB | Redis
  - kazdy scenariusz to multirow z 3 skalami (500k, 1M, 10M)
  - wartosci w ms, najlepszy wynik w wierszu pogrubiony

Pliki zapisywane do sprawozdanie/tables/.

Uruchomienie:
    python scripts/_gen_tables_v2.py
"""
import os
import sys
from collections import defaultdict

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_CSV = os.path.join(ROOT, "results", "results_all.csv")
OUT_DIR = os.path.join(ROOT, "sprawozdanie", "tables")
os.makedirs(OUT_DIR, exist_ok=True)

SCALES = [500_000, 1_000_000, 10_000_000]
SCALE_LABEL = {500_000: "500k", 1_000_000: "1M", 10_000_000: "10M"}
DBS = ["PostgreSQL", "MySQL", "MongoDB", "Redis"]
OPERATIONS = ["CREATE", "READ", "UPDATE", "DELETE"]
OP_PL = {
    "CREATE": "CREATE (INSERT)",
    "READ": "READ (SELECT)",
    "UPDATE": "UPDATE",
    "DELETE": "DELETE",
}


def load_df() -> pd.DataFrame:
    df = pd.read_csv(RESULTS_CSV)
    df["Indexed"] = df["Indexed"].astype(str).str.lower().isin(["true", "1", "yes"])
    df["ms"] = df["Average_Time_Seconds"] * 1000.0
    return df


def fmt_ms(v):
    if v is None or (isinstance(v, float) and v != v):
        return None
    if v < 0.5:
        return f"{v:.2f}"
    if v < 100:
        return f"{v:.2f}"
    return f"{v:.1f}"


def gen_table(df: pd.DataFrame, op: str, indexed: bool) -> str:
    sub = df[(df["Operation_Type"] == op) & (df["Indexed"] == indexed)]
    scenarios = sorted(sub["Scenario_Name"].unique())
    if not scenarios:
        return ""

    idx_label = "Z INDEKSAMI" if indexed else "BEZ INDEKSOW"
    caption = (
        f"Czas operacji {OP_PL[op]} [ms] -- "
        + ("z~indeksami" if indexed else "bez indeksow")
        + ". Najlepszy wynik w~wierszu pogrubiony."
        + (" Redis nie posiada mechanizmu indeksow (kolumna `---')." if indexed else "")
    )
    label = f"tab:{op.lower()}_{'idx' if indexed else 'no'}"

    lines = []
    lines.append("\\begin{table}[H]")
    lines.append("\\centering")
    lines.append(f"\\caption{{{caption}}}")
    lines.append(f"\\label{{{label}}}")
    lines.append("\\small")
    lines.append("\\begin{tabular}{@{}llrrrr@{}}")
    lines.append("\\toprule")
    lines.append(
        "\\textbf{Scenariusz} & \\textbf{Skala} & \\textbf{PostgreSQL} & "
        "\\textbf{MySQL} & \\textbf{MongoDB} & \\textbf{Redis} \\\\"
    )
    lines.append("\\midrule")

    for i, scen in enumerate(scenarios):
        if i > 0:
            lines.append("\\midrule")
        scen_escaped = scen.replace("_", "\\_")
        lines.append(f"\\multirow{{3}}{{*}}{{\\texttt{{{scen_escaped}}}}}")
        for k, scale in enumerate(SCALES):
            row_vals = {}
            for db in DBS:
                if indexed and db == "Redis":
                    row_vals[db] = None  # placeholder for "---"
                    continue
                m = (
                    (sub["Database"] == db)
                    & (sub["Scenario_Name"] == scen)
                    & (sub["Scale"] == scale)
                )
                vals = sub.loc[m, "ms"].values
                row_vals[db] = float(vals[0]) if len(vals) else None

            # Find best (min) numeric value among non-None for bolding
            numeric = [(db, v) for db, v in row_vals.items() if v is not None]
            best_db = min(numeric, key=lambda x: x[1])[0] if numeric else None

            cells = []
            for db in DBS:
                v = row_vals[db]
                if v is None:
                    cells.append("---")
                else:
                    s = fmt_ms(v)
                    if db == best_db:
                        s = f"\\textbf{{{s}}}"
                    cells.append(s)

            line = f" & {SCALE_LABEL[scale]} & " + " & ".join(cells) + " \\\\"
            lines.append(line)

    lines.append("\\bottomrule")
    lines.append("\\end{tabular}")
    lines.append("\\end{table}")
    return "\n".join(lines) + "\n"


def main():
    if not os.path.isfile(RESULTS_CSV):
        sys.exit(f"Brak pliku: {RESULTS_CSV}")
    df = load_df()

    for op in OPERATIONS:
        for indexed in [False, True]:
            tex = gen_table(df, op, indexed)
            suffix = "idx" if indexed else "no_idx"
            out = os.path.join(OUT_DIR, f"tab_{op}_{suffix}.tex")
            with open(out, "w", encoding="utf-8") as f:
                f.write(tex)
            print(f"OK  {out}")


if __name__ == "__main__":
    main()
