#!/usr/bin/env python3
"""
scripts/analyze_hypothesis.py – Weryfikacja ilościowa hipotezy badawczej H1.

Hipoteza:
  "Indeksy przyspieszają operacje odczytu kosztem wolniejszych zapisów.
   Oba efekty narastają ze skalą danych."

Metryki:
  S_read  = czas_bez_indeksów / czas_z_indeksami   (>1 oznacza przyspieszenie READ)
  K_write = czas_z_indeksami / czas_bez_indeksów   (>1 oznacza spowolnienie WRITE)

Zakres:
  - Bazy: PostgreSQL, MySQL, MongoDB  (Redis pominięty – brak indeksów)
  - Operacje READ: scenariusze z JOINami (R2-R6)
  - Operacje WRITE: CREATE i UPDATE
  - Skale: wszystkie dostępne w results_all.csv

Wyjście:
  - Tabele tekstowe S_read i K_write w formacie Markdown
  - Opcjonalnie: eksport do CSV (--csv)
"""

import argparse
import csv
import os
import sys

RESULTS_FILE = os.path.join("results", "results_all.csv")

READ_SCENARIOS = [
    "select_visits_with_doctor",
    "select_visit_diagnoses",
    "select_patient_full_history",
    "select_aggregated_costs",
    "select_prescriptions_with_meds",
]

WRITE_SCENARIOS_CREATE = [
    "insert_patient",
    "insert_visit",
    "insert_prescription_with_items",
]

WRITE_SCENARIOS_UPDATE = [
    "update_service_price",
    "update_diagnosis_notes",
]

ANALYSIS_DATABASES = ["PostgreSQL", "MySQL", "MongoDB"]


def load_results(path: str) -> list[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # Używaj mediany jako głównej metryki (odporna na outliers)
                # z fallbackiem na średnią arytmetyczną
                med = row.get("Median_Time_Seconds", "").strip()
                avg = row.get("Average_Time_Seconds", "").strip()
                row["time"] = float(med) if med else float(avg)
            except (ValueError, KeyError):
                continue
            rows.append(row)
    return rows


def build_index(rows: list[dict]) -> dict:
    idx = {}
    for r in rows:
        key = (r["Database"], r["Scale"], r["Indexed"], r["Scenario_Name"])
        idx[key] = r["time"]
    return idx


def compute_ratio(idx: dict, db: str, scale: str, scenario: str, numerator_indexed: bool) -> float | None:
    with_idx = idx.get((db, scale, "True", scenario))
    without_idx = idx.get((db, scale, "False", scenario))
    if with_idx is None or without_idx is None:
        return None
    if numerator_indexed:
        if without_idx == 0:
            return None
        return with_idx / without_idx
    else:
        if with_idx == 0:
            return None
        return without_idx / with_idx


def print_table(title: str, scenarios: list[str], rows: list[dict], idx: dict,
                numerator_indexed: bool, scales: list[str]):
    print(f"\n## {title}\n")
    header = ["Scenariusz", "Baza"] + [f"{int(s):,}" for s in scales]
    print("| " + " | ".join(header) + " |")
    print("| " + " | ".join(["---"] * len(header)) + " |")
    for scenario in scenarios:
        for db in ANALYSIS_DATABASES:
            vals = []
            for scale in scales:
                ratio = compute_ratio(idx, db, scale, scenario, numerator_indexed)
                if ratio is None:
                    vals.append("n/d")
                else:
                    vals.append(f"{ratio:.2f}x")
            print(f"| {scenario} | {db} | " + " | ".join(vals) + " |")


def export_csv(path: str, scenarios: list[str], idx: dict,
               numerator_indexed: bool, scales: list[str], label: str):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "scenario", "database"] + scales)
        for scenario in scenarios:
            for db in ANALYSIS_DATABASES:
                vals = []
                for scale in scales:
                    ratio = compute_ratio(idx, db, scale, scenario, numerator_indexed)
                    vals.append(f"{ratio:.4f}" if ratio is not None else "")
                writer.writerow([label, scenario, db] + vals)
    print(f"Wyeksportowano: {path}")


def main():
    parser = argparse.ArgumentParser(description="Analiza hipotezy H1: wpływ indeksów")
    parser.add_argument("--file", default=RESULTS_FILE, help="Ścieżka do results_all.csv")
    parser.add_argument("--csv", action="store_true", help="Eksportuj wyniki do CSV")
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        print(f"BLAD: Brak pliku {args.file}")
        sys.exit(1)

    rows = load_results(args.file)
    idx = build_index(rows)

    scales = sorted(set(r["Scale"] for r in rows), key=lambda x: int(x))
    print(f"Zaladowano {len(rows)} wierszy. Skale: {[f'{int(s):,}' for s in scales]}")

    print("\n" + "=" * 70)
    print("WERYFIKACJA HIPOTEZY H1")
    print("H1: Indeksy przyspieszają READ kosztem wolniejszych WRITE.")
    print("    Oba efekty narastają ze skalą danych.")
    print("=" * 70)
    print("\nMETRYKI:")
    print("  S_read  = czas_BEZ / czas_Z  (>1 = przyspieszenie READ z indeksami)")
    print("  K_write = czas_Z / czas_BEZ  (>1 = spowolnienie WRITE z indeksami)")
    print("  Redis pominięty – brak mechanizmu indeksów.")

    print_table(
        "S_read — przyspieszenie operacji READ dzięki indeksom",
        READ_SCENARIOS, rows, idx,
        numerator_indexed=False,
        scales=scales,
    )

    print_table(
        "K_write (CREATE) — spowolnienie operacji CREATE z indeksami",
        WRITE_SCENARIOS_CREATE, rows, idx,
        numerator_indexed=True,
        scales=scales,
    )

    print_table(
        "K_write (UPDATE) — spowolnienie operacji UPDATE z indeksami",
        WRITE_SCENARIOS_UPDATE, rows, idx,
        numerator_indexed=True,
        scales=scales,
    )

    print("\n## Interpretacja trendu ze skalą\n")
    for scenario in READ_SCENARIOS[:3]:
        print(f"  {scenario}:")
        for db in ANALYSIS_DATABASES:
            vals = []
            for scale in scales:
                ratio = compute_ratio(idx, db, scale, scenario, numerator_indexed=False)
                vals.append(f"{ratio:.2f}x" if ratio is not None else "n/d")
            trend = " -> ".join(vals)
            print(f"    {db:12s}: {trend}")

    if args.csv:
        os.makedirs("results", exist_ok=True)
        export_csv(
            "results/hypothesis_S_read.csv",
            READ_SCENARIOS, idx, numerator_indexed=False, scales=scales, label="S_read",
        )
        export_csv(
            "results/hypothesis_K_write.csv",
            WRITE_SCENARIOS_CREATE + WRITE_SCENARIOS_UPDATE, idx,
            numerator_indexed=True, scales=scales, label="K_write",
        )

    print()


if __name__ == "__main__":
    main()
