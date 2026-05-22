#!/usr/bin/env python3
"""
scripts/validate_results.py – Walidacja pliku results_all.csv.

Sprawdza:
  - Istnienie pliku
  - Poprawność nagłówka CSV
  - Brak wartości ujemnych w kolumnie Average_Time_Seconds
  - Brak pustych pól
  - Oczekiwane bazy danych (PostgreSQL, MySQL, MongoDB, Redis)
  - Oczekiwane operacje CRUD
  - Oczekiwane 24 scenariusze per baza per skala
  - Scenariusze Redis select_prescriptions_with_meds mają wyniki > 0

Zwraca exit code 0 jeśli wszystko OK, 1 jeśli są błędy.
"""

import csv
import os
import sys

RESULTS_FILE = os.path.join("results", "results_all.csv")

EXPECTED_HEADER = [
    "Database", "Scale", "Indexed",
    "Operation_Type", "Scenario_Name", "Average_Time_Seconds",
    "Median_Time_Seconds", "StdDev_Time_Seconds",
]

EXPECTED_DATABASES = {"PostgreSQL", "MySQL", "MongoDB", "Redis"}
EXPECTED_OPERATIONS = {"CREATE", "READ", "UPDATE", "DELETE"}
EXPECTED_SCENARIOS_PER_OP = 6


def main():
    errors = []
    warnings = []

    if not os.path.isfile(RESULTS_FILE):
        print(f"BLAD: Brak pliku {RESULTS_FILE}")
        sys.exit(1)

    rows = []
    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header != EXPECTED_HEADER:
            errors.append(f"Niepoprawny naglowek: {header!r}\n  Oczekiwano: {EXPECTED_HEADER!r}")
        for i, row in enumerate(reader, start=2):
            if len(row) != len(EXPECTED_HEADER):
                errors.append(f"Linia {i}: niepoprawna liczba kolumn ({len(row)}): {row!r}")
                continue
            if any(cell.strip() == "" for cell in row):
                errors.append(f"Linia {i}: puste pole w wierszu: {row!r}")
            try:
                val = float(row[5])
                if val < 0:
                    errors.append(f"Linia {i}: ujemny czas {val} dla {row[0]}/{row[4]}")
            except ValueError:
                errors.append(f"Linia {i}: nieprawidlowy czas '{row[5]}'")
            # Walidacja mediany (kolumna 6) jeśli obecna
            if len(row) >= 7 and row[6].strip():
                try:
                    med = float(row[6])
                    if med < 0:
                        errors.append(f"Linia {i}: ujemna mediana {med} dla {row[0]}/{row[4]}")
                except ValueError:
                    errors.append(f"Linia {i}: nieprawidlowa mediana '{row[6]}'")
            rows.append(row)

    print(f"Wczytano {len(rows)} wierszy wynikow.")

    databases_found = set(r[0] for r in rows)
    scales_found = set(r[1] for r in rows)
    operations_found = set(r[3] for r in rows)

    missing_dbs = EXPECTED_DATABASES - databases_found
    if missing_dbs:
        errors.append(f"Brakujace bazy danych: {missing_dbs}")

    unexpected_dbs = databases_found - EXPECTED_DATABASES
    if unexpected_dbs:
        warnings.append(f"Nieoczekiwane bazy danych: {unexpected_dbs}")

    missing_ops = EXPECTED_OPERATIONS - operations_found
    if missing_ops:
        errors.append(f"Brakujace operacje CRUD: {missing_ops}")

    print(f"Bazy: {sorted(databases_found)}")
    print(f"Skale: {sorted(scales_found)}")
    print(f"Operacje: {sorted(operations_found)}")

    for db in databases_found:
        for scale in scales_found:
            for indexed in ["True", "False"]:
                subset = [r for r in rows if r[0] == db and r[1] == scale and r[2] == indexed]
                by_op = {}
                for r in subset:
                    by_op.setdefault(r[3], set()).add(r[4])
                for op in EXPECTED_OPERATIONS:
                    count = len(by_op.get(op, set()))
                    if count < EXPECTED_SCENARIOS_PER_OP:
                        errors.append(
                            f"{db} skala={scale} indexed={indexed} {op}: "
                            f"tylko {count}/{EXPECTED_SCENARIOS_PER_OP} scenariuszy"
                        )

    redis_rx = [
        r for r in rows
        if r[0] == "Redis" and r[4] == "select_prescriptions_with_meds"
    ]
    for r in redis_rx:
        val = float(r[5])
        if val <= 0:
            warnings.append(
                f"Redis select_prescriptions_with_meds skala={r[1]}: czas={val:.6f}s (podejrzanie niski)"
            )

    print()
    if warnings:
        print("OSTRZEZENIA:")
        for w in warnings:
            print(f"  [WARN] {w}")
    if errors:
        print("BLEDY:")
        for e in errors:
            print(f"  [ERR ] {e}")
        print(f"\nWalidacja NIEUDANA – {len(errors)} bledow, {len(warnings)} ostrzezen.")
        sys.exit(1)
    else:
        print(f"Walidacja UDANA – {len(rows)} wierszy poprawnych. ({len(warnings)} ostrzezen)")
        sys.exit(0)


if __name__ == "__main__":
    main()
