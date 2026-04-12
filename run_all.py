#!/usr/bin/env python3
"""
run_all.py – Pełna automatyzacja benchmarków dla projektu ZTDB.
Uruchamia generowanie danych, wstawianie, benchmarki (bez/z indeksami)
oraz analizę EXPLAIN dla każdej skali. Na końcu generuje wykresy.

Użycie:
    python run_all.py                     # Domyślne skale: 10000, 100000, 500000
    python run_all.py --scales 10000 100000
    python run_all.py --skip-seed         # Pomiń wstawianie, tylko benchmark
"""

import argparse
import csv
import os
import sys
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from core.database import ConnectionManager
from core.generator import DataGenerator
from core.seeder import DatabaseSeeder
from core.benchmark import (
    BenchmarkEngine,
    RESULTS_DIR,
    RESULTS_FILE_NO_INDEX,
    RESULTS_FILE_INDEXED,
)


CHARTS_DIR = os.path.join(RESULTS_DIR, "charts")
ALL_RESULTS_FILE = os.path.join(RESULTS_DIR, "results_all.csv")
HEADER = ["Database", "Scale", "Operation_Type", "Scenario_Name", "Average_Time_Seconds"]


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def merge_csv_files(file_list: list[str], output: str):
    all_rows = []
    for fp in file_list:
        if os.path.isfile(fp):
            with open(fp, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader)
                for row in reader:
                    all_rows.append(row)

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        for row in all_rows:
            writer.writerow(row)
    log(f"Polaczono wyniki -> {output} ({len(all_rows)} wierszy)")


def generate_charts():
    os.makedirs(CHARTS_DIR, exist_ok=True)

    if not os.path.isfile(ALL_RESULTS_FILE):
        log("Brak pliku wyników – pomijam wykresy.")
        return

    df = pd.read_csv(ALL_RESULTS_FILE)
    if df.empty:
        log("Pusty plik wyników.")
        return

    databases = df["Database"].unique()
    operations = ["CREATE", "READ", "UPDATE", "DELETE"]
    scales = sorted(df["Scale"].unique())

    plt.rcParams.update({
        "figure.facecolor": "#ffffff",
        "axes.facecolor": "#f8f9fa",
        "font.size": 9,
    })

    # 1) Porównanie baz danych dla każdej operacji (uśrednione po scenariuszach)
    for scale in scales:
        df_scale = df[df["Scale"] == scale]
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(f"Porównanie wydajności CRUD – skala {scale:,}", fontsize=14, fontweight="bold")

        for i, op in enumerate(operations):
            ax = axes[i // 2][i % 2]
            df_op = df_scale[df_scale["Operation_Type"] == op]
            if df_op.empty:
                ax.set_visible(False)
                continue

            avg_by_db = df_op.groupby("Database")["Average_Time_Seconds"].mean()
            colors = ["#4e79a7", "#f28e2b", "#59a14f", "#e15759"]
            bars = ax.bar(avg_by_db.index, avg_by_db.values, color=colors[:len(avg_by_db)])
            ax.set_title(op, fontsize=12, fontweight="bold")
            ax.set_ylabel("Średni czas [s]")
            for bar, val in zip(bars, avg_by_db.values):
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                        f"{val:.6f}", ha="center", va="bottom", fontsize=7)

        fig.tight_layout(rect=[0, 0, 1, 0.95])
        path = os.path.join(CHARTS_DIR, f"crud_comparison_{scale}.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        log(f"Wykres -> {path}")

    # 2) Porównanie scenariuszy per baza danych
    for db_name in databases:
        df_db = df[df["Database"] == db_name]
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle(f"Scenariusze CRUD – {db_name}", fontsize=14, fontweight="bold")

        for i, op in enumerate(operations):
            ax = axes[i // 2][i % 2]
            df_op = df_db[df_db["Operation_Type"] == op]
            if df_op.empty:
                ax.set_visible(False)
                continue

            scenarios = df_op["Scenario_Name"].unique()
            x_pos = range(len(scenarios))
            width = 0.25
            for j, scale in enumerate(scales):
                df_s = df_op[df_op["Scale"] == scale]
                vals = [
                    df_s[df_s["Scenario_Name"] == s]["Average_Time_Seconds"].values[0]
                    if s in df_s["Scenario_Name"].values else 0
                    for s in scenarios
                ]
                offset = (j - len(scales) / 2 + 0.5) * width
                ax.bar([xi + offset for xi in x_pos], vals, width,
                       label=f"Skala {scale:,}")

            ax.set_title(op, fontsize=11, fontweight="bold")
            ax.set_ylabel("Czas [s]")
            ax.set_xticks(list(x_pos))
            labels = [s.replace("_", "\n") for s in scenarios]
            ax.set_xticklabels(labels, fontsize=7, rotation=30, ha="right")
            ax.legend(fontsize=7)

        fig.tight_layout(rect=[0, 0, 1, 0.95])
        path = os.path.join(CHARTS_DIR, f"scenarios_{db_name}.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        log(f"Wykres -> {path}")

    # 3) Skalowalność – czas vs rozmiar danych (hipoteza badawcza)
    if len(scales) >= 2:
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle("Skalowalność – wpływ rozmiaru danych na wydajność", fontsize=14, fontweight="bold")

        for i, op in enumerate(operations):
            ax = axes[i // 2][i % 2]
            df_op = df[df["Operation_Type"] == op]

            for db_name in databases:
                df_db_op = df_op[df_op["Database"] == db_name]
                avg_per_scale = df_db_op.groupby("Scale")["Average_Time_Seconds"].mean()
                if not avg_per_scale.empty:
                    ax.plot(avg_per_scale.index, avg_per_scale.values,
                            marker="o", label=db_name, linewidth=2)

            ax.set_title(op, fontsize=11, fontweight="bold")
            ax.set_xlabel("Liczba rekordów")
            ax.set_ylabel("Średni czas [s]")
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)

        fig.tight_layout(rect=[0, 0, 1, 0.95])
        path = os.path.join(CHARTS_DIR, "scalability.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        log(f"Wykres -> {path}")


def run_scale(cm: ConnectionManager, scale: int, skip_seed: bool):
    log(f"{'='*60}")
    log(f"  SKALA: {scale:,} wizyt")
    log(f"{'='*60}")

    if not skip_seed:
        log("Generowanie danych w pamięci...")
        gen = DataGenerator(scale)
        data = gen.generate(progress_callback=log)

        log("Wstawianie danych do 4 baz...")
        seeder = DatabaseSeeder(cm)
        seeder.seed_all(data, progress_callback=log)
    else:
        log("Pomijam wstawianie danych (--skip-seed).")

    seeder = DatabaseSeeder(cm)

    log("Usuwanie indeksów (jeśli istnieją)...")
    seeder.drop_indexes(progress_callback=log)

    log("Uruchamianie benchmarku BEZ INDEKSÓW...")
    engine = BenchmarkEngine(cm, scale)
    engine.run_benchmarks(is_indexed=False, progress_callback=log)

    log("Tworzenie indeksów...")
    seeder.create_indexes(progress_callback=log)

    log("Uruchamianie benchmarku Z INDEKSAMI...")
    engine2 = BenchmarkEngine(cm, scale)
    engine2.run_benchmarks(is_indexed=True, progress_callback=log)

    log("Generowanie raportów EXPLAIN...")
    engine2.generate_explain(progress_callback=log)

    # Zbierz wyniki tej skali do pliku zbiorczego
    partial_files = []
    for fname in [RESULTS_FILE_NO_INDEX, RESULTS_FILE_INDEXED]:
        if os.path.isfile(fname):
            scale_fname = fname.replace(".csv", f"_{scale}.csv")
            os.replace(fname, scale_fname)
            partial_files.append(scale_fname)
            log(f"Przeniesiono: {fname} -> {scale_fname}")

    return partial_files


def main():
    parser = argparse.ArgumentParser(description="ZTDB – Automatyczne benchmarki baz danych")
    parser.add_argument(
        "--scales", nargs="+", type=int, default=[10_000, 100_000, 500_000],
        help="Skale do przetestowania (domyślnie: 10000 100000 500000)",
    )
    parser.add_argument("--skip-seed", action="store_true", help="Pomiń generowanie i wstawianie danych")
    args = parser.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)

    log("Sprawdzanie połączeń z bazami danych...")
    cm = ConnectionManager()
    status = cm.ping_all()
    for db_name, ok in status.items():
        symbol = "OK" if ok else "FAIL"
        log(f"  {db_name}: {symbol}")

    failed = [n for n, ok in status.items() if not ok]
    if failed:
        log(f"BŁĄD: Nie można połączyć z: {', '.join(failed)}")
        log("Upewnij się, że kontenery Docker działają:")
        log("  docker compose up -d")
        sys.exit(1)

    log("Wszystkie bazy danych dostępne!")

    all_partial = []
    total_start = time.time()

    for scale in args.scales:
        scale_start = time.time()
        partials = run_scale(cm, scale, args.skip_seed)
        all_partial.extend(partials)
        elapsed = time.time() - scale_start
        log(f"Skala {scale:,} ukończona w {elapsed:.1f}s")

    merge_csv_files(all_partial, ALL_RESULTS_FILE)

    log("Generowanie wykresów...")
    generate_charts()

    total_elapsed = time.time() - total_start
    log(f"{'='*60}")
    log(f"  ZAKOŃCZONO – łączny czas: {total_elapsed:.1f}s")
    log(f"  Wyniki: {RESULTS_DIR}/")
    log(f"  Wykresy: {CHARTS_DIR}/")
    log(f"{'='*60}")


if __name__ == "__main__":
    main()
