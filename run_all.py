#!/usr/bin/env python3
"""
run_all.py – Pełna automatyzacja benchmarków dla projektu ZTDB.
Uruchamia generowanie danych, wstawianie, benchmarki (bez/z indeksami)
oraz analizę EXPLAIN dla każdej skali. Na końcu generuje wykresy.

Użycie:
    python run_all.py                     # Domyślne skale: 500000, 1000000, 10000000
    python run_all.py --scales 10000 100000
    python run_all.py --skip-seed         # Pomiń wstawianie, tylko benchmark
    python run_all.py --charts-only       # Tylko wykresy z istniejących wynikow
"""

import argparse
import csv
import gc
import os
import sys
import time

# matplotlib and pandas are heavy; import lazily inside generate_charts()
# to avoid Windows DLL-scan hang on startup.

from core.database import ConnectionManager
from core.generator import DataGenerator
from core.seeder import DatabaseSeeder
from core.benchmark import (
    BenchmarkEngine,
    CSV_HEADER,
    RESULTS_DIR,
    RESULTS_FILE_NO_INDEX,
    RESULTS_FILE_INDEXED,
)


CHARTS_DIR = os.path.join(RESULTS_DIR, "charts")
ALL_RESULTS_FILE = os.path.join(RESULTS_DIR, "results_all.csv")

# Scales >= this threshold use streaming seeding to avoid OOM (Python in-memory lists)
STREAMING_THRESHOLD = 5_000_000
VISIT_CHUNK_SIZE = 500_000


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def merge_csv_files(file_list: list[str], output: str):
    all_rows = []
    for fp in file_list:
        if os.path.isfile(fp):
            with open(fp, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader)  # skip header
                for row in reader:
                    all_rows.append(row)

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for row in all_rows:
            writer.writerow(row)
    log(f"Polaczono wyniki -> {output} ({len(all_rows)} wierszy)")


def generate_charts():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd

    os.makedirs(CHARTS_DIR, exist_ok=True)

    if not os.path.isfile(ALL_RESULTS_FILE):
        log("Brak pliku wynikow – pomijam wykresy.")
        return

    df = pd.read_csv(ALL_RESULTS_FILE)
    if df.empty:
        log("Pusty plik wynikow.")
        return

    df["Indexed"] = df["Indexed"].astype(str).str.lower().isin(["true", "1", "yes"])

    databases = sorted(df["Database"].unique())
    operations = ["CREATE", "READ", "UPDATE", "DELETE"]
    scales = sorted(df["Scale"].unique())

    db_colors = {"PostgreSQL": "#4e79a7", "MySQL": "#f28e2b",
                 "MongoDB": "#59a14f", "Redis": "#e15759"}

    plt.rcParams.update({
        "figure.facecolor": "#ffffff",
        "axes.facecolor": "#f8f9fa",
        "font.size": 9,
    })

    # ── 1) KLUCZOWY: Porównanie scenariuszy PER SCENARIUSZ – wszystkie DB ─
    #       Każdy scenariusz porównywany oddzielnie we wszystkich technologiach
    for scale in scales:
        for is_indexed in [False, True]:
            label = "z_indeksami" if is_indexed else "bez_indeksow"
            df_sub = df[(df["Scale"] == scale) & (df["Indexed"] == is_indexed)]
            if df_sub.empty:
                continue

            fig, axes = plt.subplots(2, 2, figsize=(18, 12))
            idx_label = "z indeksami" if is_indexed else "bez indeksow"
            fig.suptitle(
                f"Porownanie scenariuszy CRUD – skala {scale:,} ({idx_label})",
                fontsize=13, fontweight="bold",
            )

            for i, op in enumerate(operations):
                ax = axes[i // 2][i % 2]
                df_op = df_sub[df_sub["Operation_Type"] == op]
                if df_op.empty:
                    ax.set_visible(False)
                    continue

                scenarios = sorted(df_op["Scenario_Name"].unique())
                x_pos = range(len(scenarios))
                n_db = len(databases)
                width = 0.8 / max(n_db, 1)

                for j, db_name in enumerate(databases):
                    df_db = df_op[df_op["Database"] == db_name]
                    vals = [
                        df_db[df_db["Scenario_Name"] == s]["Average_Time_Seconds"].values[0]
                        if s in df_db["Scenario_Name"].values else 0.0
                        for s in scenarios
                    ]
                    offset = (j - n_db / 2 + 0.5) * width
                    bars = ax.bar(
                        [xi + offset for xi in x_pos], vals, width,
                        label=db_name, color=db_colors.get(db_name, "#999"),
                        alpha=0.85,
                    )
                    for bar, val in zip(bars, vals):
                        if val > 0:
                            ax.text(
                                bar.get_x() + bar.get_width() / 2,
                                bar.get_height(),
                                f"{val*1000:.2f}",
                                ha="center", va="bottom", fontsize=5.5,
                                rotation=90,
                            )

                ax.set_title(op, fontsize=11, fontweight="bold")
                ax.set_ylabel("Czas [s]")
                ax.set_xticks(list(x_pos))
                lbl = [s.replace("_", "\n") for s in scenarios]
                ax.set_xticklabels(lbl, fontsize=6.5, rotation=30, ha="right")
                ax.legend(fontsize=7)
                ax.grid(axis="y", alpha=0.3)

            fig.tight_layout(rect=[0, 0, 1, 0.95])
            path = os.path.join(CHARTS_DIR, f"crud_comparison_{scale}_{label}.png")
            fig.savefig(path, dpi=150)
            plt.close(fig)
            log(f"Wykres -> {path}")

    # ── 2) Wpływ indeksów: bez vs z indeksami (per DB per scale) ──────
    for scale in scales:
        for db_name in databases:
            df_db = df[(df["Scale"] == scale) & (df["Database"] == db_name)]
            if df_db.empty:
                continue

            fig, axes = plt.subplots(2, 2, figsize=(16, 11))
            fig.suptitle(
                f"Wplyw indeksow na scenariusze – {db_name}  (skala {scale:,})",
                fontsize=13, fontweight="bold",
            )

            for i, op in enumerate(operations):
                ax = axes[i // 2][i % 2]
                df_op = df_db[df_db["Operation_Type"] == op]
                if df_op.empty:
                    ax.set_visible(False)
                    continue

                df_no = df_op[~df_op["Indexed"]]
                df_ix = df_op[df_op["Indexed"]]
                scenarios = sorted(df_op["Scenario_Name"].unique())
                x = range(len(scenarios))
                w = 0.38

                vals_no = [
                    df_no[df_no["Scenario_Name"] == s]["Average_Time_Seconds"].values[0]
                    if s in df_no["Scenario_Name"].values else 0.0
                    for s in scenarios
                ]
                vals_ix = [
                    df_ix[df_ix["Scenario_Name"] == s]["Average_Time_Seconds"].values[0]
                    if s in df_ix["Scenario_Name"].values else 0.0
                    for s in scenarios
                ]

                ax.bar([xi - w / 2 for xi in x], vals_no, w,
                       label="Bez indeksow", color="#e15759", alpha=0.85)
                ax.bar([xi + w / 2 for xi in x], vals_ix, w,
                       label="Z indeksami", color="#59a14f", alpha=0.85)

                ax.set_title(op, fontsize=11, fontweight="bold")
                ax.set_ylabel("Czas [s]")
                ax.set_xticks(list(x))
                labels = [s.replace("_", "\n") for s in scenarios]
                ax.set_xticklabels(labels, fontsize=6.5, rotation=30, ha="right")
                ax.legend(fontsize=8)
                ax.grid(axis="y", alpha=0.3)

            fig.tight_layout(rect=[0, 0, 1, 0.95])
            path = os.path.join(CHARTS_DIR, f"index_impact_{db_name}_{scale}.png")
            fig.savefig(path, dpi=150)
            plt.close(fig)
            log(f"Wykres -> {path}")

    # ── 3) Porównanie scenariuszy per baza (skale na jednym wykresie) ──
    for db_name in databases:
        for is_indexed in [False, True]:
            label = "indexed" if is_indexed else "no_index"
            df_db = df[(df["Database"] == db_name) & (df["Indexed"] == is_indexed)]
            if df_db.empty:
                continue

            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            idx_label = "z indeksami" if is_indexed else "bez indeksow"
            fig.suptitle(
                f"Scenariusze CRUD – {db_name} ({idx_label})",
                fontsize=13, fontweight="bold",
            )

            for i, op in enumerate(operations):
                ax = axes[i // 2][i % 2]
                df_op = df_db[df_db["Operation_Type"] == op]
                if df_op.empty:
                    ax.set_visible(False)
                    continue

                scenarios = sorted(df_op["Scenario_Name"].unique())
                x_pos = range(len(scenarios))
                width = 0.25
                scale_colors = ["#4e79a7", "#f28e2b", "#59a14f"]
                for j, scale in enumerate(scales):
                    df_s = df_op[df_op["Scale"] == scale]
                    vals = [
                        df_s[df_s["Scenario_Name"] == s]["Average_Time_Seconds"].values[0]
                        if s in df_s["Scenario_Name"].values else 0
                        for s in scenarios
                    ]
                    offset = (j - len(scales) / 2 + 0.5) * width
                    ax.bar(
                        [xi + offset for xi in x_pos], vals, width,
                        label=f"Skala {scale:,}",
                        color=scale_colors[j % len(scale_colors)],
                        alpha=0.85,
                    )

                ax.set_title(op, fontsize=11, fontweight="bold")
                ax.set_ylabel("Czas [s]")
                ax.set_xticks(list(x_pos))
                lbl = [s.replace("_", "\n") for s in scenarios]
                ax.set_xticklabels(lbl, fontsize=7, rotation=30, ha="right")
                ax.legend(fontsize=7)
                ax.grid(axis="y", alpha=0.3)

            fig.tight_layout(rect=[0, 0, 1, 0.95])
            path = os.path.join(CHARTS_DIR, f"scenarios_{db_name}_{label}.png")
            fig.savefig(path, dpi=150)
            plt.close(fig)
            log(f"Wykres -> {path}")

    # ── 4) Skalowalność – czas vs rozmiar danych (per scenariusz) ─────
    if len(scales) >= 2:
        for is_indexed in [False, True]:
            label = "indexed" if is_indexed else "no_index"
            idx_label = "z indeksami" if is_indexed else "bez indeksow"
            df_sub = df[df["Indexed"] == is_indexed]
            if df_sub.empty:
                continue

            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            fig.suptitle(
                f"Skalowalnosc – wplyw rozmiaru danych na wydajnosc ({idx_label})",
                fontsize=13, fontweight="bold",
            )

            for i, op in enumerate(operations):
                ax = axes[i // 2][i % 2]
                df_op = df_sub[df_sub["Operation_Type"] == op]
                for db_name in databases:
                    df_db_op = df_op[df_op["Database"] == db_name]
                    avg_per_scale = df_db_op.groupby("Scale")["Average_Time_Seconds"].mean()
                    if not avg_per_scale.empty:
                        ax.plot(
                            avg_per_scale.index, avg_per_scale.values,
                            marker="o", label=db_name, linewidth=2,
                            color=db_colors.get(db_name),
                        )

                ax.set_title(op, fontsize=11, fontweight="bold")
                ax.set_xlabel("Liczba rekordow")
                ax.set_ylabel("Sredni czas [s]")
                ax.legend(fontsize=8)
                ax.grid(True, alpha=0.3)

            fig.tight_layout(rect=[0, 0, 1, 0.95])
            path = os.path.join(CHARTS_DIR, f"scalability_{label}.png")
            fig.savefig(path, dpi=150)
            plt.close(fig)
            log(f"Wykres -> {path}")


def _seed_large_scale(
    cm: ConnectionManager, scale: int, seeder: DatabaseSeeder, gen: DataGenerator
):
    """Streaming seeding for scales >= STREAMING_THRESHOLD.
    Generates base tables in memory, then visits+children in VISIT_CHUNK_SIZE chunks.
    MongoDB is built by reading from PostgreSQL after SQL seeding completes.
    Redis is seeded by streaming from PostgreSQL server-side cursors.
    """
    log(f"  [Streaming] Skala {scale:,} – tryb streaming (chunks po {VISIT_CHUNK_SIZE:,}).")

    log("  [Streaming] Generowanie danych bazowych (bez wizyt)...")
    base_data = gen.generate_base_data(seed=42)

    log("  [Streaming] Tworzenie schematu SQL + seedowanie danych bazowych...")
    seeder.seed_sql_schema_and_base(base_data, progress_callback=log)

    total_visits = gen.cfg["visits"]
    log(f"  [Streaming] Start seedowania wizyt: {total_visits:,} wizyt...")
    chunk_num = 0
    seeded_visits = 0
    for chunk in gen.generate_visits_streaming(chunk_size=VISIT_CHUNK_SIZE):
        visits, diagnoses, services, prescriptions, rx_items, test_results = chunk
        chunk_num += 1
        seeded_visits += len(visits)
        log(
            f"  [Streaming] Chunk {chunk_num}: "
            f"{seeded_visits:,}/{total_visits:,} wizyt"
        )
        seeder.seed_sql_visit_chunk(
            visits, diagnoses, services, prescriptions, rx_items, test_results,
            progress_callback=log,
        )
        del visits, diagnoses, services, prescriptions, rx_items, test_results
        gc.collect()

    log("  [Streaming] SQL gotowe. Seedowanie MongoDB z PostgreSQL...")
    seeder.seed_mongo_from_postgres(progress_callback=log)

    log("  [Streaming] Seedowanie Redis (streaming z PostgreSQL)...")
    seeder.seed_redis_streaming(base_data, progress_callback=log)

    log("  [Streaming] Seedowanie zakonczone.")


def run_scale(cm: ConnectionManager, scale: int, skip_seed: bool, redis_only: bool = False):
    log(f"{'='*60}")
    log(f"  SKALA: {scale:,} wizyt")
    log(f"{'='*60}")

    seeder = DatabaseSeeder(cm)
    gen = DataGenerator(scale)

    if not skip_seed:
        if scale >= STREAMING_THRESHOLD:
            if redis_only:
                # SQL+MongoDB already seeded; only re-seed Redis
                log("  [redis-only] Re-seedowanie Redis (streaming z PostgreSQL)...")
                gen2 = DataGenerator(scale)
                base_data = gen2.generate_base_data(seed=42)
                seeder.seed_redis_streaming(base_data, progress_callback=log)
            else:
                _seed_large_scale(cm, scale, seeder, gen)
        else:
            log("Generowanie danych SQL w pamieci...")
            data = gen.generate(progress_callback=log, seed=42)

            log("Czyszczenie baz danych...")
            seeder.clear_all()

            log("[Faza 1/3] Seedowanie PostgreSQL i MySQL...")
            seeder.seed_postgres(data, progress_callback=log)
            seeder.seed_mysql(data, progress_callback=log)

            log("[Faza 2/3] Seedowanie MongoDB (streaming, oszczednosc RAM)...")
            seeder.seed_mongo_streaming(
                DataGenerator._mongo_doc_generator(data),
                progress_callback=log,
            )
            gc.collect()

            log("[Faza 3/3] Budowanie i seedowanie Redis...")
            data.redis_visit_statuses, data.redis_doctor_sessions = (
                DataGenerator._build_redis_data(data)
            )
            seeder.seed_redis_data(data, progress_callback=log)
            del data.redis_visit_statuses, data.redis_doctor_sessions
            data.redis_visit_statuses = []
            data.redis_doctor_sessions = []
            gc.collect()

            log("Seedowanie zakonczone.")
    else:
        log("Pomijam wstawianie danych (--skip-seed).")

    log("Usuwanie indeksow (jesli istnieja)...")
    seeder.drop_indexes(progress_callback=log)

    log("Uruchamianie benchmarku BEZ INDEKSOW...")
    engine = BenchmarkEngine(cm, scale)
    engine.run_benchmarks(is_indexed=False, progress_callback=log)

    log("Tworzenie indeksow...")
    seeder.create_indexes(progress_callback=log)

    log("Uruchamianie benchmarku Z INDEKSAMI...")
    engine2 = BenchmarkEngine(cm, scale)
    engine2.run_benchmarks(is_indexed=True, progress_callback=log)

    log("Generowanie raportow EXPLAIN...")
    engine2.generate_explain(progress_callback=log)

    # Przenieś wyniki tej skali do pliku z nazwą zawierającą skalę
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
        "--scales", nargs="+", type=int, default=[500_000, 1_000_000, 10_000_000],
        help="Skale do przetestowania (domyslnie: 500000 1000000 5000000)",
    )
    parser.add_argument(
        "--skip-seed", action="store_true",
        help="Pomin generowanie i wstawianie danych",
    )
    parser.add_argument(
        "--redis-only", action="store_true",
        help="Tylko seeduj Redis (streaming z PG) i uruchom benchmarki; pomija SQL/Mongo seeding",
    )
    parser.add_argument(
        "--charts-only", action="store_true",
        help="Tylko generuj wykresy z istniejacego results_all.csv",
    )
    args = parser.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)

    if args.charts_only:
        log("Tryb --charts-only: generowanie wykresow...")
        generate_charts()
        return

    log("Sprawdzanie polaczen z bazami danych...")
    cm = ConnectionManager()
    status = cm.ping_all()
    for db_name, ok in status.items():
        symbol = "OK" if ok else "FAIL"
        log(f"  {db_name}: {symbol}")

    failed = [n for n, ok in status.items() if not ok]
    if failed:
        log(f"BLAD: Nie mozna polaczyc z: {', '.join(failed)}")
        log("Upewnij sie, ze kontenery Docker dzialaja:")
        log("  docker compose up -d")
        sys.exit(1)

    log("Wszystkie bazy danych dostepne!")

    all_partial = []
    total_start = time.time()

    for scale in args.scales:
        scale_start = time.time()
        partials = run_scale(cm, scale, args.skip_seed, redis_only=args.redis_only)
        all_partial.extend(partials)
        elapsed = time.time() - scale_start
        log(f"Skala {scale:,} ukonczona w {elapsed:.1f}s")

    merge_csv_files(all_partial, ALL_RESULTS_FILE)

    log("Generowanie wykresow (subprocess, timeout 180s)...")
    import subprocess as _subprocess
    try:
        result = _subprocess.run(
            [sys.executable, __file__, "--charts-only"],
            timeout=180,
            capture_output=True,
            text=True,
        )
        for line in result.stdout.splitlines():
            log(f"  {line}")
        if result.returncode != 0:
            log(f"  WARN: charts subprocess exit code {result.returncode}")
            for line in result.stderr.splitlines()[-5:]:
                log(f"  ERR: {line}")
    except _subprocess.TimeoutExpired:
        log("  WARN: generowanie wykresow przekroczilo timeout 180s – pominieto.")
        log("  Uruchom pozniej: python run_all.py --charts-only")

    total_elapsed = time.time() - total_start
    log(f"{'='*60}")
    log(f"  ZAKONCZONE – laczny czas: {total_elapsed:.1f}s")
    log(f"  Wyniki: {RESULTS_DIR}/")
    log(f"  Wykresy: {CHARTS_DIR}/")
    log(f"{'='*60}")


if __name__ == "__main__":
    main()
