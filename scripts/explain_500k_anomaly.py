"""
Wyja\u015bnia anomali\u0119 select_aggregated_costs @ skala 500k w PostgreSQL.

Reseeduje PG do skali 500k (lokalna operacja, ~30s) i wykonuje EXPLAIN ANALYZE
zapytania select_aggregated_costs zar\u00f3wno z indeksami jak i bez.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.generator import DataGenerator
from core.seeder import DatabaseSeeder
from core.database import ConnectionManager

import psycopg2

OUT = ROOT / "results" / "explain_aggregated_500k.txt"


def main():
    print("Generowanie danych @ 500k sumarycznych...")
    t0 = time.time()
    data = DataGenerator(500_000).generate()
    print(f"  done in {time.time()-t0:.1f}s")

    print("Seedowanie PostgreSQL...")
    t0 = time.time()
    cm = ConnectionManager()
    seeder = DatabaseSeeder(cm)
    seeder.clear_all()
    seeder.drop_indexes()
    seeder.seed_postgres(data)
    print(f"  done in {time.time()-t0:.1f}s")

    conn = psycopg2.connect(host="localhost", port=5432, dbname="hospital_db",
                            user="admin", password="password")
    conn.autocommit = True
    cur = conn.cursor()

    out: list[str] = []
    out.append("=" * 76)
    out.append("  EXPLAIN ANALYZE: select_aggregated_costs @ skala 500k")
    out.append("=" * 76)

    sql = ("SELECT v.id, SUM(ps.final_price) AS total FROM visits v "
           "JOIN performed_services ps ON ps.visit_id = v.id "
           "WHERE v.patient_id = %s GROUP BY v.id LIMIT 50")

    cur.execute("SELECT MIN(id) FROM patients")
    pid = cur.fetchone()[0]

    def run(label):
        cur.execute("ANALYZE visits; ANALYZE performed_services;")
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {sql}", (pid,))
        plan = "\n".join(row[0] for row in cur.fetchall())
        out.append("")
        out.append("-" * 76)
        out.append(f"  {label}")
        out.append("-" * 76)
        out.append(f"Zapytanie: {sql}  (patient_id={pid})")
        out.append("")
        for line in plan.splitlines():
            out.append("  " + line)

    # 1) Bez indeks\u00f3w (pocz\u0105tkowy stan po reseedowaniu)
    run("BEZ INDEKS\u00d3W (czysta baza, brak indeks\u00f3w)")

    # 2) Z indeksami
    seeder.create_indexes()
    run("Z INDEKSAMI")

    # Tabela rozmiar\u00f3w
    cur.execute("""
        SELECT relname AS table_name,
               pg_size_pretty(pg_relation_size(relid)) AS size,
               pg_relation_size(relid) AS size_bytes
        FROM pg_catalog.pg_statio_user_tables
        WHERE relname IN ('visits', 'performed_services')
        ORDER BY relname
    """)
    out.append("")
    out.append("-" * 76)
    out.append("  Rozmiar tabel (relacja, bez indeks\u00f3w):")
    out.append("-" * 76)
    for row in cur.fetchall():
        out.append(f"  {row[0]:25s} {row[1]:>15s}  ({row[2]} bajt\u00f3w)")

    cur.execute("SHOW shared_buffers")
    out.append(f"\n  shared_buffers = {cur.fetchone()[0]}")

    cur.execute("""
        SELECT count(*) FROM visits;
    """)
    out.append(f"  visits rows  = {cur.fetchone()[0]}")
    cur.execute("SELECT count(*) FROM performed_services;")
    out.append(f"  performed_services rows = {cur.fetchone()[0]}")

    cur.close()
    conn.close()
    cm.close_all()

    OUT.write_text("\n".join(out), encoding="utf-8")
    print(f"\nZapisano: {OUT}")


if __name__ == "__main__":
    main()
