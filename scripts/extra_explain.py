"""
Generuje pog\u0142\u0119bione raporty EXPLAIN dla najwa\u017cniejszych scenariuszy.

Tryb pracy:
  - Wymaga aktualnego stanu baz danych (po `run_all.py`).
  - Dla PostgreSQL i MySQL: wykonuje EXPLAIN ANALYZE z indeksami, nast\u0119pnie DROP INDEX,
    EXPLAIN ANALYZE bez indeks\u00f3w, nast\u0119pnie CREATE INDEX z powrotem.
  - Dla MongoDB: wykonuje explain() dla zapyta\u0144 z indeksem i bez (drop + create).

Zapisuje do results/explain_extra.txt.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg2
import pymysql
from pymongo import MongoClient


OUT = ROOT / "results" / "explain_extra.txt"


PG_INDEXES = {
    "idx_visits_patient_id": "CREATE INDEX idx_visits_patient_id ON visits(patient_id)",
    "idx_visits_doctor_id": "CREATE INDEX idx_visits_doctor_id ON visits(doctor_id)",
    "idx_diagnoses_visit_id": "CREATE INDEX idx_diagnoses_visit_id ON diagnoses(visit_id)",
    "idx_diagnoses_disease_id": "CREATE INDEX idx_diagnoses_disease_id ON diagnoses(disease_id)",
    "idx_performed_services_visit_id": "CREATE INDEX idx_performed_services_visit_id ON performed_services(visit_id)",
    "idx_prescriptions_visit_id": "CREATE INDEX idx_prescriptions_visit_id ON prescriptions(visit_id)",
}

MYSQL_INDEXES = {
    ("visits", "idx_visits_patient_id"): "ALTER TABLE visits ADD INDEX idx_visits_patient_id(patient_id)",
    ("visits", "idx_visits_doctor_id"): "ALTER TABLE visits ADD INDEX idx_visits_doctor_id(doctor_id)",
    ("diagnoses", "idx_diagnoses_visit_id"): "ALTER TABLE diagnoses ADD INDEX idx_diagnoses_visit_id(visit_id)",
    ("performed_services", "idx_performed_services_visit_id"):
        "ALTER TABLE performed_services ADD INDEX idx_performed_services_visit_id(visit_id)",
}


def hdr(s: str, c: str = "=") -> str:
    return f"\n{c*72}\n  {s}\n{c*72}\n"


def section(s: str) -> str:
    return f"\n{'-'*72}\n  {s}\n{'-'*72}\n"


def explain_pg(out, pid: int = 1, vid: int = 1):
    """PG: EXPLAIN ANALYZE dla kluczowych zapyta\u0144 (z indeksami / bez)."""
    out.append(hdr("PostgreSQL (10M sumarycznych rekord\u00f3w)"))

    conn = psycopg2.connect(host="localhost", port=5432, dbname="hospital_db",
                            user="admin", password="password")
    conn.autocommit = True
    cur = conn.cursor()

    queries = [
        ("select_visit_diagnoses",
         "SELECT dg.diagnosis_type, dg.notes, ds.name FROM diagnoses dg "
         "JOIN diseases ds ON dg.disease_id = ds.id WHERE dg.visit_id = %s",
         (vid,)),
        ("select_aggregated_costs",
         "SELECT v.id, SUM(ps.final_price) AS total FROM visits v "
         "JOIN performed_services ps ON ps.visit_id = v.id "
         "WHERE v.patient_id = %s GROUP BY v.id LIMIT 50",
         (pid,)),
        ("update_service_price (WHERE po FK)",
         "UPDATE performed_services SET final_price = final_price * 1.0 "
         "WHERE visit_id = %s",
         (vid,)),
    ]

    def run_explain(label):
        for name, sql, params in queries:
            cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql, params)
            plan = "\n".join(row[0] for row in cur.fetchall())
            out.append(f"\n> {name}  [{label}]\n  Zapytanie: {sql}\n")
            for line in plan.splitlines():
                out.append("  " + line)
            out.append("")

    # Z indeksami (stan obecny)
    out.append(section("Z INDEKSAMI"))
    run_explain("z indeksami")

    # Bez indeks\u00f3w
    out.append(section("BEZ INDEKS\u00d3W"))
    for name in PG_INDEXES:
        cur.execute(f"DROP INDEX IF EXISTS {name}")
    cur.execute("ANALYZE visits; ANALYZE diagnoses; ANALYZE performed_services;")
    run_explain("bez indeks\u00f3w")

    # Przywr\u00f3\u0107 indeksy
    for name, sql in PG_INDEXES.items():
        cur.execute(sql)
    cur.execute("ANALYZE visits; ANALYZE diagnoses; ANALYZE performed_services;")

    cur.close()
    conn.close()


def explain_mysql(out, pid: int = 1, vid: int = 1):
    """MySQL: EXPLAIN FORMAT=JSON + EXPLAIN ANALYZE dla wybranych zapyta\u0144."""
    out.append(hdr("MySQL 8.0 (10M sumarycznych rekord\u00f3w)"))

    conn = pymysql.connect(host="localhost", port=3306, user="root",
                           password="password", database="hospital_db")
    cur = conn.cursor()

    queries = [
        ("select_visit_diagnoses",
         "SELECT dg.diagnosis_type, dg.notes, ds.name FROM diagnoses dg "
         "JOIN diseases ds ON dg.disease_id = ds.id WHERE dg.visit_id = %s",
         (vid,)),
        ("select_aggregated_costs",
         "SELECT v.id, SUM(ps.final_price) AS total FROM visits v "
         "JOIN performed_services ps ON ps.visit_id = v.id "
         "WHERE v.patient_id = %s GROUP BY v.id LIMIT 50",
         (pid,)),
    ]

    def run_explain(label):
        for name, sql, params in queries:
            cur.execute("EXPLAIN ANALYZE " + sql, params)
            plan = "\n".join(row[0] for row in cur.fetchall())
            out.append(f"\n> {name}  [{label}]  (EXPLAIN ANALYZE)\n  Zapytanie: {sql}\n")
            for line in plan.splitlines():
                out.append("  " + line)
            out.append("")

    out.append(section("Z INDEKSAMI"))
    run_explain("z indeksami")

    out.append(section("BEZ INDEKS\u00d3W"))
    dropped = []
    for (table, idx), sql in MYSQL_INDEXES.items():
        try:
            cur.execute(f"ALTER TABLE {table} DROP INDEX {idx}")
            dropped.append((table, idx, sql))
        except Exception:
            pass
    cur.execute("ANALYZE TABLE visits, diagnoses, performed_services")
    cur.fetchall()
    run_explain("bez indeks\u00f3w")

    for table, idx, sql in dropped:
        cur.execute(sql)
    cur.execute("ANALYZE TABLE visits, diagnoses, performed_services")
    cur.fetchall()

    cur.close()
    conn.close()


def explain_mongo(out):
    """MongoDB: explain() dla select_visits_with_doctor (kluczowy scenariusz)."""
    out.append(hdr("MongoDB 7 (10M sumarycznych rekord\u00f3w)"))

    cli = MongoClient("mongodb://admin:password@localhost:27017")
    db = cli["hospital_db"]
    patients = db["patients"]

    sample_id = patients.find_one(sort=[("_id", 1)])["_id"]

    def run_explain(label):
        pipeline = [
            {"$match": {"_id": sample_id}},
            {"$unwind": "$visits"},
            {"$match": {"visits.doctor_id": {"$exists": True}}},
            {"$limit": 50},
            {"$project": {"_id": 0, "visit_id": "$visits._id",
                          "doctor_id": "$visits.doctor_id",
                          "status": "$visits.status"}},
        ]
        plan = db.command("aggregate", "patients", pipeline=pipeline,
                          explain=True, cursor={})
        stage = plan.get("stages", [{}])[0].get("$cursor", {})
        win = stage.get("queryPlanner", {}).get("winningPlan", {})
        exec_stats = stage.get("executionStats", {})
        out.append(f"\n> select_visits_with_doctor  [{label}]\n")
        out.append(f"  winningPlan.stage: {win.get('stage')}")
        sub = win
        depth = 1
        while "inputStage" in sub:
            sub = sub["inputStage"]
            out.append(f"  {'  '*depth}-> {sub.get('stage')}"
                       + (f"  indexName={sub.get('indexName')}" if sub.get('indexName') else ""))
            depth += 1
        if exec_stats:
            out.append(f"  executionTimeMillis: {exec_stats.get('executionTimeMillis')}")
            out.append(f"  totalKeysExamined: {exec_stats.get('totalKeysExamined')}")
            out.append(f"  totalDocsExamined: {exec_stats.get('totalDocsExamined')}")
        out.append("")

    out.append(section("Z INDEKSAMI"))
    run_explain("z indeksami")

    out.append(section("BEZ INDEKS\u00d3W"))
    idx_dropped = []
    for ix in patients.list_indexes():
        if ix["name"] != "_id_":
            try:
                patients.drop_index(ix["name"])
                idx_dropped.append(ix)
            except Exception:
                pass
    run_explain("bez indeks\u00f3w")

    for ix in idx_dropped:
        keys = list(ix["key"].items())
        opts = {k: v for k, v in ix.items() if k in ("unique", "sparse")}
        patients.create_index(keys, name=ix["name"], **opts)


def main():
    out: list[str] = []
    out.append(hdr("RAPORT EXPLAIN ROZSZERZONY (10M sumarycznych rekord\u00f3w)"))
    out.append("Por\u00f3wnanie plan\u00f3w zapyta\u0144 z indeksami i bez indeks\u00f3w "
               "dla scenariuszy o najwi\u0119kszej r\u00f3\u017cnicy czas\u00f3w.\n")

    explain_pg(out)
    explain_mysql(out)
    explain_mongo(out)

    OUT.write_text("\n".join(out), encoding="utf-8")
    print(f"Zapisano: {OUT}")


if __name__ == "__main__":
    main()
