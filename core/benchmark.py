"""
core/benchmark.py – 24 scenariusze testowe CRUD x 4 bazy danych.
Każdy scenariusz wykonywany RUNS razy, raportowana jest średnia.
Wyniki zapisywane do CSV; EXPLAIN do osobnego pliku tekstowego.
"""

import csv
import json
import os
import random
import time
from typing import Callable, Optional

from core.database import ConnectionManager, DatabaseType
from core.generator import SCALE_MAP

RESULTS_DIR = "results"
RESULTS_FILE_NO_INDEX = os.path.join(RESULTS_DIR, "results_no_index.csv")
RESULTS_FILE_INDEXED = os.path.join(RESULTS_DIR, "results_indexed.csv")
EXPLAIN_FILE = os.path.join(RESULTS_DIR, "explain_report.txt")
RUNS = 3

ProgressCallback = Optional[Callable[[str], None]]


def _ensure_results_dir():
    os.makedirs(RESULTS_DIR, exist_ok=True)


class BenchmarkEngine:
    """Silnik benchmarków – 24 scenariusze CRUD dla każdej bazy danych."""

    def __init__(self, connection_manager: ConnectionManager, scale: int):
        self.cm = connection_manager
        self.scale = scale
        self.results: list[tuple] = []
        cfg = SCALE_MAP.get(scale, {})
        self.max_patient = cfg.get("patients", max(1, scale // 5))
        self.max_doctor = cfg.get("doctors", max(1, scale // 200))
        self.max_visit = cfg.get("visits", max(1, scale))
        self.max_disease = cfg.get("diseases", 100)
        self.max_service = cfg.get("medical_services", 50)
        self.max_medication = cfg.get("medications", 80)
        self.max_department = cfg.get("departments", 10)

    def _rid(self, max_id: int) -> int:
        return random.randint(1, max(1, max_id))

    @staticmethod
    def _avg_time(func, runs: int = RUNS) -> float:
        times = []
        for _ in range(runs):
            start = time.perf_counter()
            func()
            elapsed = time.perf_counter() - start
            times.append(elapsed)
        return sum(times) / len(times)

    def _record(self, db_name: str, op: str, scenario: str, avg: float):
        self.results.append((db_name, self.scale, op, scenario, avg))

    # ═══════════════════════════════════════════════════════════════════
    #  Publiczne API
    # ═══════════════════════════════════════════════════════════════════

    def run_benchmarks(
        self, is_indexed: bool, progress_callback: ProgressCallback = None
    ):
        _ensure_results_dir()
        self.results = []

        def _report(msg):
            if progress_callback:
                progress_callback(msg)

        for db_type, runner in [
            (DatabaseType.POSTGRES, self._sql_scenarios),
            (DatabaseType.MYSQL, self._sql_scenarios),
            (DatabaseType.MONGODB, self._mongo_scenarios),
            (DatabaseType.REDIS, self._redis_scenarios),
        ]:
            _report(f"=> {db_type.value} benchmarks...")
            try:
                if db_type in (DatabaseType.POSTGRES, DatabaseType.MYSQL):
                    runner(db_type)
                else:
                    runner()
            except Exception as e:
                _report(f"   BŁĄD {db_type.value}: {e}")

        filename = RESULTS_FILE_INDEXED if is_indexed else RESULTS_FILE_NO_INDEX
        self._save_results(filename)
        _report(f"Zapisano wyniki -> {filename}")
        return self.results

    def generate_explain(self, progress_callback: ProgressCallback = None):
        _ensure_results_dir()

        def _report(msg):
            if progress_callback:
                progress_callback(msg)

        _report("Generowanie analizy EXPLAIN...")

        with open(EXPLAIN_FILE, "w", encoding="utf-8") as f:
            f.write(f"{'='*70}\n")
            f.write(f"  RAPORT EXPLAIN / QUERY PLAN   (Skala: {self.scale})\n")
            f.write(f"{'='*70}\n\n")

            SEP = '-' * 70

            queries_pg = [
                (
                    "SELECT z JOINem – wizyty pacjenta z danymi lekarza",
                    "EXPLAIN ANALYZE SELECT v.id, v.visit_date, v.status, d.first_name, d.last_name "
                    "FROM visits v JOIN doctors d ON v.doctor_id = d.id "
                    "WHERE v.patient_id = 1",
                ),
                (
                    "SELECT z agregacją – suma kosztów usług na wizytę",
                    "EXPLAIN ANALYZE SELECT v.id, SUM(ps.final_price) AS total "
                    "FROM visits v JOIN performed_services ps ON ps.visit_id = v.id "
                    "WHERE v.patient_id = 1 GROUP BY v.id",
                ),
                (
                    "SELECT z wieloma JOINami – pełna historia pacjenta",
                    "EXPLAIN ANALYZE SELECT p.first_name, p.last_name, v.visit_date, "
                    "dg.diagnosis_type, ds.name AS disease "
                    "FROM patients p "
                    "JOIN visits v ON v.patient_id = p.id "
                    "JOIN diagnoses dg ON dg.visit_id = v.id "
                    "JOIN diseases ds ON ds.id = dg.disease_id "
                    "WHERE p.id = 1",
                ),
                (
                    "SELECT z filtrowaniem po statusie",
                    "EXPLAIN ANALYZE SELECT * FROM visits WHERE status = 'completed' LIMIT 100",
                ),
            ]

            # PostgreSQL EXPLAIN
            f.write(f"\n{SEP}\n  PostgreSQL\n{SEP}\n")
            try:
                pg = self.cm.get_connector(DatabaseType.POSTGRES).get_connection()
                cur = pg.cursor()
                for title, query in queries_pg:
                    f.write(f"\n> {title}\n  Zapytanie: {query.replace('EXPLAIN ANALYZE ', '')}\n\n")
                    try:
                        cur.execute(query)
                        for row in cur.fetchall():
                            f.write(f"  {row[0]}\n")
                    except Exception as e:
                        f.write(f"  BŁĄD: {e}\n")
                cur.close()
            except Exception as e:
                f.write(f"  Nie można połączyć: {e}\n")

            # MySQL EXPLAIN
            f.write(f"\n{SEP}\n  MySQL\n{SEP}\n")
            try:
                my = self.cm.get_connector(DatabaseType.MYSQL).get_connection()
                cur = my.cursor()
                for title, query in queries_pg:
                    plain = query.replace("EXPLAIN ANALYZE ", "")
                    mysql_q = f"EXPLAIN FORMAT=JSON {plain}"
                    f.write(f"\n> {title}\n  Zapytanie: {plain}\n\n")
                    try:
                        cur.execute(mysql_q)
                        row = cur.fetchone()
                        if row:
                            parsed = json.loads(row[0])
                            f.write(f"  {json.dumps(parsed, indent=2)}\n")
                    except Exception as e:
                        f.write(f"  BŁĄD: {e}\n")
                cur.close()
            except Exception as e:
                f.write(f"  Nie można połączyć: {e}\n")

            # MongoDB EXPLAIN
            f.write(f"\n{SEP}\n  MongoDB\n{SEP}\n")
            try:
                db = self.cm.get_connector(DatabaseType.MONGODB).get_db()

                mongo_queries = [
                    (
                        "find po _id",
                        {"find": "patients", "filter": {"_id": 1}},
                    ),
                    (
                        "find po statusie wizyty (zagnieżdżenie)",
                        {"find": "patients", "filter": {"visits.status": "completed"}},
                    ),
                    (
                        "find po nazwisku z projekcją",
                        {
                            "find": "patients",
                            "filter": {"last_name": "Kowalski"},
                            "projection": {"first_name": 1, "last_name": 1, "visits.visit_date": 1},
                        },
                    ),
                ]
                for title, cmd in mongo_queries:
                    f.write(f"\n> {title}\n  Zapytanie: {cmd}\n\n")
                    try:
                        plan = db.command(
                            "explain", cmd, verbosity="executionStats"
                        )
                        stats = plan.get("executionStats", {})
                        f.write(f"  executionSuccess: {stats.get('executionSuccess')}\n")
                        f.write(f"  nReturned: {stats.get('nReturned')}\n")
                        f.write(f"  executionTimeMillis: {stats.get('executionTimeMillis')}\n")
                        f.write(f"  totalKeysExamined: {stats.get('totalKeysExamined')}\n")
                        f.write(f"  totalDocsExamined: {stats.get('totalDocsExamined')}\n")
                        stage = stats.get("executionStages", stats.get("inputStage", {}))
                        f.write(f"  stage: {stage.get('stage', 'N/A')}\n")
                    except Exception as e:
                        f.write(f"  BŁĄD: {e}\n")
            except Exception as e:
                f.write(f"  Nie można połączyć: {e}\n")

            f.write(f"\n{SEP}\n  Redis\n{SEP}\n")
            f.write("  Redis jest bazą klucz-wartość i nie posiada mechanizmu EXPLAIN.\n")
            f.write("  Złożoność operacji: GET/SET → O(1), HGETALL → O(N), SCAN → O(N).\n")

        _report(f"Raport EXPLAIN -> {EXPLAIN_FILE}")

    # ═══════════════════════════════════════════════════════════════════
    #  Scenariusze SQL (PostgreSQL / MySQL) – 24 scenariusze
    # ═══════════════════════════════════════════════════════════════════

    def _sql_scenarios(self, db_type: DatabaseType):
        db_name = db_type.value
        conn = self.cm.get_connector(db_type).get_connection()

        def _run(op: str, name: str, func):
            avg = self._avg_time(func)
            self._record(db_name, op, name, avg)

        # ── CREATE (6 scenariuszy) ──────────────────────────────────

        def c1():
            cur = conn.cursor()
            pid = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO patients (id, national_id, first_name, last_name, birth_date, gender) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (pid, "99999999999", "TestName", "TestSurname", "2000-01-01", "M"),
            )
            cur.execute("DELETE FROM patients WHERE id=%s", (pid,))
            cur.close()

        _run("CREATE", "insert_patient", c1)

        def c2():
            cur = conn.cursor()
            vid = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO visits (id, patient_id, doctor_id, visit_date, status) "
                "VALUES (%s,%s,%s,%s,%s)",
                (vid, self._rid(self.max_patient), self._rid(self.max_doctor), "2025-06-01", "scheduled"),
            )
            cur.execute("DELETE FROM visits WHERE id=%s", (vid,))
            cur.close()

        _run("CREATE", "insert_visit", c2)

        def c3():
            cur = conn.cursor()
            did = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO diagnoses (id, visit_id, disease_id, diagnosis_type, notes) "
                "VALUES (%s,%s,%s,%s,%s)",
                (did, self._rid(self.max_visit), self._rid(self.max_disease), "primary", "bench note"),
            )
            cur.execute("DELETE FROM diagnoses WHERE id=%s", (did,))
            cur.close()

        _run("CREATE", "insert_diagnosis", c3)

        def c4():
            cur = conn.cursor()
            pid = random.randint(10_000_000, 99_999_999)
            iid = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO prescriptions (id, visit_id, prescription_code, issue_date) "
                "VALUES (%s,%s,%s,%s)",
                (pid, self._rid(self.max_visit), "RX-BENCH", "2025-06-01"),
            )
            cur.execute(
                "INSERT INTO prescription_items (id, prescription_id, medication_id, dosage) "
                "VALUES (%s,%s,%s,%s)",
                (iid, pid, self._rid(self.max_medication), "1x500mg"),
            )
            cur.execute("DELETE FROM prescription_items WHERE id=%s", (iid,))
            cur.execute("DELETE FROM prescriptions WHERE id=%s", (pid,))
            cur.close()

        _run("CREATE", "insert_prescription_with_items", c4)

        def c5():
            cur = conn.cursor()
            sid = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO performed_services (id, visit_id, service_id, quantity, final_price) "
                "VALUES (%s,%s,%s,%s,%s)",
                (sid, self._rid(self.max_visit), self._rid(self.max_service), 1, 199.99),
            )
            cur.execute("DELETE FROM performed_services WHERE id=%s", (sid,))
            cur.close()

        _run("CREATE", "insert_performed_service", c5)

        def c6():
            cur = conn.cursor()
            tid = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO test_results (id, visit_id, parameter_name, result_value, unit, min_norm, max_norm) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (tid, self._rid(self.max_visit), "Hemoglobina", 13.5, "g/dL", 12.0, 16.0),
            )
            cur.execute("DELETE FROM test_results WHERE id=%s", (tid,))
            cur.close()

        _run("CREATE", "insert_test_result", c6)

        # ── READ (6 scenariuszy) ────────────────────────────────────

        def r1():
            cur = conn.cursor()
            cur.execute("SELECT * FROM patients WHERE id = %s", (self._rid(self.max_patient),))
            cur.fetchall()
            cur.close()

        _run("READ", "select_patient_by_id", r1)

        def r2():
            cur = conn.cursor()
            cur.execute(
                "SELECT v.id, v.visit_date, v.status, d.first_name, d.last_name "
                "FROM visits v JOIN doctors d ON v.doctor_id = d.id "
                "WHERE v.patient_id = %s LIMIT 50",
                (self._rid(self.max_patient),),
            )
            cur.fetchall()
            cur.close()

        _run("READ", "select_visits_with_doctor", r2)

        def r3():
            cur = conn.cursor()
            cur.execute(
                "SELECT dg.diagnosis_type, dg.notes, ds.name "
                "FROM diagnoses dg JOIN diseases ds ON dg.disease_id = ds.id "
                "WHERE dg.visit_id = %s",
                (self._rid(self.max_visit),),
            )
            cur.fetchall()
            cur.close()

        _run("READ", "select_visit_diagnoses", r3)

        def r4():
            cur = conn.cursor()
            cur.execute(
                "SELECT p.first_name, p.last_name, v.visit_date, v.status, "
                "ps.final_price, ms.name AS service_name "
                "FROM patients p "
                "JOIN visits v ON v.patient_id = p.id "
                "LEFT JOIN performed_services ps ON ps.visit_id = v.id "
                "LEFT JOIN medical_services ms ON ms.id = ps.service_id "
                "WHERE p.id = %s",
                (self._rid(self.max_patient),),
            )
            cur.fetchall()
            cur.close()

        _run("READ", "select_patient_full_history", r4)

        def r5():
            cur = conn.cursor()
            cur.execute(
                "SELECT v.id, SUM(ps.final_price) AS total "
                "FROM visits v "
                "JOIN performed_services ps ON ps.visit_id = v.id "
                "WHERE v.patient_id = %s "
                "GROUP BY v.id",
                (self._rid(self.max_patient),),
            )
            cur.fetchall()
            cur.close()

        _run("READ", "select_aggregated_costs", r5)

        def r6():
            cur = conn.cursor()
            cur.execute(
                "SELECT pr.prescription_code, m.name AS medication, pi.dosage "
                "FROM prescriptions pr "
                "JOIN prescription_items pi ON pi.prescription_id = pr.id "
                "JOIN medications m ON pi.medication_id = m.id "
                "WHERE pr.visit_id = %s",
                (self._rid(self.max_visit),),
            )
            cur.fetchall()
            cur.close()

        _run("READ", "select_prescriptions_with_meds", r6)

        # ── UPDATE (6 scenariuszy) ──────────────────────────────────

        def u1():
            cur = conn.cursor()
            cur.execute(
                "UPDATE patients SET last_name = %s WHERE id = %s",
                ("NazwiskoBench", self._rid(self.max_patient)),
            )
            cur.close()

        _run("UPDATE", "update_patient_name", u1)

        def u2():
            cur = conn.cursor()
            cur.execute(
                "UPDATE visits SET status = %s WHERE id = %s",
                ("completed", self._rid(self.max_visit)),
            )
            cur.close()

        _run("UPDATE", "update_visit_status", u2)

        def u3():
            cur = conn.cursor()
            cur.execute(
                "UPDATE performed_services SET final_price = %s WHERE visit_id = %s",
                (999.99, self._rid(self.max_visit)),
            )
            cur.close()

        _run("UPDATE", "update_service_price", u3)

        def u4():
            cur = conn.cursor()
            cur.execute(
                "UPDATE diagnoses SET notes = %s WHERE visit_id = %s",
                ("Zaktualizowana notatka benchmarku", self._rid(self.max_visit)),
            )
            cur.close()

        _run("UPDATE", "update_diagnosis_notes", u4)

        def u5():
            cur = conn.cursor()
            cur.execute(
                "UPDATE doctors SET license_number = %s WHERE id = %s",
                ("NEW-LIC-7777", self._rid(self.max_doctor)),
            )
            cur.close()

        _run("UPDATE", "update_doctor_license", u5)

        def u6():
            cur = conn.cursor()
            cur.execute(
                "UPDATE departments SET phone = %s WHERE id = %s",
                ("+48 000 000 000", self._rid(self.max_department)),
            )
            cur.close()

        _run("UPDATE", "update_department_phone", u6)

        # ── DELETE (6 scenariuszy) ──────────────────────────────────
        # Każdy DELETE: INSERT tymczasowego rekordu, mierzenie czasu DELETE

        def d1():
            cur = conn.cursor()
            tid = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO test_results (id, visit_id, parameter_name, result_value, unit, min_norm, max_norm) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (tid, 1, "TMP", 1.0, "U", 0.0, 1.0),
            )
            cur.execute("DELETE FROM test_results WHERE id=%s", (tid,))
            cur.close()

        _run("DELETE", "delete_test_result", d1)

        def d2():
            cur = conn.cursor()
            did = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO diagnoses (id, visit_id, disease_id, diagnosis_type, notes) "
                "VALUES (%s,%s,%s,%s,%s)",
                (did, 1, 1, "primary", "tmp"),
            )
            cur.execute("DELETE FROM diagnoses WHERE id=%s", (did,))
            cur.close()

        _run("DELETE", "delete_diagnosis", d2)

        def d3():
            cur = conn.cursor()
            pid = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO patients (id, national_id, first_name, last_name, birth_date, gender) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (pid, "00000000000", "Del", "Test", "2000-01-01", "M"),
            )
            cur.execute("DELETE FROM patients WHERE id=%s", (pid,))
            cur.close()

        _run("DELETE", "delete_patient", d3)

        def d4():
            cur = conn.cursor()
            sid = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO performed_services (id, visit_id, service_id, quantity, final_price) "
                "VALUES (%s,%s,%s,%s,%s)",
                (sid, 1, 1, 1, 100.0),
            )
            cur.execute("DELETE FROM performed_services WHERE id=%s", (sid,))
            cur.close()

        _run("DELETE", "delete_performed_service", d4)

        def d5():
            cur = conn.cursor()
            rxid = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO prescriptions (id, visit_id, prescription_code, issue_date) "
                "VALUES (%s,%s,%s,%s)",
                (rxid, 1, "RX-DEL", "2025-01-01"),
            )
            cur.execute("DELETE FROM prescriptions WHERE id=%s", (rxid,))
            cur.close()

        _run("DELETE", "delete_prescription", d5)

        def d6():
            cur = conn.cursor()
            vid = random.randint(10_000_000, 99_999_999)
            pid_tmp = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO patients (id, national_id, first_name, last_name, birth_date, gender) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (pid_tmp, "00000000001", "V", "D", "2000-01-01", "F"),
            )
            cur.execute(
                "INSERT INTO visits (id, patient_id, doctor_id, visit_date, status) "
                "VALUES (%s,%s,%s,%s,%s)",
                (vid, pid_tmp, 1, "2025-01-01", "scheduled"),
            )
            cur.execute("DELETE FROM visits WHERE id=%s", (vid,))
            cur.execute("DELETE FROM patients WHERE id=%s", (pid_tmp,))
            cur.close()

        _run("DELETE", "delete_visit_cascade", d6)

    # ═══════════════════════════════════════════════════════════════════
    #  Scenariusze MongoDB – 24 scenariusze
    # ═══════════════════════════════════════════════════════════════════

    def _mongo_scenarios(self):
        db = self.cm.get_connector(DatabaseType.MONGODB).get_db()
        db_name = DatabaseType.MONGODB.value

        def _run(op: str, name: str, func):
            avg = self._avg_time(func)
            self._record(db_name, op, name, avg)

        # ── CREATE ──────────────────────────────────────────────────

        def c1():
            pid = random.randint(10_000_000, 99_999_999)
            db.patients.insert_one({
                "_id": pid, "first_name": "T", "last_name": "P",
                "national_id": "00000", "gender": "M", "visits": [],
            })
            db.patients.delete_one({"_id": pid})

        _run("CREATE", "insert_patient", c1)

        def c2():
            db.patients.update_one(
                {"_id": self._rid(self.max_patient)},
                {"$push": {"visits": {
                    "visit_id": random.randint(10_000_000, 99_999_999),
                    "doctor_id": self._rid(self.max_doctor),
                    "status": "scheduled",
                    "visit_date": "2025-06-01",
                }}},
            )

        _run("CREATE", "insert_visit", c2)

        def c3():
            db.patients.update_one(
                {"_id": self._rid(self.max_patient)},
                {"$push": {"visits.0.diagnoses": {
                    "disease_id": random.randint(1, 100),
                    "diagnosis_type": "primary",
                    "notes": "bench",
                }}},
            )

        _run("CREATE", "insert_diagnosis", c3)

        def c4():
            db.patients.update_one(
                {"_id": self._rid(self.max_patient)},
                {"$push": {"visits.0.prescriptions": {
                    "prescription_code": "RX-NEW",
                    "items": [{"medication_id": 1, "dosage": "1x200mg"}],
                }}},
            )

        _run("CREATE", "insert_prescription_with_items", c4)

        def c5():
            db.patients.update_one(
                {"_id": self._rid(self.max_patient)},
                {"$push": {"visits.0.performed_services": {
                    "service_id": 99, "quantity": 1, "final_price": 100.0,
                }}},
            )

        _run("CREATE", "insert_performed_service", c5)

        def c6():
            db.patients.update_one(
                {"_id": self._rid(self.max_patient)},
                {"$push": {"visits.0.test_results": {
                    "parameter_name": "Glukoza", "result_value": 90.0,
                    "unit": "mg/dL", "min_norm": 70.0, "max_norm": 110.0,
                }}},
            )

        _run("CREATE", "insert_test_result", c6)

        # ── READ ────────────────────────────────────────────────────

        _run("READ", "select_patient_by_id",
             lambda: db.patients.find_one({"_id": self._rid(self.max_patient)}))

        _run("READ", "select_visits_with_doctor",
             lambda: list(db.patients.find(
                 {"visits.doctor_id": self._rid(self.max_doctor)},
                 {"first_name": 1, "last_name": 1, "visits.$": 1}
             ).limit(50)))

        _run("READ", "select_visit_diagnoses",
             lambda: list(db.patients.find(
                 {"visits.status": "completed"}
             ).limit(10)))

        _run("READ", "select_patient_full_history",
             lambda: db.patients.find_one(
                 {"_id": self._rid(self.max_patient)},
                 {"visits.performed_services": 1, "visits.diagnoses": 1,
                  "visits.visit_date": 1, "first_name": 1, "last_name": 1}
             ))

        def r5():
            pipeline = [
                {"$match": {"_id": self._rid(self.max_patient)}},
                {"$unwind": "$visits"},
                {"$unwind": "$visits.performed_services"},
                {"$group": {
                    "_id": "$_id",
                    "total": {"$sum": "$visits.performed_services.final_price"},
                }},
            ]
            list(db.patients.aggregate(pipeline))

        _run("READ", "select_aggregated_costs", r5)

        _run("READ", "select_prescriptions_with_meds",
             lambda: db.patients.find_one(
                 {"visits.prescriptions.prescription_code": {"$exists": True},
                  "_id": self._rid(self.max_patient)},
                 {"visits.prescriptions": 1}
             ))

        # ── UPDATE ──────────────────────────────────────────────────

        _run("UPDATE", "update_patient_name",
             lambda: db.patients.update_one(
                 {"_id": self._rid(self.max_patient)},
                 {"$set": {"last_name": "NazwiskoMongo"}}
             ))

        _run("UPDATE", "update_visit_status",
             lambda: db.patients.update_one(
                 {"_id": self._rid(self.max_patient)},
                 {"$set": {"visits.0.status": "completed"}}
             ))

        _run("UPDATE", "update_service_price",
             lambda: db.patients.update_one(
                 {"_id": self._rid(self.max_patient)},
                 {"$set": {"visits.0.performed_services.0.final_price": 999.99}}
             ))

        _run("UPDATE", "update_diagnosis_notes",
             lambda: db.patients.update_one(
                 {"_id": self._rid(self.max_patient)},
                 {"$set": {"visits.0.diagnoses.0.notes": "Zaktualizowane Mongo"}}
             ))

        _run("UPDATE", "update_doctor_license",
             lambda: db.patients.update_many(
                 {"visits.doctor_id": self._rid(self.max_doctor)},
                 {"$set": {"visits.$[].doctor_id": self._rid(self.max_doctor)}}
             ))

        _run("UPDATE", "update_department_phone",
             lambda: db.patients.update_many(
                 {"_id": {"$in": [self._rid(self.max_patient) for _ in range(3)]}},
                 {"$set": {"tag": "batch_update"}}
             ))

        # ── DELETE ──────────────────────────────────────────────────

        def d1():
            db.patients.update_one(
                {"_id": 1},
                {"$push": {"visits.0.test_results": {"parameter_name": "TMP_DEL"}}},
            )
            db.patients.update_one(
                {"_id": 1},
                {"$pull": {"visits.0.test_results": {"parameter_name": "TMP_DEL"}}},
            )

        _run("DELETE", "delete_test_result", d1)

        def d2():
            db.patients.update_one(
                {"_id": 1},
                {"$push": {"visits.0.diagnoses": {"disease_id": 99999}}},
            )
            db.patients.update_one(
                {"_id": 1},
                {"$pull": {"visits.0.diagnoses": {"disease_id": 99999}}},
            )

        _run("DELETE", "delete_diagnosis", d2)

        def d3():
            pid = random.randint(10_000_000, 99_999_999)
            db.patients.insert_one({"_id": pid, "visits": []})
            db.patients.delete_one({"_id": pid})

        _run("DELETE", "delete_patient", d3)

        def d4():
            db.patients.update_one(
                {"_id": 1},
                {"$push": {"visits.0.performed_services": {"service_id": 99999}}},
            )
            db.patients.update_one(
                {"_id": 1},
                {"$pull": {"visits.0.performed_services": {"service_id": 99999}}},
            )

        _run("DELETE", "delete_performed_service", d4)

        def d5():
            db.patients.update_one(
                {"_id": 1},
                {"$push": {"visits.0.prescriptions": {"prescription_code": "RX-DEL"}}},
            )
            db.patients.update_one(
                {"_id": 1},
                {"$pull": {"visits.0.prescriptions": {"prescription_code": "RX-DEL"}}},
            )

        _run("DELETE", "delete_prescription", d5)

        def d6():
            db.patients.update_one(
                {"_id": 1},
                {"$push": {"visits": {"visit_id": 99999999}}},
            )
            db.patients.update_one(
                {"_id": 1},
                {"$pull": {"visits": {"visit_id": 99999999}}},
            )

        _run("DELETE", "delete_visit_cascade", d6)

    # ═══════════════════════════════════════════════════════════════════
    #  Scenariusze Redis – 24 scenariusze
    # ═══════════════════════════════════════════════════════════════════

    def _redis_scenarios(self):
        r = self.cm.get_connector(DatabaseType.REDIS).get_connection()
        db_name = DatabaseType.REDIS.value

        def _run(op: str, name: str, func):
            avg = self._avg_time(func)
            self._record(db_name, op, name, avg)

        # ── CREATE ──────────────────────────────────────────────────

        _run("CREATE", "insert_patient",
             lambda: r.hset(
                 f"patient:{random.randint(10_000_000, 99_999_999)}",
                 mapping={"name": "Test", "national_id": "000", "gender": "M"}
             ))

        _run("CREATE", "insert_visit",
             lambda: r.set(
                 f"visit:status:{random.randint(10_000_000, 99_999_999)}",
                 "scheduled"
             ))

        _run("CREATE", "insert_diagnosis",
             lambda: r.lpush(
                 f"visit:diag:{random.randint(10_000_000, 99_999_999)}",
                 "primary:disease_1"
             ))

        _run("CREATE", "insert_prescription_with_items",
             lambda: r.hset(
                 f"prescription:{random.randint(10_000_000, 99_999_999)}",
                 mapping={"code": "RX-NEW", "med": "Lek_1", "dosage": "1x200mg"}
             ))

        _run("CREATE", "insert_performed_service",
             lambda: r.hset(
                 f"service:{random.randint(10_000_000, 99_999_999)}",
                 mapping={"service_id": "1", "price": "199.99", "qty": "1"}
             ))

        _run("CREATE", "insert_test_result",
             lambda: r.hset(
                 f"test_result:{random.randint(10_000_000, 99_999_999)}",
                 mapping={"param": "Hemoglobina", "value": "13.5", "unit": "g/dL"}
             ))

        # ── READ ────────────────────────────────────────────────────

        _run("READ", "select_patient_by_id",
             lambda: r.hgetall(f"patient:{self._rid(self.max_patient)}"))

        _run("READ", "select_visits_with_doctor",
             lambda: r.hgetall(f"session:doctor:{self._rid(self.max_doctor)}"))

        _run("READ", "select_visit_diagnoses",
             lambda: r.get(f"visit:status:{self._rid(self.max_visit)}"))

        _run("READ", "select_patient_full_history",
             lambda: r.lrange(f"visit:diag:{self._rid(self.max_visit)}", 0, -1))

        _run("READ", "select_aggregated_costs",
             lambda: r.mget(
                 *[f"visit:status:{i}" for i in range(1, min(11, self.max_visit))]
             ))

        _run("READ", "select_prescriptions_with_meds",
             lambda: r.hgetall(f"prescription:{self._rid(self.max_visit)}"))

        # ── UPDATE ──────────────────────────────────────────────────

        _run("UPDATE", "update_patient_name",
             lambda: r.hset(
                 f"patient:{self._rid(self.max_patient)}", "name", "Updated"
             ))

        _run("UPDATE", "update_visit_status",
             lambda: r.set(
                 f"visit:status:{self._rid(self.max_visit)}", "cancelled"
             ))

        _run("UPDATE", "update_service_price",
             lambda: r.hset(
                 f"service:{self._rid(self.max_visit)}", "price", "999.99"
             ))

        _run("UPDATE", "update_diagnosis_notes",
             lambda: r.append(
                 f"visit:status:{self._rid(self.max_visit)}", "_upd"
             ))

        _run("UPDATE", "update_doctor_license",
             lambda: r.hset(
                 f"session:doctor:{self._rid(self.max_doctor)}", "license_number", "NEW"
             ))

        _run("UPDATE", "update_department_phone",
             lambda: r.hset(
                 f"session:doctor:{self._rid(self.max_doctor)}", "status", "active"
             ))

        # ── DELETE ──────────────────────────────────────────────────

        def _del(key_prefix: str, setup, delete):
            k = f"{key_prefix}:{random.randint(10_000_000, 99_999_999)}"
            setup(k)
            delete(k)

        _run("DELETE", "delete_test_result",
             lambda: _del("tmp:tr",
                          lambda k: r.hset(k, "p", "v"),
                          lambda k: r.delete(k)))

        _run("DELETE", "delete_diagnosis",
             lambda: _del("tmp:dg",
                          lambda k: r.lpush(k, "x"),
                          lambda k: r.delete(k)))

        _run("DELETE", "delete_patient",
             lambda: _del("tmp:pat",
                          lambda k: r.hset(k, mapping={"n": "T"}),
                          lambda k: r.delete(k)))

        _run("DELETE", "delete_performed_service",
             lambda: _del("tmp:ps",
                          lambda k: r.hset(k, "s", "1"),
                          lambda k: r.delete(k)))

        _run("DELETE", "delete_prescription",
             lambda: _del("tmp:rx",
                          lambda k: r.set(k, "RX"),
                          lambda k: r.delete(k)))

        _run("DELETE", "delete_visit_cascade",
             lambda: _del("tmp:vis",
                          lambda k: r.set(k, "scheduled"),
                          lambda k: r.delete(k)))

    # ═══════════════════════════════════════════════════════════════════
    #  Zapis wyników
    # ═══════════════════════════════════════════════════════════════════

    def _save_results(self, filename: str):
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Database", "Scale", "Operation_Type",
                "Scenario_Name", "Average_Time_Seconds",
            ])
            for row in self.results:
                writer.writerow(row)
