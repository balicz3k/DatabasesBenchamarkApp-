"""
core/benchmark.py – 24 scenariusze testowe CRUD x 4 bazy danych.
Każdy scenariusz wykonywany RUNS razy, raportowana jest średnia.
Wyniki zapisywane do CSV; EXPLAIN do osobnego pliku tekstowego.

UWAGA METODOLOGICZNA – Redis vs. SQL/MongoDB:
  Redis jest bazą klucz-wartość (in-memory). Nie obsługuje JOINów ani
  transakcji wielotabelowych. Scenariusze Redis symulują odpowiedniki
  logiczne operacji SQL poprzez wielokluczowe pipeline'y:
    - insert_patient  → pipeline HSET+SADD (pacjent + mapowanie wizyt)
    - select_visits_with_doctor → SMEMBERS + pipeline GET/HGETALL (symulacja JOIN)
    - select_patient_full_history → 2-fazowy pipeline (pacjent + wizyty + diagnozy)
    - select_aggregated_costs → SMEMBERS + batch HGETALL + agregacja w kliencie
  Dlatego czasy Redis nie są w pełni porównywalne z SQL (brak JOINów, brak dysku),
  ale scenariusze odzwierciedlają realistyczne wzorce dostępu klucz-wartość.
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

CSV_HEADER = [
    "Database", "Scale", "Indexed",
    "Operation_Type", "Scenario_Name", "Average_Time_Seconds",
]

ProgressCallback = Optional[Callable[[str], None]]


def _ensure_results_dir():
    os.makedirs(RESULTS_DIR, exist_ok=True)


class BenchmarkEngine:
    """Silnik benchmarków – 24 scenariusze CRUD dla każdej bazy danych."""

    def __init__(self, connection_manager: ConnectionManager, scale: int):
        self.cm = connection_manager
        self.scale = scale
        self.is_indexed: bool = False
        self.results: list[tuple] = []
        cfg = SCALE_MAP.get(scale, {})
        self.max_patient = cfg.get("patients", max(1, scale // 5))
        self.max_doctor = cfg.get("doctors", max(1, scale // 200))
        self.max_visit = cfg.get("visits", max(1, scale))
        self.max_disease = cfg.get("diseases", 100)
        self.max_service = cfg.get("medical_services", 50)
        self.max_medication = cfg.get("medications", 80)
        self.max_department = cfg.get("departments", 10)
        # Przybliżona liczba recept (~40% wizyt ma recepte)
        self.max_prescription = max(1, int(self.max_visit * 0.4))

        # Bufor patient_id z potwierdzonymi wizytami (MongoDB)
        self._cached_mongo_pids: Optional[list] = None

    def _rid(self, max_id: int) -> int:
        return random.randint(1, max(1, max_id))

    def _mongo_pid_with_visits(self, db) -> int:
        """Zwraca losowy _id pacjenta który ma co najmniej jedną wizytę (MongoDB)."""
        if self._cached_mongo_pids is None:
            docs = list(
                db.patients.find(
                    {"visits.0": {"$exists": True}}, {"_id": 1}
                ).limit(2000).sort("_id", 1)
            )
            self._cached_mongo_pids = (
                [d["_id"] for d in docs] if docs else list(range(1, 11))
            )
        return random.choice(self._cached_mongo_pids)

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
        self.results.append((db_name, self.scale, self.is_indexed, op, scenario, avg))

    # ═══════════════════════════════════════════════════════════════════
    #  Publiczne API
    # ═══════════════════════════════════════════════════════════════════

    def run_benchmarks(
        self, is_indexed: bool, progress_callback: ProgressCallback = None
    ):
        _ensure_results_dir()
        self.is_indexed = is_indexed
        self.results = []
        self._cached_mongo_pids = None  # reset cache dla nowego runu

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
                _report(f"   BLAD {db_type.value}: {e}")

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

            SEP = "-" * 70

            queries_pg = [
                (
                    "SELECT z JOINem - wizyty pacjenta z danymi lekarza",
                    "EXPLAIN ANALYZE SELECT v.id, v.visit_date, v.status, d.first_name, d.last_name "
                    "FROM visits v JOIN doctors d ON v.doctor_id = d.id "
                    "WHERE v.patient_id = 1",
                ),
                (
                    "SELECT z agregacja - suma kosztow uslug na wizyte",
                    "EXPLAIN ANALYZE SELECT v.id, SUM(ps.final_price) AS total "
                    "FROM visits v JOIN performed_services ps ON ps.visit_id = v.id "
                    "WHERE v.patient_id = 1 GROUP BY v.id",
                ),
                (
                    "SELECT z wieloma JOINami - pelna historia pacjenta",
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
                        f.write(f"  BLAD: {e}\n")
                cur.close()
            except Exception as e:
                f.write(f"  Nie mozna polaczyc: {e}\n")

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
                        f.write(f"  BLAD: {e}\n")
                cur.close()
            except Exception as e:
                f.write(f"  Nie mozna polaczyc: {e}\n")

            f.write(f"\n{SEP}\n  MongoDB\n{SEP}\n")
            try:
                db = self.cm.get_connector(DatabaseType.MONGODB).get_db()
                mongo_queries = [
                    ("find po _id", {"find": "patients", "filter": {"_id": 1}}),
                    (
                        "find po statusie wizyty (zagniezdzone)",
                        {"find": "patients", "filter": {"visits.status": "completed"}},
                    ),
                    (
                        "find po nazwisku z projekcja",
                        {
                            "find": "patients",
                            "filter": {"last_name": "Kowalski"},
                            "projection": {
                                "first_name": 1, "last_name": 1, "visits.visit_date": 1,
                            },
                        },
                    ),
                ]
                for title, cmd in mongo_queries:
                    f.write(f"\n> {title}\n  Zapytanie: {cmd}\n\n")
                    try:
                        plan = db.command("explain", cmd, verbosity="executionStats")
                        stats = plan.get("executionStats", {})
                        f.write(f"  executionSuccess: {stats.get('executionSuccess')}\n")
                        f.write(f"  nReturned: {stats.get('nReturned')}\n")
                        f.write(f"  executionTimeMillis: {stats.get('executionTimeMillis')}\n")
                        f.write(f"  totalKeysExamined: {stats.get('totalKeysExamined')}\n")
                        f.write(f"  totalDocsExamined: {stats.get('totalDocsExamined')}\n")
                        stage = stats.get("executionStages", stats.get("inputStage", {}))
                        f.write(f"  stage: {stage.get('stage', 'N/A')}\n")
                    except Exception as e:
                        f.write(f"  BLAD: {e}\n")
            except Exception as e:
                f.write(f"  Nie mozna polaczyc: {e}\n")

            f.write(f"\n{SEP}\n  Redis\n{SEP}\n")
            f.write(
                "  Redis jest baza klucz-wartosc i nie posiada mechanizmu EXPLAIN.\n"
                "  Zlozonosc operacji: GET/SET -> O(1), HGETALL -> O(N pól w hashu),\n"
                "  LRANGE -> O(S+N), MGET -> O(N kluczy).\n"
                "  Redis operuje wylacznie w pamieci RAM – brak dostepu do dysku.\n"
            )

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

        # ── CREATE (6 scenariuszy) ─────────────────────────────────────

        def c1():
            cur = conn.cursor()
            pid = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO patients (id,national_id,first_name,last_name,birth_date,gender) "
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
                "INSERT INTO visits (id,patient_id,doctor_id,visit_date,status) "
                "VALUES (%s,%s,%s,%s,%s)",
                (vid, self._rid(self.max_patient), self._rid(self.max_doctor),
                 "2025-06-01", "scheduled"),
            )
            cur.execute("DELETE FROM visits WHERE id=%s", (vid,))
            cur.close()

        _run("CREATE", "insert_visit", c2)

        def c3():
            cur = conn.cursor()
            did = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO diagnoses (id,visit_id,disease_id,diagnosis_type,notes) "
                "VALUES (%s,%s,%s,%s,%s)",
                (did, self._rid(self.max_visit), self._rid(self.max_disease),
                 "primary", "bench note"),
            )
            cur.execute("DELETE FROM diagnoses WHERE id=%s", (did,))
            cur.close()

        _run("CREATE", "insert_diagnosis", c3)

        def c4():
            cur = conn.cursor()
            pid = random.randint(10_000_000, 99_999_999)
            iid = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO prescriptions (id,visit_id,prescription_code,issue_date) "
                "VALUES (%s,%s,%s,%s)",
                (pid, self._rid(self.max_visit), "RX-BENCH", "2025-06-01"),
            )
            cur.execute(
                "INSERT INTO prescription_items (id,prescription_id,medication_id,dosage) "
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
                "INSERT INTO performed_services (id,visit_id,service_id,quantity,final_price) "
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
                "INSERT INTO test_results (id,visit_id,parameter_name,result_value,unit,min_norm,max_norm) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (tid, self._rid(self.max_visit), "Hemoglobina", 13.5, "g/dL", 12.0, 16.0),
            )
            cur.execute("DELETE FROM test_results WHERE id=%s", (tid,))
            cur.close()

        _run("CREATE", "insert_test_result", c6)

        # ── READ (6 scenariuszy) ───────────────────────────────────────

        def r1():
            cur = conn.cursor()
            cur.execute("SELECT * FROM patients WHERE id = %s",
                        (self._rid(self.max_patient),))
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

        # ── UPDATE (6 scenariuszy) ─────────────────────────────────────

        def u1():
            cur = conn.cursor()
            cur.execute("UPDATE patients SET last_name = %s WHERE id = %s",
                        ("NazwiskoBench", self._rid(self.max_patient)))
            cur.close()

        _run("UPDATE", "update_patient_name", u1)

        def u2():
            cur = conn.cursor()
            cur.execute("UPDATE visits SET status = %s WHERE id = %s",
                        ("completed", self._rid(self.max_visit)))
            cur.close()

        _run("UPDATE", "update_visit_status", u2)

        def u3():
            cur = conn.cursor()
            cur.execute("UPDATE performed_services SET final_price = %s WHERE visit_id = %s",
                        (999.99, self._rid(self.max_visit)))
            cur.close()

        _run("UPDATE", "update_service_price", u3)

        def u4():
            cur = conn.cursor()
            cur.execute("UPDATE diagnoses SET notes = %s WHERE visit_id = %s",
                        ("Zaktualizowana notatka benchmarku", self._rid(self.max_visit)))
            cur.close()

        _run("UPDATE", "update_diagnosis_notes", u4)

        def u5():
            cur = conn.cursor()
            cur.execute("UPDATE doctors SET license_number = %s WHERE id = %s",
                        ("NEW-LIC-7777", self._rid(self.max_doctor)))
            cur.close()

        _run("UPDATE", "update_doctor_license", u5)

        def u6():
            cur = conn.cursor()
            cur.execute("UPDATE departments SET phone = %s WHERE id = %s",
                        ("+48 000 000 000", self._rid(self.max_department)))
            cur.close()

        _run("UPDATE", "update_department_phone", u6)

        # ── DELETE (6 scenariuszy) ─────────────────────────────────────
        # Wzorzec: INSERT tymczasowy + DELETE – mierzone razem (spójne z innymi DB)

        def d1():
            cur = conn.cursor()
            tid = random.randint(10_000_000, 99_999_999)
            cur.execute(
                "INSERT INTO test_results (id,visit_id,parameter_name,result_value,unit,min_norm,max_norm) "
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
                "INSERT INTO diagnoses (id,visit_id,disease_id,diagnosis_type,notes) "
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
                "INSERT INTO patients (id,national_id,first_name,last_name,birth_date,gender) "
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
                "INSERT INTO performed_services (id,visit_id,service_id,quantity,final_price) "
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
                "INSERT INTO prescriptions (id,visit_id,prescription_code,issue_date) "
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
                "INSERT INTO patients (id,national_id,first_name,last_name,birth_date,gender) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (pid_tmp, "00000000001", "V", "D", "2000-01-01", "F"),
            )
            cur.execute(
                "INSERT INTO visits (id,patient_id,doctor_id,visit_date,status) "
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

        # ── CREATE ────────────────────────────────────────────────────

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
                    "diagnoses": [], "prescriptions": [],
                    "performed_services": [], "test_results": [],
                }}},
            )

        _run("CREATE", "insert_visit", c2)

        def c3():
            # Używamy max_disease zamiast hardkodowanego 100
            db.patients.update_one(
                {"_id": self._mongo_pid_with_visits(db)},
                {"$push": {"visits.0.diagnoses": {
                    "disease_id": self._rid(self.max_disease),
                    "diagnosis_type": "primary",
                    "notes": "bench",
                }}},
            )

        _run("CREATE", "insert_diagnosis", c3)

        def c4():
            db.patients.update_one(
                {"_id": self._mongo_pid_with_visits(db)},
                {"$push": {"visits.0.prescriptions": {
                    "prescription_code": "RX-NEW",
                    "issue_date": "2025-06-01",
                    "items": [{"medication_id": self._rid(self.max_medication),
                               "dosage": "1x200mg"}],
                }}},
            )

        _run("CREATE", "insert_prescription_with_items", c4)

        def c5():
            db.patients.update_one(
                {"_id": self._mongo_pid_with_visits(db)},
                {"$push": {"visits.0.performed_services": {
                    "service_id": self._rid(self.max_service),
                    "quantity": 1, "final_price": 100.0,
                }}},
            )

        _run("CREATE", "insert_performed_service", c5)

        def c6():
            db.patients.update_one(
                {"_id": self._mongo_pid_with_visits(db)},
                {"$push": {"visits.0.test_results": {
                    "parameter_name": "Glukoza", "result_value": 90.0,
                    "unit": "mg/dL", "min_norm": 70.0, "max_norm": 110.0,
                }}},
            )

        _run("CREATE", "insert_test_result", c6)

        # ── READ ──────────────────────────────────────────────────────

        _run("READ", "select_patient_by_id",
             lambda: db.patients.find_one({"_id": self._rid(self.max_patient)}))

        _run("READ", "select_visits_with_doctor",
             lambda: list(db.patients.find(
                 {"visits.doctor_id": self._rid(self.max_doctor)},
                 {"first_name": 1, "last_name": 1, "visits.$": 1},
             ).limit(50)))

        def r3_mongo():
            pid = self._mongo_pid_with_visits(db)
            return list(db.patients.aggregate([
                {"$match": {"_id": pid}},
                {"$unwind": "$visits"},
                {"$unwind": "$visits.diagnoses"},
                {"$project": {
                    "visit_date": "$visits.visit_date",
                    "diagnosis_type": "$visits.diagnoses.diagnosis_type",
                    "disease_id": "$visits.diagnoses.disease_id",
                    "notes": "$visits.diagnoses.notes",
                }},
            ]))

        _run("READ", "select_visit_diagnoses", r3_mongo)

        _run("READ", "select_patient_full_history",
             lambda: db.patients.find_one(
                 {"_id": self._rid(self.max_patient)},
                 {"visits.performed_services": 1, "visits.diagnoses": 1,
                  "visits.visit_date": 1, "first_name": 1, "last_name": 1},
             ))

        def r5():
            pipeline = [
                {"$match": {"_id": self._rid(self.max_patient)}},
                {"$unwind": "$visits"},
                {"$unwind": "$visits.performed_services"},
                {"$group": {"_id": "$_id",
                            "total": {"$sum": "$visits.performed_services.final_price"}}},
            ]
            list(db.patients.aggregate(pipeline))

        _run("READ", "select_aggregated_costs", r5)

        _run("READ", "select_prescriptions_with_meds",
             lambda: db.patients.find_one(
                 {"_id": self._rid(self.max_patient),
                  "visits.prescriptions.prescription_code": {"$exists": True}},
                 {"visits.prescriptions": 1},
             ))

        # ── UPDATE ────────────────────────────────────────────────────

        _run("UPDATE", "update_patient_name",
             lambda: db.patients.update_one(
                 {"_id": self._rid(self.max_patient)},
                 {"$set": {"last_name": "NazwiskoMongo"}},
             ))

        _run("UPDATE", "update_visit_status",
             lambda: db.patients.update_one(
                 {"_id": self._mongo_pid_with_visits(db)},
                 {"$set": {"visits.0.status": "completed"}},
             ))

        _run("UPDATE", "update_service_price",
             lambda: db.patients.update_one(
                 {"_id": self._mongo_pid_with_visits(db)},
                 {"$set": {"visits.0.performed_services.0.final_price": 999.99}},
             ))

        _run("UPDATE", "update_diagnosis_notes",
             lambda: db.patients.update_one(
                 {"_id": self._mongo_pid_with_visits(db)},
                 {"$set": {"visits.0.diagnoses.0.notes": "Zaktualizowane Mongo"}},
             ))

        def u5():
            pid = self._mongo_pid_with_visits(db)
            did = self._rid(self.max_doctor)
            db.patients.update_one(
                {"_id": pid, "visits.doctor_id": {"$exists": True}},
                {"$set": {"visits.0.doctor_license": f"LIC-{did}"}},
            )

        _run("UPDATE", "update_doctor_license", u5)

        def u6():
            pid = self._mongo_pid_with_visits(db)
            db.patients.update_one(
                {"_id": pid},
                {"$set": {"visits.0.department_phone": "+48 000 000 000"}},
            )

        _run("UPDATE", "update_department_phone", u6)

        # ── DELETE ────────────────────────────────────────────────────
        # Wzorzec: $push tymczasowego elementu + $pull – mierzone razem

        def d1():
            pid = self._mongo_pid_with_visits(db)
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits.0.test_results": {"parameter_name": "TMP_DEL"}}},
            )
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits.0.test_results": {"parameter_name": "TMP_DEL"}}},
            )

        _run("DELETE", "delete_test_result", d1)

        def d2():
            pid = self._mongo_pid_with_visits(db)
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits.0.diagnoses": {"disease_id": 99999}}},
            )
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits.0.diagnoses": {"disease_id": 99999}}},
            )

        _run("DELETE", "delete_diagnosis", d2)

        def d3():
            pid = random.randint(10_000_000, 99_999_999)
            db.patients.insert_one({"_id": pid, "visits": []})
            db.patients.delete_one({"_id": pid})

        _run("DELETE", "delete_patient", d3)

        def d4():
            pid = self._mongo_pid_with_visits(db)
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits.0.performed_services": {"service_id": 99999}}},
            )
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits.0.performed_services": {"service_id": 99999}}},
            )

        _run("DELETE", "delete_performed_service", d4)

        def d5():
            pid = self._mongo_pid_with_visits(db)
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits.0.prescriptions": {"prescription_code": "RX-DEL"}}},
            )
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits.0.prescriptions": {"prescription_code": "RX-DEL"}}},
            )

        _run("DELETE", "delete_prescription", d5)

        def d6():
            pid = self._rid(self.max_patient)
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits": {"visit_id": 99999999}}},
            )
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits": {"visit_id": 99999999}}},
            )

        _run("DELETE", "delete_visit_cascade", d6)

    # ═══════════════════════════════════════════════════════════════════
    #  Scenariusze Redis – 24 scenariusze
    #
    #  Redis jest bazą klucz-wartość i nie posiada JOINów. Scenariusze
    #  symulują odpowiedniki logiczne operacji SQL poprzez pipeline'y
    #  łączące odczyty z wielu kluczy (multi-key lookup).
    #  Seedowane klucze:
    #    patient:{id}            HASH
    #    patient:visits:{pid}    SET  {visit_id, ...}
    #    visit:status:{id}       STRING
    #    visit:doctor:{vid}      STRING
    #    session:doctor:{id}     HASH
    #    visit:diag:{visit_id}   LIST
    #    prescription:{id}       HASH
    #    service:total:{vid}     HASH
    #    test:{vid}              HASH
    #    department:{id}         HASH
    # ═══════════════════════════════════════════════════════════════════

    def _redis_scenarios(self):
        r = self.cm.get_connector(DatabaseType.REDIS).get_connection()
        db_name = DatabaseType.REDIS.value

        def _run(op: str, name: str, func):
            avg = self._avg_time(func)
            self._record(db_name, op, name, avg)

        # ── CREATE ────────────────────────────────────────────────────
        # Wzorzec: tworzenie wielu powiązanych kluczy (jak INSERT w SQL
        # tworzy wiersz + aktualizuje indeksy/FK), następnie cleanup.

        def c1():
            pid = random.randint(10_000_000, 99_999_999)
            pipe = r.pipeline()
            pipe.hset(f"patient:{pid}", mapping={
                "first_name": "T", "last_name": "P",
                "national_id": "000", "gender": "M",
            })
            pipe.sadd(f"patient:visits:{pid}", "0")
            pipe.execute()
            r.delete(f"patient:{pid}", f"patient:visits:{pid}")

        _run("CREATE", "insert_patient", c1)

        def c2():
            vid = random.randint(10_000_000, 99_999_999)
            pid = self._rid(self.max_patient)
            did = self._rid(self.max_doctor)
            pipe = r.pipeline()
            pipe.set(f"visit:status:{vid}", "scheduled")
            pipe.set(f"visit:doctor:{vid}", str(did))
            pipe.sadd(f"patient:visits:{pid}", str(vid))
            pipe.execute()
            pipe2 = r.pipeline()
            pipe2.delete(f"visit:status:{vid}", f"visit:doctor:{vid}")
            pipe2.srem(f"patient:visits:{pid}", str(vid))
            pipe2.execute()

        _run("CREATE", "insert_visit", c2)

        def c3():
            vid = self._rid(self.max_visit)
            pipe = r.pipeline()
            pipe.rpush(f"visit:diag:{vid}", f"{self._rid(self.max_disease)}:primary:bench")
            pipe.execute()

        _run("CREATE", "insert_diagnosis", c3)

        def c4():
            rxid = random.randint(10_000_000, 99_999_999)
            pipe = r.pipeline()
            pipe.hset(f"prescription:{rxid}", mapping={
                "visit_id": str(self._rid(self.max_visit)),
                "code": "RX-NEW",
                "issue_date": "2025-06-01",
                "med_1": str(self._rid(self.max_medication)),
                "dosage_1": "1x500mg",
                "med_2": str(self._rid(self.max_medication)),
                "dosage_2": "2x200mg",
            })
            pipe.execute()
            r.delete(f"prescription:{rxid}")

        _run("CREATE", "insert_prescription_with_items", c4)

        def c5():
            sid = random.randint(10_000_000, 99_999_999)
            vid = self._rid(self.max_visit)
            pipe = r.pipeline()
            pipe.hset(f"service:perf:{sid}", mapping={
                "visit_id": str(vid), "service_id": str(self._rid(self.max_service)),
                "quantity": "1", "final_price": "199.99",
            })
            pipe.hincrbyfloat(f"service:total:{vid}", "total_price", 199.99)
            pipe.execute()
            pipe2 = r.pipeline()
            pipe2.delete(f"service:perf:{sid}")
            pipe2.hincrbyfloat(f"service:total:{vid}", "total_price", -199.99)
            pipe2.execute()

        _run("CREATE", "insert_performed_service", c5)

        def c6():
            tid = random.randint(10_000_000, 99_999_999)
            r.hset(f"test:{tid}", mapping={
                "visit_id": str(self._rid(self.max_visit)),
                "parameter": "Hemoglobina", "value": "13.5",
                "unit": "g/dL", "min_norm": "12.0", "max_norm": "16.0",
            })
            r.delete(f"test:{tid}")

        _run("CREATE", "insert_test_result", c6)

        # ── READ ──────────────────────────────────────────────────────
        # Symulacja JOINów przez wielokluczowe pipeline'y.

        _run("READ", "select_patient_by_id",
             lambda: r.hgetall(f"patient:{self._rid(self.max_patient)}"))

        def r2():
            pid = self._rid(self.max_patient)
            visit_ids = r.smembers(f"patient:visits:{pid}")
            if not visit_ids:
                return []
            sample = list(visit_ids)[:20]
            pipe = r.pipeline()
            for vid in sample:
                pipe.get(f"visit:status:{vid}")
                pipe.get(f"visit:doctor:{vid}")
            results = pipe.execute()
            doctor_ids = set()
            for i in range(1, len(results), 2):
                if results[i]:
                    doctor_ids.add(results[i])
            if doctor_ids:
                pipe2 = r.pipeline()
                for did in list(doctor_ids)[:10]:
                    pipe2.hgetall(f"session:doctor:{did}")
                pipe2.execute()

        _run("READ", "select_visits_with_doctor", r2)

        def r3():
            vid = self._rid(self.max_visit)
            diagnoses = r.lrange(f"visit:diag:{vid}", 0, -1)
            if diagnoses:
                pipe = r.pipeline()
                for d in diagnoses[:10]:
                    parts = d.split(":")
                    if parts:
                        pipe.exists(f"disease:{parts[0]}")
                pipe.execute()

        _run("READ", "select_visit_diagnoses", r3)

        def r4():
            pid = self._rid(self.max_patient)
            pipe = r.pipeline()
            pipe.hgetall(f"patient:{pid}")
            pipe.smembers(f"patient:visits:{pid}")
            res = pipe.execute()
            visit_ids = res[1] if res[1] else set()
            if visit_ids:
                sample = list(visit_ids)[:10]
                pipe2 = r.pipeline()
                for vid in sample:
                    pipe2.get(f"visit:status:{vid}")
                    pipe2.lrange(f"visit:diag:{vid}", 0, -1)
                    pipe2.hgetall(f"service:total:{vid}")
                pipe2.execute()

        _run("READ", "select_patient_full_history", r4)

        def r5():
            pid = self._rid(self.max_patient)
            visit_ids = r.smembers(f"patient:visits:{pid}")
            if not visit_ids:
                return 0.0
            pipe = r.pipeline()
            for vid in visit_ids:
                pipe.hgetall(f"service:total:{vid}")
            results = pipe.execute()
            total = 0.0
            for h in results:
                if h and "total_price" in h:
                    try:
                        total += float(h["total_price"])
                    except (ValueError, TypeError):
                        pass
            return total

        _run("READ", "select_aggregated_costs", r5)

        def r6():
            pid = self._rid(self.max_patient)
            visit_ids = r.smembers(f"patient:visits:{pid}")
            if not visit_ids:
                return
            sample = list(visit_ids)[:10]
            pipe = r.pipeline()
            for vid in sample:
                pipe.get(f"visit:status:{vid}")
            statuses = pipe.execute()
            pipe2 = r.pipeline()
            for i, vid in enumerate(sample):
                pipe2.hgetall(f"prescription:{vid}")
            pipe2.execute()

        _run("READ", "select_prescriptions_with_meds", r6)

        # ── UPDATE ────────────────────────────────────────────────────

        _run("UPDATE", "update_patient_name",
             lambda: r.hset(f"patient:{self._rid(self.max_patient)}",
                            "last_name", "Updated"))

        _run("UPDATE", "update_visit_status",
             lambda: r.set(f"visit:status:{self._rid(self.max_visit)}", "cancelled"))

        def u3():
            vid = self._rid(self.max_visit)
            pipe = r.pipeline()
            pipe.hset(f"service:total:{vid}", "total_price", "999.99")
            pipe.execute()

        _run("UPDATE", "update_service_price", u3)

        def u4():
            vid = self._rid(self.max_visit)
            k = f"visit:diag:{vid}"
            length = r.llen(k)
            if length > 0:
                r.lset(k, 0, f"{self._rid(self.max_disease)}:updated")
            else:
                r.rpush(k, f"{self._rid(self.max_disease)}:primary")

        _run("UPDATE", "update_diagnosis_notes", u4)

        _run("UPDATE", "update_doctor_license",
             lambda: r.hset(f"session:doctor:{self._rid(self.max_doctor)}",
                            "license_number", "NEW-LIC"))

        _run("UPDATE", "update_department_phone",
             lambda: r.hset(f"department:{self._rid(self.max_department)}",
                            "phone", "+48 000 000 000"))

        # ── DELETE ────────────────────────────────────────────────────
        # Wzorzec: INSERT tmp + DELETE – pomiar cyklu życia klucza.

        def d1():
            tid = random.randint(10_000_000, 99_999_999)
            r.hset(f"test:{tid}", mapping={
                "parameter": "Hemo", "value": "13.5", "unit": "g/dL",
                "min_norm": "12.0", "max_norm": "16.0",
            })
            r.delete(f"test:{tid}")

        _run("DELETE", "delete_test_result", d1)

        def d2():
            vid = self._rid(self.max_visit)
            r.rpush(f"visit:diag:{vid}", "99999:primary:tmp")
            r.lrem(f"visit:diag:{vid}", 1, "99999:primary:tmp")

        _run("DELETE", "delete_diagnosis", d2)

        def d3():
            pid = random.randint(10_000_000, 99_999_999)
            pipe = r.pipeline()
            pipe.hset(f"patient:{pid}", mapping={
                "first_name": "T", "last_name": "P", "gender": "M",
                "national_id": "000",
            })
            pipe.sadd(f"patient:visits:{pid}", "0")
            pipe.execute()
            r.delete(f"patient:{pid}", f"patient:visits:{pid}")

        _run("DELETE", "delete_patient", d3)

        def d4():
            sid = random.randint(10_000_000, 99_999_999)
            r.hset(f"service:perf:{sid}", mapping={
                "total_price": "100.0", "visit_id": "1",
            })
            r.delete(f"service:perf:{sid}")

        _run("DELETE", "delete_performed_service", d4)

        def d5():
            rxid = random.randint(10_000_000, 99_999_999)
            r.hset(f"prescription:{rxid}", mapping={
                "code": "RX-DEL", "visit_id": "1",
                "med_1": "10", "dosage_1": "1x100mg",
            })
            r.delete(f"prescription:{rxid}")

        _run("DELETE", "delete_prescription", d5)

        def d6():
            vid = random.randint(10_000_000, 99_999_999)
            pid = random.randint(10_000_000, 99_999_999)
            pipe = r.pipeline()
            pipe.hset(f"patient:{pid}", mapping={"first_name": "V", "gender": "F"})
            pipe.set(f"visit:status:{vid}", "scheduled")
            pipe.set(f"visit:doctor:{vid}", "1")
            pipe.sadd(f"patient:visits:{pid}", str(vid))
            pipe.execute()
            r.delete(
                f"patient:{pid}", f"visit:status:{vid}",
                f"visit:doctor:{vid}", f"patient:visits:{pid}",
            )

        _run("DELETE", "delete_visit_cascade", d6)

    # ═══════════════════════════════════════════════════════════════════
    #  Zapis wyników
    # ═══════════════════════════════════════════════════════════════════

    def _save_results(self, filename: str):
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)
            for row in self.results:
                writer.writerow(row)
