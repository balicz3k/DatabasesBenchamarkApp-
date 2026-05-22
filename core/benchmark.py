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
    "Median_Time_Seconds", "StdDev_Time_Seconds",
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

        # Nasionko losowości – ustawiane przez run_benchmarks dla reprodukowalności
        self._rng_seed: Optional[int] = None

    def _rid(self, max_id: int) -> int:
        return random.randint(1, max(1, max_id))

    def _mongo_pid_with_visits(self, db) -> int:
        """Zwraca losowy _id pacjenta który ma co najmniej jedną wizytę (MongoDB)."""
        if self._cached_mongo_pids is None:
            # Duży sample (50k) eliminuje bias przy skalach 1M+
            docs = list(
                db.patients.aggregate([
                    {"$match": {"visits.0": {"$exists": True}}},
                    {"$sample": {"size": 50_000}},
                    {"$project": {"_id": 1}},
                ])
            )
            self._cached_mongo_pids = (
                [d["_id"] for d in docs] if docs else list(range(1, 11))
            )
        return random.choice(self._cached_mongo_pids)

    @staticmethod
    def _stats(times):
        times_sorted = sorted(times)
        n = len(times_sorted)
        mean = sum(times_sorted) / n
        median = (
            times_sorted[n // 2]
            if n % 2 == 1
            else (times_sorted[n // 2 - 1] + times_sorted[n // 2]) / 2
        )
        stdev = (sum((t - mean) ** 2 for t in times_sorted) / n) ** 0.5
        return median, mean, stdev

    @staticmethod
    def _avg_time(func, cleanup=None, runs: int = RUNS):
        """Wykonuje runs pomiarów + 1 warmup.
        Zwraca (median, mean, stdev).
        Opcjonalny cleanup() wywoływany po każdym wywołaniu (poza pomiarem).
        """
        func()  # warm-up – nie liczony do statystyk
        if cleanup:
            cleanup()
        times = []
        for _ in range(runs):
            start = time.perf_counter()
            func()
            elapsed = time.perf_counter() - start
            times.append(elapsed)
            if cleanup:
                cleanup()
        return BenchmarkEngine._stats(times)

    @staticmethod
    def _avg_time_split(measure_fn, setup_fn=None, teardown_fn=None,
                        runs: int = RUNS):
        """Pomiar z oddzieleniem setup/measure/teardown.

        Sekwencja per iteracja:
          ctx = setup_fn()           # POZA timerem
          start = perf_counter()
          measure_fn(ctx)            # MIERZONE
          times.append(elapsed)
          teardown_fn(ctx)           # POZA timerem

        Dzięki temu timer obejmuje TYLKO właściwą operację CRUD,
        bez inserta przygotowawczego (DELETE) i bez sprzątania (CREATE).
        """
        def _do_setup():
            return setup_fn() if setup_fn else None

        def _do_teardown(ctx):
            if teardown_fn:
                teardown_fn(ctx)

        # Warmup – nie wliczany
        ctx = _do_setup()
        measure_fn(ctx)
        _do_teardown(ctx)

        times = []
        for _ in range(runs):
            ctx = _do_setup()
            start = time.perf_counter()
            measure_fn(ctx)
            elapsed = time.perf_counter() - start
            times.append(elapsed)
            _do_teardown(ctx)
        return BenchmarkEngine._stats(times)

    def _record(self, db_name: str, op: str, scenario: str,
                mean: float, median: float, stdev: float):
        self.results.append((
            db_name, self.scale, self.is_indexed, op, scenario,
            mean, median, stdev,
        ))

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
        if self._rng_seed is not None:
            random.seed(self._rng_seed)

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
            median, mean, stdev = self._avg_time(func)
            self._record(db_name, op, name, mean, median, stdev)

        def _run_split(op: str, name: str, measure_fn, setup_fn=None,
                       teardown_fn=None):
            median, mean, stdev = self._avg_time_split(
                measure_fn, setup_fn=setup_fn, teardown_fn=teardown_fn)
            self._record(db_name, op, name, mean, median, stdev)

        # ── CREATE (6 scenariuszy) ─────────────────────────────────────

        def c1_setup():
            return random.randint(10_000_000, 99_999_999)

        def c1_measure(pid):
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO patients (id,national_id,first_name,last_name,birth_date,gender) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (pid, "99999999999", "TestName", "TestSurname", "2000-01-01", "M"),
            )
            cur.close()

        def c1_teardown(pid):
            cur = conn.cursor()
            cur.execute("DELETE FROM patients WHERE id=%s", (pid,))
            cur.close()

        _run_split("CREATE", "insert_patient", c1_measure, c1_setup, c1_teardown)

        def c2_setup():
            return (random.randint(10_000_000, 99_999_999),
                    self._rid(self.max_patient), self._rid(self.max_doctor))

        def c2_measure(ctx):
            vid, pid, did = ctx
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO visits (id,patient_id,doctor_id,visit_date,status) "
                "VALUES (%s,%s,%s,%s,%s)",
                (vid, pid, did, "2025-06-01", "scheduled"),
            )
            cur.close()

        def c2_teardown(ctx):
            vid, _, _ = ctx
            cur = conn.cursor()
            cur.execute("DELETE FROM visits WHERE id=%s", (vid,))
            cur.close()

        _run_split("CREATE", "insert_visit", c2_measure, c2_setup, c2_teardown)

        def c3_setup():
            return (random.randint(10_000_000, 99_999_999),
                    self._rid(self.max_visit), self._rid(self.max_disease))

        def c3_measure(ctx):
            did, vid, dis = ctx
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO diagnoses (id,visit_id,disease_id,diagnosis_type,notes) "
                "VALUES (%s,%s,%s,%s,%s)",
                (did, vid, dis, "primary", "bench note"),
            )
            cur.close()

        def c3_teardown(ctx):
            did, _, _ = ctx
            cur = conn.cursor()
            cur.execute("DELETE FROM diagnoses WHERE id=%s", (did,))
            cur.close()

        _run_split("CREATE", "insert_diagnosis", c3_measure, c3_setup, c3_teardown)

        def c4_setup():
            return (random.randint(10_000_000, 99_999_999),
                    random.randint(10_000_000, 99_999_999),
                    self._rid(self.max_visit), self._rid(self.max_medication))

        def c4_measure(ctx):
            pid, iid, vid, mid = ctx
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO prescriptions (id,visit_id,prescription_code,issue_date) "
                "VALUES (%s,%s,%s,%s)",
                (pid, vid, "RX-BENCH", "2025-06-01"),
            )
            cur.execute(
                "INSERT INTO prescription_items (id,prescription_id,medication_id,dosage) "
                "VALUES (%s,%s,%s,%s)",
                (iid, pid, mid, "1x500mg"),
            )
            cur.close()

        def c4_teardown(ctx):
            pid, iid, _, _ = ctx
            cur = conn.cursor()
            cur.execute("DELETE FROM prescription_items WHERE id=%s", (iid,))
            cur.execute("DELETE FROM prescriptions WHERE id=%s", (pid,))
            cur.close()

        _run_split("CREATE", "insert_prescription_with_items",
                   c4_measure, c4_setup, c4_teardown)

        def c5_setup():
            return (random.randint(10_000_000, 99_999_999),
                    self._rid(self.max_visit), self._rid(self.max_service))

        def c5_measure(ctx):
            sid, vid, srv = ctx
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO performed_services (id,visit_id,service_id,quantity,final_price) "
                "VALUES (%s,%s,%s,%s,%s)",
                (sid, vid, srv, 1, 199.99),
            )
            cur.close()

        def c5_teardown(ctx):
            sid, _, _ = ctx
            cur = conn.cursor()
            cur.execute("DELETE FROM performed_services WHERE id=%s", (sid,))
            cur.close()

        _run_split("CREATE", "insert_performed_service",
                   c5_measure, c5_setup, c5_teardown)

        def c6_setup():
            return (random.randint(10_000_000, 99_999_999), self._rid(self.max_visit))

        def c6_measure(ctx):
            tid, vid = ctx
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO test_results (id,visit_id,parameter_name,result_value,unit,min_norm,max_norm) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (tid, vid, "Hemoglobina", 13.5, "g/dL", 12.0, 16.0),
            )
            cur.close()

        def c6_teardown(ctx):
            tid, _ = ctx
            cur = conn.cursor()
            cur.execute("DELETE FROM test_results WHERE id=%s", (tid,))
            cur.close()

        _run_split("CREATE", "insert_test_result", c6_measure, c6_setup, c6_teardown)

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
                "WHERE p.id = %s LIMIT 50",
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
                "GROUP BY v.id LIMIT 50",
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
                "WHERE pr.visit_id = %s LIMIT 50",
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
        # Wzorzec: setup wstawia wiersz (poza timerem), measure wykonuje
        # TYLKO DELETE. Dzięki temu timer mierzy czysty koszt DELETE –
        # bez wliczania czasu INSERT-a przygotowawczego.

        def d1_setup():
            tid = random.randint(10_000_000, 99_999_999)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO test_results (id,visit_id,parameter_name,result_value,unit,min_norm,max_norm) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (tid, 1, "TMP", 1.0, "U", 0.0, 1.0),
            )
            cur.close()
            return tid

        def d1_measure(tid):
            cur = conn.cursor()
            cur.execute("DELETE FROM test_results WHERE id=%s", (tid,))
            cur.close()

        _run_split("DELETE", "delete_test_result", d1_measure, d1_setup)

        def d2_setup():
            did = random.randint(10_000_000, 99_999_999)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO diagnoses (id,visit_id,disease_id,diagnosis_type,notes) "
                "VALUES (%s,%s,%s,%s,%s)",
                (did, 1, 1, "primary", "tmp"),
            )
            cur.close()
            return did

        def d2_measure(did):
            cur = conn.cursor()
            cur.execute("DELETE FROM diagnoses WHERE id=%s", (did,))
            cur.close()

        _run_split("DELETE", "delete_diagnosis", d2_measure, d2_setup)

        def d3_setup():
            pid = random.randint(10_000_000, 99_999_999)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO patients (id,national_id,first_name,last_name,birth_date,gender) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (pid, "00000000000", "Del", "Test", "2000-01-01", "M"),
            )
            cur.close()
            return pid

        def d3_measure(pid):
            cur = conn.cursor()
            cur.execute("DELETE FROM patients WHERE id=%s", (pid,))
            cur.close()

        _run_split("DELETE", "delete_patient", d3_measure, d3_setup)

        def d4_setup():
            sid = random.randint(10_000_000, 99_999_999)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO performed_services (id,visit_id,service_id,quantity,final_price) "
                "VALUES (%s,%s,%s,%s,%s)",
                (sid, 1, 1, 1, 100.0),
            )
            cur.close()
            return sid

        def d4_measure(sid):
            cur = conn.cursor()
            cur.execute("DELETE FROM performed_services WHERE id=%s", (sid,))
            cur.close()

        _run_split("DELETE", "delete_performed_service", d4_measure, d4_setup)

        def d5_setup():
            rxid = random.randint(10_000_000, 99_999_999)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO prescriptions (id,visit_id,prescription_code,issue_date) "
                "VALUES (%s,%s,%s,%s)",
                (rxid, 1, "RX-DEL", "2025-01-01"),
            )
            cur.close()
            return rxid

        def d5_measure(rxid):
            cur = conn.cursor()
            cur.execute("DELETE FROM prescriptions WHERE id=%s", (rxid,))
            cur.close()

        _run_split("DELETE", "delete_prescription", d5_measure, d5_setup)

        def d6_setup():
            vid = random.randint(10_000_000, 99_999_999)
            pid_tmp = random.randint(10_000_000, 99_999_999)
            cur = conn.cursor()
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
            cur.close()
            return (vid, pid_tmp)

        def d6_measure(ctx):
            vid, _ = ctx
            cur = conn.cursor()
            cur.execute("DELETE FROM visits WHERE id=%s", (vid,))
            cur.close()

        def d6_teardown(ctx):
            _, pid_tmp = ctx
            cur = conn.cursor()
            cur.execute("DELETE FROM patients WHERE id=%s", (pid_tmp,))
            cur.close()

        _run_split("DELETE", "delete_visit_cascade",
                   d6_measure, d6_setup, d6_teardown)

    # ═══════════════════════════════════════════════════════════════════
    #  Scenariusze MongoDB – 24 scenariusze
    # ═══════════════════════════════════════════════════════════════════

    def _mongo_scenarios(self):
        db = self.cm.get_connector(DatabaseType.MONGODB).get_db()
        db_name = DatabaseType.MONGODB.value

        def _run(op: str, name: str, func, cleanup=None):
            median, mean, stdev = self._avg_time(func, cleanup=cleanup)
            self._record(db_name, op, name, mean, median, stdev)

        def _run_split(op: str, name: str, measure_fn, setup_fn=None,
                       teardown_fn=None):
            median, mean, stdev = self._avg_time_split(
                measure_fn, setup_fn=setup_fn, teardown_fn=teardown_fn)
            self._record(db_name, op, name, mean, median, stdev)

        # ── CREATE ────────────────────────────────────────────────────
        # Wzorzec: setup poza timerem (przygotowuje ID/pid),
        # measure wykonuje tylko zapis, teardown sprząta (poza timerem).

        def mc1_setup():
            return random.randint(10_000_000, 99_999_999)

        def mc1_measure(pid):
            db.patients.insert_one({
                "_id": pid, "first_name": "T", "last_name": "P",
                "national_id": "00000", "gender": "M", "visits": [],
            })

        def mc1_teardown(pid):
            db.patients.delete_one({"_id": pid})

        _run_split("CREATE", "insert_patient",
                   mc1_measure, mc1_setup, mc1_teardown)

        def mc2_setup():
            return (
                random.randint(10_000_000, 99_999_999),
                self._rid(self.max_patient),
                self._rid(self.max_doctor),
            )

        def mc2_measure(ctx):
            bench_vid, pid, did = ctx
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits": {
                    "visit_id": bench_vid,
                    "doctor_id": did,
                    "status": "scheduled",
                    "visit_date": "2025-06-01",
                    "diagnoses": [], "prescriptions": [],
                    "performed_services": [], "test_results": [],
                }}},
            )

        def mc2_teardown(ctx):
            bench_vid, pid, _ = ctx
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits": {"visit_id": bench_vid}}},
            )

        _run_split("CREATE", "insert_visit",
                   mc2_measure, mc2_setup, mc2_teardown)

        def mc3_setup():
            return self._mongo_pid_with_visits(db)

        def mc3_measure(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits.0.diagnoses": {
                    "disease_id": self._rid(self.max_disease),
                    "diagnosis_type": "primary",
                    "notes": "bench",
                    "_bench": True,
                }}},
            )

        def mc3_teardown(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits.0.diagnoses": {"_bench": True}}},
            )

        _run_split("CREATE", "insert_diagnosis",
                   mc3_measure, mc3_setup, mc3_teardown)

        def mc4_measure(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits.0.prescriptions": {
                    "prescription_code": "RX-NEW",
                    "issue_date": "2025-06-01",
                    "items": [{"medication_id": self._rid(self.max_medication),
                               "dosage": "1x200mg"}],
                    "_bench": True,
                }}},
            )

        def mc4_teardown(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits.0.prescriptions": {"_bench": True}}},
            )

        _run_split("CREATE", "insert_prescription_with_items",
                   mc4_measure, mc3_setup, mc4_teardown)

        def mc5_measure(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits.0.performed_services": {
                    "service_id": self._rid(self.max_service),
                    "quantity": 1, "final_price": 100.0,
                    "_bench": True,
                }}},
            )

        def mc5_teardown(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits.0.performed_services": {"_bench": True}}},
            )

        _run_split("CREATE", "insert_performed_service",
                   mc5_measure, mc3_setup, mc5_teardown)

        def mc6_measure(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits.0.test_results": {
                    "parameter_name": "Glukoza", "result_value": 90.0,
                    "unit": "mg/dL", "min_norm": 70.0, "max_norm": 110.0,
                    "_bench": True,
                }}},
            )

        def mc6_teardown(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits.0.test_results": {"_bench": True}}},
            )

        _run_split("CREATE", "insert_test_result",
                   mc6_measure, mc3_setup, mc6_teardown)

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
                {"$limit": 50},
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
                {"$group": {"_id": "$visits.visit_id",
                            "total": {"$sum": "$visits.performed_services.final_price"}}},
                {"$limit": 50},
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
        # Wzorzec: setup wykonuje $push tymczasowego elementu (poza timerem),
        # measure wykonuje TYLKO $pull (czysty pomiar usunięcia).

        def md1_setup():
            pid = self._mongo_pid_with_visits(db)
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits.0.test_results": {"parameter_name": "TMP_DEL"}}},
            )
            return pid

        def md1_measure(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits.0.test_results": {"parameter_name": "TMP_DEL"}}},
            )

        _run_split("DELETE", "delete_test_result", md1_measure, md1_setup)

        def md2_setup():
            pid = self._mongo_pid_with_visits(db)
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits.0.diagnoses": {"disease_id": 99999}}},
            )
            return pid

        def md2_measure(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits.0.diagnoses": {"disease_id": 99999}}},
            )

        _run_split("DELETE", "delete_diagnosis", md2_measure, md2_setup)

        def md3_setup():
            pid = random.randint(10_000_000, 99_999_999)
            db.patients.insert_one({"_id": pid, "visits": []})
            return pid

        def md3_measure(pid):
            db.patients.delete_one({"_id": pid})

        _run_split("DELETE", "delete_patient", md3_measure, md3_setup)

        def md4_setup():
            pid = self._mongo_pid_with_visits(db)
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits.0.performed_services": {"service_id": 99999}}},
            )
            return pid

        def md4_measure(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits.0.performed_services": {"service_id": 99999}}},
            )

        _run_split("DELETE", "delete_performed_service", md4_measure, md4_setup)

        def md5_setup():
            pid = self._mongo_pid_with_visits(db)
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits.0.prescriptions": {"prescription_code": "RX-DEL"}}},
            )
            return pid

        def md5_measure(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits.0.prescriptions": {"prescription_code": "RX-DEL"}}},
            )

        _run_split("DELETE", "delete_prescription", md5_measure, md5_setup)

        def md6_setup():
            pid = self._rid(self.max_patient)
            db.patients.update_one(
                {"_id": pid},
                {"$push": {"visits": {"visit_id": 99999999}}},
            )
            return pid

        def md6_measure(pid):
            db.patients.update_one(
                {"_id": pid},
                {"$pull": {"visits": {"visit_id": 99999999}}},
            )

        _run_split("DELETE", "delete_visit_cascade", md6_measure, md6_setup)

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
            median, mean, stdev = self._avg_time(func)
            self._record(db_name, op, name, mean, median, stdev)

        def _run_split(op: str, name: str, measure_fn, setup_fn=None,
                       teardown_fn=None):
            median, mean, stdev = self._avg_time_split(
                measure_fn, setup_fn=setup_fn, teardown_fn=teardown_fn)
            self._record(db_name, op, name, mean, median, stdev)

        # ── CREATE ────────────────────────────────────────────────────
        # Wzorzec: setup (poza timerem) generuje ID/kontekst,
        # measure wykonuje TYLKO zapis, teardown czyści (poza timerem).

        def rc1_setup():
            return random.randint(10_000_000, 99_999_999)

        def rc1_measure(pid):
            pipe = r.pipeline()
            pipe.hset(f"patient:{pid}", mapping={
                "first_name": "T", "last_name": "P",
                "national_id": "000", "gender": "M",
            })
            pipe.sadd(f"patient:visits:{pid}", "0")
            pipe.execute()

        def rc1_teardown(pid):
            r.delete(f"patient:{pid}", f"patient:visits:{pid}")

        _run_split("CREATE", "insert_patient", rc1_measure, rc1_setup, rc1_teardown)

        def rc2_setup():
            return (
                random.randint(10_000_000, 99_999_999),
                self._rid(self.max_patient),
                self._rid(self.max_doctor),
            )

        def rc2_measure(ctx):
            vid, pid, did = ctx
            pipe = r.pipeline()
            pipe.set(f"visit:status:{vid}", "scheduled")
            pipe.set(f"visit:doctor:{vid}", str(did))
            pipe.sadd(f"patient:visits:{pid}", str(vid))
            pipe.execute()

        def rc2_teardown(ctx):
            vid, pid, _ = ctx
            pipe2 = r.pipeline()
            pipe2.delete(f"visit:status:{vid}", f"visit:doctor:{vid}")
            pipe2.srem(f"patient:visits:{pid}", str(vid))
            pipe2.execute()

        _run_split("CREATE", "insert_visit", rc2_measure, rc2_setup, rc2_teardown)

        def rc3_setup():
            return (
                self._rid(self.max_visit),
                f"{self._rid(self.max_disease)}:primary:bench",
            )

        def rc3_measure(ctx):
            vid, val = ctx
            r.rpush(f"visit:diag:{vid}", val)

        def rc3_teardown(ctx):
            vid, val = ctx
            r.lrem(f"visit:diag:{vid}", 1, val)

        _run_split("CREATE", "insert_diagnosis", rc3_measure, rc3_setup, rc3_teardown)

        def rc4_setup():
            return random.randint(10_000_000, 99_999_999)

        def rc4_measure(rxid):
            r.hset(f"prescription:{rxid}", mapping={
                "visit_id": str(self._rid(self.max_visit)),
                "code": "RX-NEW",
                "issue_date": "2025-06-01",
                "med_1": str(self._rid(self.max_medication)),
                "dosage_1": "1x500mg",
                "med_2": str(self._rid(self.max_medication)),
                "dosage_2": "2x200mg",
            })

        def rc4_teardown(rxid):
            r.delete(f"prescription:{rxid}")

        _run_split("CREATE", "insert_prescription_with_items",
                   rc4_measure, rc4_setup, rc4_teardown)

        def rc5_setup():
            return (
                random.randint(10_000_000, 99_999_999),
                self._rid(self.max_visit),
            )

        def rc5_measure(ctx):
            sid, vid = ctx
            pipe = r.pipeline()
            pipe.hset(f"service:perf:{sid}", mapping={
                "visit_id": str(vid), "service_id": str(self._rid(self.max_service)),
                "quantity": "1", "final_price": "199.99",
            })
            pipe.hincrbyfloat(f"service:total:{vid}", "total_price", 199.99)
            pipe.execute()

        def rc5_teardown(ctx):
            sid, vid = ctx
            pipe2 = r.pipeline()
            pipe2.delete(f"service:perf:{sid}")
            pipe2.hincrbyfloat(f"service:total:{vid}", "total_price", -199.99)
            pipe2.execute()

        _run_split("CREATE", "insert_performed_service",
                   rc5_measure, rc5_setup, rc5_teardown)

        def rc6_setup():
            return random.randint(10_000_000, 99_999_999)

        def rc6_measure(tid):
            r.hset(f"test:{tid}", mapping={
                "visit_id": str(self._rid(self.max_visit)),
                "parameter": "Hemoglobina", "value": "13.5",
                "unit": "g/dL", "min_norm": "12.0", "max_norm": "16.0",
            })

        def rc6_teardown(tid):
            r.delete(f"test:{tid}")

        _run_split("CREATE", "insert_test_result",
                   rc6_measure, rc6_setup, rc6_teardown)

        # ── READ ──────────────────────────────────────────────────────
        # Symulacja JOINów przez wielokluczowe pipeline'y.
        # Wzorzec: setup (smembers/lrange + slice do LIMIT 50) poza timerem,
        # measure wykonuje tylko właściwe odczyty wielokluczowe.

        _run("READ", "select_patient_by_id",
             lambda: r.hgetall(f"patient:{self._rid(self.max_patient)}"))

        def rr2_setup():
            pid = self._rid(self.max_patient)
            visit_ids = r.smembers(f"patient:visits:{pid}")
            return list(visit_ids)[:50] if visit_ids else []

        def rr2_measure(sample):
            if not sample:
                return
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
                for did in list(doctor_ids)[:50]:
                    pipe2.hgetall(f"session:doctor:{did}")
                pipe2.execute()

        _run_split("READ", "select_visits_with_doctor", rr2_measure, rr2_setup)

        def rr3_setup():
            vid = self._rid(self.max_visit)
            diagnoses = r.lrange(f"visit:diag:{vid}", 0, -1)
            return diagnoses[:50] if diagnoses else []

        def rr3_measure(diagnoses):
            if not diagnoses:
                return
            pipe = r.pipeline()
            for d in diagnoses:
                parts = d.split(":")
                if parts:
                    pipe.exists(f"disease:{parts[0]}")
            pipe.execute()

        _run_split("READ", "select_visit_diagnoses", rr3_measure, rr3_setup)

        def rr4_setup():
            pid = self._rid(self.max_patient)
            visit_ids = r.smembers(f"patient:visits:{pid}")
            sample = list(visit_ids)[:50] if visit_ids else []
            return (pid, sample)

        def rr4_measure(ctx):
            pid, sample = ctx
            pipe = r.pipeline()
            pipe.hgetall(f"patient:{pid}")
            for vid in sample:
                pipe.get(f"visit:status:{vid}")
                pipe.lrange(f"visit:diag:{vid}", 0, -1)
                pipe.hgetall(f"service:total:{vid}")
            pipe.execute()

        _run_split("READ", "select_patient_full_history", rr4_measure, rr4_setup)

        def rr5_setup():
            pid = self._rid(self.max_patient)
            visit_ids = r.smembers(f"patient:visits:{pid}")
            return list(visit_ids)[:50] if visit_ids else []

        def rr5_measure(sample):
            if not sample:
                return 0.0
            pipe = r.pipeline()
            for vid in sample:
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

        _run_split("READ", "select_aggregated_costs", rr5_measure, rr5_setup)

        def rr6_setup():
            vid = self._rid(self.max_visit)
            rx_ids = r.smembers(f"visit:prescriptions:{vid}")
            return list(rx_ids)[:50] if rx_ids else []

        def rr6_measure(rx_ids):
            if not rx_ids:
                return
            pipe = r.pipeline()
            for rxid in rx_ids:
                pipe.hgetall(f"prescription:{rxid}")
            pipe.execute()

        _run_split("READ", "select_prescriptions_with_meds", rr6_measure, rr6_setup)

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
        # Wzorzec: setup tworzy klucze (poza timerem), measure wykonuje TYLKO DELETE.

        def rd1_setup():
            tid = random.randint(10_000_000, 99_999_999)
            r.hset(f"test:{tid}", mapping={
                "parameter": "Hemo", "value": "13.5", "unit": "g/dL",
                "min_norm": "12.0", "max_norm": "16.0",
            })
            return tid

        def rd1_measure(tid):
            r.delete(f"test:{tid}")

        _run_split("DELETE", "delete_test_result", rd1_measure, rd1_setup)

        def rd2_setup():
            vid = self._rid(self.max_visit)
            r.rpush(f"visit:diag:{vid}", "99999:primary:tmp")
            return vid

        def rd2_measure(vid):
            r.lrem(f"visit:diag:{vid}", 1, "99999:primary:tmp")

        _run_split("DELETE", "delete_diagnosis", rd2_measure, rd2_setup)

        def rd3_setup():
            pid = random.randint(10_000_000, 99_999_999)
            pipe = r.pipeline()
            pipe.hset(f"patient:{pid}", mapping={
                "first_name": "T", "last_name": "P", "gender": "M",
                "national_id": "000",
            })
            pipe.sadd(f"patient:visits:{pid}", "0")
            pipe.execute()
            return pid

        def rd3_measure(pid):
            r.delete(f"patient:{pid}", f"patient:visits:{pid}")

        _run_split("DELETE", "delete_patient", rd3_measure, rd3_setup)

        def rd4_setup():
            sid = random.randint(10_000_000, 99_999_999)
            r.hset(f"service:perf:{sid}", mapping={
                "total_price": "100.0", "visit_id": "1",
            })
            return sid

        def rd4_measure(sid):
            r.delete(f"service:perf:{sid}")

        _run_split("DELETE", "delete_performed_service", rd4_measure, rd4_setup)

        def rd5_setup():
            rxid = random.randint(10_000_000, 99_999_999)
            r.hset(f"prescription:{rxid}", mapping={
                "code": "RX-DEL", "visit_id": "1",
                "med_1": "10", "dosage_1": "1x100mg",
            })
            return rxid

        def rd5_measure(rxid):
            r.delete(f"prescription:{rxid}")

        _run_split("DELETE", "delete_prescription", rd5_measure, rd5_setup)

        def rd6_setup():
            vid = random.randint(10_000_000, 99_999_999)
            pid = random.randint(10_000_000, 99_999_999)
            pipe = r.pipeline()
            pipe.hset(f"patient:{pid}", mapping={"first_name": "V", "gender": "F"})
            pipe.set(f"visit:status:{vid}", "scheduled")
            pipe.set(f"visit:doctor:{vid}", "1")
            pipe.sadd(f"patient:visits:{pid}", str(vid))
            pipe.execute()
            return (pid, vid)

        def rd6_measure(ctx):
            pid, vid = ctx
            r.delete(
                f"patient:{pid}", f"visit:status:{vid}",
                f"visit:doctor:{vid}", f"patient:visits:{pid}",
            )

        _run_split("DELETE", "delete_visit_cascade", rd6_measure, rd6_setup)

    # ═══════════════════════════════════════════════════════════════════
    #  Zapis wyników
    # ═══════════════════════════════════════════════════════════════════

    def _save_results(self, filename: str):
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)
            for row in self.results:
                writer.writerow(row)
