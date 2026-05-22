"""
core/seeder.py – Masowe wstawianie danych (Bulk Insert) do 4 systemów bazodanowych
oraz tworzenie indeksów i schematów.

Model danych Redis:
  patient:{id}            HASH  {first_name, last_name, national_id, gender}
  patient:visits:{pid}    SET   {visit_id, ...}
  visit:status:{id}       STRING {status}
  visit:doctor:{vid}      STRING {doctor_id}
  session:doctor:{id}     HASH  {first_name, last_name, license_number, department_id, specialization_id}
  visit:diag:{visit_id}   LIST  ["disease_id:type", ...]
  prescription:{id}       HASH  {visit_id, code, issue_date}
  service:total:{vid}     HASH  {total_price, visit_id}
  test:{visit_id}         HASH  {parameter, value, unit}
  department:{id}         HASH  {name, phone}
"""

from typing import Generator

from psycopg2.extras import execute_values

from core.database import ConnectionManager, DatabaseType
from core.generator import GeneratedData

SQL_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS departments (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS specializations (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);
CREATE TABLE IF NOT EXISTS diseases (
    id INT PRIMARY KEY,
    icd10_code VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL
);
CREATE TABLE IF NOT EXISTS medical_services (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    base_price DECIMAL(10,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS medications (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    active_substance VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS patients (
    id INT PRIMARY KEY,
    national_id VARCHAR(20) NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    birth_date DATE,
    gender CHAR(1)
);
CREATE TABLE IF NOT EXISTS doctors (
    id INT PRIMARY KEY,
    department_id INT,
    specialization_id INT,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    license_number VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS visits (
    id INT PRIMARY KEY,
    patient_id INT,
    doctor_id INT,
    visit_date DATE,
    status VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS performed_services (
    id INT PRIMARY KEY,
    visit_id INT,
    service_id INT,
    quantity INT,
    final_price DECIMAL(10,2)
);
CREATE TABLE IF NOT EXISTS diagnoses (
    id INT PRIMARY KEY,
    visit_id INT,
    disease_id INT,
    diagnosis_type VARCHAR(20),
    notes TEXT
);
CREATE TABLE IF NOT EXISTS prescriptions (
    id INT PRIMARY KEY,
    visit_id INT,
    prescription_code VARCHAR(30),
    issue_date DATE
);
CREATE TABLE IF NOT EXISTS prescription_items (
    id INT PRIMARY KEY,
    prescription_id INT,
    medication_id INT,
    dosage VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS test_results (
    id INT PRIMARY KEY,
    visit_id INT,
    parameter_name VARCHAR(50),
    result_value DECIMAL(10,2),
    unit VARCHAR(20),
    min_norm DECIMAL(10,2),
    max_norm DECIMAL(10,2)
);
"""

SQL_ALTER_FK = [
    "ALTER TABLE doctors ADD CONSTRAINT fk_doc_dep FOREIGN KEY (department_id) REFERENCES departments(id)",
    "ALTER TABLE doctors ADD CONSTRAINT fk_doc_spec FOREIGN KEY (specialization_id) REFERENCES specializations(id)",
    "ALTER TABLE visits ADD CONSTRAINT fk_vis_pat FOREIGN KEY (patient_id) REFERENCES patients(id)",
    "ALTER TABLE visits ADD CONSTRAINT fk_vis_doc FOREIGN KEY (doctor_id) REFERENCES doctors(id)",
    "ALTER TABLE performed_services ADD CONSTRAINT fk_ps_vis FOREIGN KEY (visit_id) REFERENCES visits(id) ON DELETE CASCADE",
    "ALTER TABLE performed_services ADD CONSTRAINT fk_ps_srv FOREIGN KEY (service_id) REFERENCES medical_services(id)",
    "ALTER TABLE diagnoses ADD CONSTRAINT fk_diag_vis FOREIGN KEY (visit_id) REFERENCES visits(id) ON DELETE CASCADE",
    "ALTER TABLE diagnoses ADD CONSTRAINT fk_diag_dis FOREIGN KEY (disease_id) REFERENCES diseases(id)",
    "ALTER TABLE prescriptions ADD CONSTRAINT fk_rx_vis FOREIGN KEY (visit_id) REFERENCES visits(id) ON DELETE CASCADE",
    "ALTER TABLE prescription_items ADD CONSTRAINT fk_rxi_rx FOREIGN KEY (prescription_id) REFERENCES prescriptions(id) ON DELETE CASCADE",
    "ALTER TABLE prescription_items ADD CONSTRAINT fk_rxi_med FOREIGN KEY (medication_id) REFERENCES medications(id)",
    "ALTER TABLE test_results ADD CONSTRAINT fk_tr_vis FOREIGN KEY (visit_id) REFERENCES visits(id) ON DELETE CASCADE",
]

SQL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_patients_last_name ON patients(last_name)",
    "CREATE INDEX IF NOT EXISTS idx_patients_national_id ON patients(national_id)",
    "CREATE INDEX IF NOT EXISTS idx_patients_gender ON patients(gender)",
    "CREATE INDEX IF NOT EXISTS idx_visits_patient_id ON visits(patient_id)",
    "CREATE INDEX IF NOT EXISTS idx_visits_doctor_id ON visits(doctor_id)",
    "CREATE INDEX IF NOT EXISTS idx_visits_status ON visits(status)",
    "CREATE INDEX IF NOT EXISTS idx_visits_date ON visits(visit_date)",
    "CREATE INDEX IF NOT EXISTS idx_performed_services_visit_id ON performed_services(visit_id)",
    "CREATE INDEX IF NOT EXISTS idx_diagnoses_visit_id ON diagnoses(visit_id)",
    "CREATE INDEX IF NOT EXISTS idx_diagnoses_disease_id ON diagnoses(disease_id)",
    "CREATE INDEX IF NOT EXISTS idx_prescriptions_visit_id ON prescriptions(visit_id)",
    "CREATE INDEX IF NOT EXISTS idx_prescription_items_prescription_id ON prescription_items(prescription_id)",
    "CREATE INDEX IF NOT EXISTS idx_test_results_visit_id ON test_results(visit_id)",
    "CREATE INDEX IF NOT EXISTS idx_test_results_parameter ON test_results(parameter_name)",
    "CREATE INDEX IF NOT EXISTS idx_doctors_department ON doctors(department_id)",
    "CREATE INDEX IF NOT EXISTS idx_doctors_specialization ON doctors(specialization_id)",
]

MYSQL_INDEXES = [
    ("patients", "idx_patients_last_name", "last_name"),
    ("patients", "idx_patients_national_id", "national_id"),
    ("patients", "idx_patients_gender", "gender"),
    ("visits", "idx_visits_patient_id", "patient_id"),
    ("visits", "idx_visits_doctor_id", "doctor_id"),
    ("visits", "idx_visits_status", "status"),
    ("visits", "idx_visits_date", "visit_date"),
    ("performed_services", "idx_performed_services_visit_id", "visit_id"),
    ("diagnoses", "idx_diagnoses_visit_id", "visit_id"),
    ("diagnoses", "idx_diagnoses_disease_id", "disease_id"),
    ("prescriptions", "idx_prescriptions_visit_id", "visit_id"),
    ("prescription_items", "idx_prescription_items_prescription_id", "prescription_id"),
    ("test_results", "idx_test_results_visit_id", "visit_id"),
    ("test_results", "idx_test_results_parameter", "parameter_name"),
    ("doctors", "idx_doctors_department", "department_id"),
    ("doctors", "idx_doctors_specialization", "specialization_id"),
]

DROP_ORDER = [
    "test_results", "prescription_items", "prescriptions",
    "diagnoses", "performed_services", "visits", "doctors",
    "patients", "medications", "medical_services",
    "diseases", "specializations", "departments",
]

TABLE_INSERTS = [
    ("departments", 3),
    ("specializations", 2),
    ("diseases", 3),
    ("medical_services", 3),
    ("medications", 3),
    ("patients", 6),
    ("doctors", 6),
    ("visits", 5),
    ("performed_services", 5),
    ("diagnoses", 5),
    ("prescriptions", 4),
    ("prescription_items", 4),
    ("test_results", 7),
]


class DatabaseSeeder:
    """Zarządza wstawianiem danych do wszystkich systemów bazodanowych."""

    BATCH_SIZE = 5000

    def __init__(self, connection_manager: ConnectionManager):
        self.cm = connection_manager

    @staticmethod
    def _chunked(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i : i + size]

    def seed_all(self, data: GeneratedData, progress_callback=None,
                  mongo_doc_gen=None):
        """Seeduje wszystkie 4 bazy danych.
        mongo_doc_gen: opcjonalny generator dokumentow MongoDB (streaming).
        Jesli None, a data.mongo_patients jest niepuste, uzywa listy.
        """
        self._seed_sql(DatabaseType.POSTGRES, data, progress_callback)
        self._seed_sql(DatabaseType.MYSQL, data, progress_callback)
        if mongo_doc_gen is not None:
            self._seed_mongodb_streaming(mongo_doc_gen, progress_callback)
        else:
            self._seed_mongodb(data, progress_callback)
        self._seed_redis(data, progress_callback)

    def seed_postgres(self, data: GeneratedData, progress_callback=None):
        self._seed_sql(DatabaseType.POSTGRES, data, progress_callback)

    def seed_mysql(self, data: GeneratedData, progress_callback=None):
        self._seed_sql(DatabaseType.MYSQL, data, progress_callback)

    def seed_mongo_streaming(self, doc_gen, progress_callback=None):
        self._seed_mongodb_streaming(doc_gen, progress_callback)

    def seed_redis_data(self, data: GeneratedData, progress_callback=None):
        self._seed_redis(data, progress_callback)

    # ── Streaming large-scale seeding (≥5 M visits) ──────────────────────────

    _BASE_TABLE_INSERTS = [
        ("departments", 3),
        ("specializations", 2),
        ("diseases", 3),
        ("medical_services", 3),
        ("medications", 3),
        ("patients", 6),
        ("doctors", 6),
    ]

    _VISIT_TABLE_INSERTS = [
        ("visits", 5),
        ("performed_services", 5),
        ("diagnoses", 5),
        ("prescriptions", 4),
        ("prescription_items", 4),
        ("test_results", 7),
    ]

    def seed_sql_schema_and_base(self, data: GeneratedData, progress_callback=None):
        """Tworzy schemat SQL i seeduje dane bazowe (bez wizyt i tabel podrzędnych)."""
        for db_type in [DatabaseType.POSTGRES, DatabaseType.MYSQL]:
            db_name = db_type.value

            def _report(msg, _dn=db_name):
                if progress_callback:
                    progress_callback(f"[{_dn}] {msg}")

            connector = self.cm.get_connector(db_type)
            conn = connector.get_connection()
            cur = conn.cursor()

            _report("DROP tabel...")
            if db_type == DatabaseType.MYSQL:
                cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                for tbl in DROP_ORDER:
                    cur.execute(f"DROP TABLE IF EXISTS {tbl}")
                cur.execute("SET FOREIGN_KEY_CHECKS = 1")
            else:
                for tbl in DROP_ORDER:
                    cur.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")

            _report("CREATE tabel...")
            for stmt in SQL_CREATE_TABLES.split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)

            _report("Dodawanie FK...")
            for fk_sql in SQL_ALTER_FK:
                try:
                    cur.execute(fk_sql)
                except Exception:
                    pass

            data_map = {
                "departments": data.departments,
                "specializations": data.specializations,
                "diseases": data.diseases,
                "medical_services": data.medical_services,
                "medications": data.medications,
                "patients": data.patients,
                "doctors": data.doctors,
            }

            for table_name, n_cols in self._BASE_TABLE_INSERTS:
                rows = data_map[table_name]
                if not rows:
                    continue
                _report(f"Wstawianie: {table_name} ({len(rows)} wierszy)")
                if db_type == DatabaseType.POSTGRES:
                    for batch in self._chunked(rows, self.BATCH_SIZE * 4):
                        execute_values(cur, f"INSERT INTO {table_name} VALUES %s", batch)
                else:
                    placeholders = ",".join(["%s"] * n_cols)
                    sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                    for batch in self._chunked(rows, self.BATCH_SIZE):
                        cur.executemany(sql, batch)

            cur.close()
            _report("Schemat + dane bazowe gotowe.")

    def seed_sql_visit_chunk(
        self,
        visits: list,
        diagnoses: list,
        services: list,
        prescriptions: list,
        rx_items: list,
        test_results: list,
        progress_callback=None,
    ):
        """Wstawia jedną paczkę wizyt i tabel podrzędnych do PostgreSQL i MySQL."""
        chunk_data = [
            ("visits", 5, visits),
            ("performed_services", 5, services),
            ("diagnoses", 5, diagnoses),
            ("prescriptions", 4, prescriptions),
            ("prescription_items", 4, rx_items),
            ("test_results", 7, test_results),
        ]
        for db_type in [DatabaseType.POSTGRES, DatabaseType.MYSQL]:
            conn = self.cm.get_connector(db_type).get_connection()
            cur = conn.cursor()
            for table_name, n_cols, rows in chunk_data:
                if not rows:
                    continue
                if db_type == DatabaseType.POSTGRES:
                    for batch in self._chunked(rows, self.BATCH_SIZE * 4):
                        execute_values(cur, f"INSERT INTO {table_name} VALUES %s", batch)
                else:
                    placeholders = ",".join(["%s"] * n_cols)
                    sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                    for batch in self._chunked(rows, self.BATCH_SIZE):
                        cur.executemany(sql, batch)
            cur.close()
        if progress_callback:
            progress_callback(
                f"  Paczka: {len(visits)} wizyt, {len(diagnoses)} diagnoz, "
                f"{len(services)} uslug, {len(prescriptions)} recept, "
                f"{len(test_results)} wynikow badan"
            )

    def seed_mongo_from_postgres(self, progress_callback=None):
        """Buduje kolekcję MongoDB pacjentów czytając dane bezpośrednio z PostgreSQL.
        Nie wymaga trzymania całości danych w pamięci Python.
        """
        def _report(msg):
            if progress_callback:
                progress_callback(f"[MongoDB] {msg}")

        pg = self.cm.get_connector(DatabaseType.POSTGRES).get_connection()
        db_mongo = self.cm.get_connector(DatabaseType.MONGODB).get_db()
        db_mongo.drop_collection("patients")

        count_cur = pg.cursor()
        count_cur.execute("SELECT COUNT(*) FROM patients")
        total_patients = count_cur.fetchone()[0]
        count_cur.close()
        _report(f"Budowanie dokumentow dla {total_patients:,} pacjentow z PostgreSQL...")

        BATCH_P = 1000
        offset = 0
        inserted = 0

        while offset < total_patients:
            pat_cur = pg.cursor()
            pat_cur.execute(
                "SELECT id, national_id, first_name, last_name, birth_date, gender "
                "FROM patients ORDER BY id LIMIT %s OFFSET %s",
                (BATCH_P, offset),
            )
            patients = pat_cur.fetchall()
            pat_cur.close()
            if not patients:
                break

            patient_ids = [p[0] for p in patients]

            vis_cur = pg.cursor()
            vis_cur.execute(
                "SELECT id, patient_id, doctor_id, visit_date, status "
                "FROM visits WHERE patient_id = ANY(%s) ORDER BY patient_id",
                (patient_ids,),
            )
            visits = vis_cur.fetchall()
            vis_cur.close()

            visit_ids = [v[0] for v in visits] if visits else []
            visits_by_patient: dict = {}
            for v in visits:
                visits_by_patient.setdefault(v[1], []).append(v)

            diags_by_visit: dict = {}
            svc_by_visit: dict = {}
            rx_by_visit: dict = {}
            rxi_by_rx: dict = {}
            tr_by_visit: dict = {}

            if visit_ids:
                d_cur = pg.cursor()
                d_cur.execute(
                    "SELECT visit_id, disease_id, diagnosis_type, notes "
                    "FROM diagnoses WHERE visit_id = ANY(%s)",
                    (visit_ids,),
                )
                for row in d_cur.fetchall():
                    diags_by_visit.setdefault(row[0], []).append(row)
                d_cur.close()

                s_cur = pg.cursor()
                s_cur.execute(
                    "SELECT visit_id, service_id, quantity, final_price "
                    "FROM performed_services WHERE visit_id = ANY(%s)",
                    (visit_ids,),
                )
                for row in s_cur.fetchall():
                    svc_by_visit.setdefault(row[0], []).append(row)
                s_cur.close()

                rx_cur = pg.cursor()
                rx_cur.execute(
                    "SELECT id, visit_id, prescription_code, issue_date "
                    "FROM prescriptions WHERE visit_id = ANY(%s)",
                    (visit_ids,),
                )
                rx_rows = rx_cur.fetchall()
                rx_cur.close()
                for row in rx_rows:
                    rx_by_visit.setdefault(row[1], []).append(row)

                if rx_rows:
                    rx_ids = [r[0] for r in rx_rows]
                    rxi_cur = pg.cursor()
                    rxi_cur.execute(
                        "SELECT prescription_id, medication_id, dosage "
                        "FROM prescription_items WHERE prescription_id = ANY(%s)",
                        (rx_ids,),
                    )
                    for row in rxi_cur.fetchall():
                        rxi_by_rx.setdefault(row[0], []).append(row)
                    rxi_cur.close()

                t_cur = pg.cursor()
                t_cur.execute(
                    "SELECT visit_id, parameter_name, result_value, unit, min_norm, max_norm "
                    "FROM test_results WHERE visit_id = ANY(%s)",
                    (visit_ids,),
                )
                for row in t_cur.fetchall():
                    tr_by_visit.setdefault(row[0], []).append(row)
                t_cur.close()

            docs = []
            for p in patients:
                doc = {
                    "_id": p[0],
                    "national_id": p[1],
                    "first_name": p[2],
                    "last_name": p[3],
                    "birth_date": str(p[4]) if p[4] else None,
                    "gender": p[5],
                    "visits": [],
                }
                for v in visits_by_patient.get(p[0], []):
                    visit_doc = {
                        "visit_id": v[0],
                        "doctor_id": v[2],
                        "visit_date": str(v[3]) if v[3] else None,
                        "status": v[4],
                        "diagnoses": [
                            {
                                "disease_id": d[1],
                                "diagnosis_type": d[2],
                                "notes": d[3],
                            }
                            for d in diags_by_visit.get(v[0], [])
                        ],
                        "performed_services": [
                            {
                                "service_id": s[1],
                                "quantity": s[2],
                                "final_price": float(s[3]),
                            }
                            for s in svc_by_visit.get(v[0], [])
                        ],
                        "test_results": [
                            {
                                "parameter_name": t[1],
                                "result_value": float(t[2]),
                                "unit": t[3],
                                "min_norm": float(t[4]),
                                "max_norm": float(t[5]),
                            }
                            for t in tr_by_visit.get(v[0], [])
                        ],
                        "prescriptions": [],
                    }
                    for rx in rx_by_visit.get(v[0], []):
                        visit_doc["prescriptions"].append({
                            "prescription_code": rx[2],
                            "issue_date": str(rx[3]) if rx[3] else None,
                            "items": [
                                {"medication_id": it[1], "dosage": it[2]}
                                for it in rxi_by_rx.get(rx[0], [])
                            ],
                        })
                    doc["visits"].append(visit_doc)
                docs.append(doc)

            if docs:
                db_mongo.patients.insert_many(docs)
            inserted += len(docs)
            offset += BATCH_P
            if inserted % 50000 == 0 or inserted == total_patients:
                _report(f"  Postep: {inserted:,}/{total_patients:,} pacjentow")

        _report(f"Gotowe. Wstawiono {inserted:,} dokumentow.")

    def seed_redis_streaming(self, data_base: GeneratedData, progress_callback=None):
        """Seeduje Redis używając base_data dla danych statycznych
        i PostgreSQL dla wizyt + ich powiązań (streaming).
        """
        def _report(msg):
            if progress_callback:
                progress_callback(f"[Redis] {msg}")

        r = self.cm.get_connector(DatabaseType.REDIS).get_connection()
        r.flushdb()
        _report("Czyszczenie zakonczone.")

        # ── departments ─────────────────────────────────────────────
        _report(f"Seedowanie oddzialow ({len(data_base.departments)})...")
        pipe = r.pipeline()
        for i, dep in enumerate(data_base.departments):
            pipe.hset(f"department:{dep[0]}", mapping={"name": dep[1], "phone": dep[2]})
            if (i + 1) % self.BATCH_SIZE == 0:
                pipe.execute()
                pipe = r.pipeline()
        pipe.execute()

        # ── session:doctor:{id} ──────────────────────────────────────
        _report(f"Seedowanie sesji lekarzy ({len(data_base.doctors)})...")
        pipe = r.pipeline()
        for i, d in enumerate(data_base.doctors):
            pipe.hset(f"session:doctor:{d[0]}", mapping={
                "first_name": d[3], "last_name": d[4],
                "license_number": d[5], "department_id": str(d[1]),
                "specialization_id": str(d[2]),
            })
            if (i + 1) % self.BATCH_SIZE == 0:
                pipe.execute()
                pipe = r.pipeline()
        pipe.execute()

        # ── patient:{id} HASH ────────────────────────────────────────
        _report(f"Seedowanie pacjentow ({len(data_base.patients):,})...")
        pipe = r.pipeline()
        for i, p in enumerate(data_base.patients):
            pipe.hset(f"patient:{p[0]}", mapping={
                "first_name": p[2], "last_name": p[3],
                "national_id": p[1], "gender": p[5],
            })
            if (i + 1) % self.BATCH_SIZE == 0:
                pipe.execute()
                pipe = r.pipeline()
        pipe.execute()

        # ── Wizyty i powiązania z PostgreSQL ────────────────────────
        pg = self.cm.get_connector(DatabaseType.POSTGRES).get_connection()
        # Named (server-side) cursors require an open transaction in psycopg2
        pg.autocommit = False

        # visit:status, visit:doctor, patient:visits
        _report("Streaming: visit:status, visit:doctor, patient:visits...")
        vis_cur = pg.cursor("redis_visits_cur")
        vis_cur.execute(
            "SELECT id, patient_id, doctor_id, status FROM visits ORDER BY id"
        )
        pipe = r.pipeline()
        batch_count = 0
        total_vis = 0
        for row in vis_cur:
            pipe.set(f"visit:status:{row[0]}", row[3])
            pipe.set(f"visit:doctor:{row[0]}", str(row[2]))
            pipe.sadd(f"patient:visits:{row[1]}", str(row[0]))
            batch_count += 1
            total_vis += 1
            if batch_count >= self.BATCH_SIZE:
                pipe.execute()
                pipe = r.pipeline()
                batch_count = 0
        pipe.execute()
        vis_cur.close()
        _report(f"  Wstawiono {total_vis:,} wpisow wizyt.")

        # visit:diag:{visit_id} LIST
        _report("Streaming: visit:diag (diagnozy)...")
        diag_cur = pg.cursor("redis_diags_cur")
        diag_cur.execute(
            "SELECT visit_id, disease_id, diagnosis_type FROM diagnoses ORDER BY visit_id"
        )
        pipe = r.pipeline()
        batch_count = 0
        total_diag = 0
        for row in diag_cur:
            pipe.rpush(f"visit:diag:{row[0]}", f"{row[1]}:{row[2]}")
            batch_count += 1
            total_diag += 1
            if batch_count >= self.BATCH_SIZE:
                pipe.execute()
                pipe = r.pipeline()
                batch_count = 0
        pipe.execute()
        diag_cur.close()
        _report(f"  Wstawiono {total_diag:,} wpisow diagnoz.")

        # prescription:{id} HASH + visit:prescriptions SET
        _report("Streaming: prescription HASH + visit:prescriptions SET...")
        rx_cur = pg.cursor("redis_rx_cur")
        rx_cur.execute(
            "SELECT id, visit_id, prescription_code, issue_date FROM prescriptions ORDER BY id"
        )
        pipe = r.pipeline()
        batch_count = 0
        total_rx = 0
        for row in rx_cur:
            pipe.hset(f"prescription:{row[0]}", mapping={
                "visit_id": str(row[1]),
                "code": row[2],
                "issue_date": str(row[3]) if row[3] else "",
            })
            pipe.sadd(f"visit:prescriptions:{row[1]}", str(row[0]))
            batch_count += 1
            total_rx += 1
            if batch_count >= self.BATCH_SIZE:
                pipe.execute()
                pipe = r.pipeline()
                batch_count = 0
        pipe.execute()
        rx_cur.close()
        _report(f"  Wstawiono {total_rx:,} recept.")

        # service:total:{vid} HASH – agregacja w partiach
        _report("Streaming: service:total (sumy uslug)...")
        svc_cur = pg.cursor("redis_svc_cur")
        svc_cur.execute(
            "SELECT visit_id, final_price FROM performed_services ORDER BY visit_id"
        )
        service_totals: dict = {}
        total_svc = 0
        for row in svc_cur:
            vid = row[0]
            service_totals[vid] = service_totals.get(vid, 0.0) + float(row[1])
            total_svc += 1
            if len(service_totals) >= 50_000:
                # Flush in small batches to avoid Redis socket timeout
                items = list(service_totals.items())
                for i in range(0, len(items), self.BATCH_SIZE):
                    pipe = r.pipeline()
                    for vid2, tot in items[i:i + self.BATCH_SIZE]:
                        pipe.hset(f"service:total:{vid2}", mapping={
                            "total_price": str(round(tot, 2)), "visit_id": str(vid2),
                        })
                    pipe.execute()
                service_totals.clear()
        # Flush remainder
        items = list(service_totals.items())
        for i in range(0, len(items), self.BATCH_SIZE):
            pipe = r.pipeline()
            for vid2, tot in items[i:i + self.BATCH_SIZE]:
                pipe.hset(f"service:total:{vid2}", mapping={
                    "total_price": str(round(tot, 2)), "visit_id": str(vid2),
                })
            pipe.execute()
        service_totals.clear()
        svc_cur.close()
        _report(f"  Wstawiono {total_svc:,} wpisow uslug.")

        # test:{vid} HASH – jeden wynik per wizyta
        _report("Streaming: test HASH (wyniki badan, 1 per wizyta)...")
        tr_cur = pg.cursor("redis_tr_cur")
        tr_cur.execute(
            "SELECT DISTINCT ON (visit_id) visit_id, parameter_name, result_value, unit "
            "FROM test_results ORDER BY visit_id, id"
        )
        pipe = r.pipeline()
        batch_count = 0
        total_tr = 0
        for row in tr_cur:
            pipe.hset(f"test:{row[0]}", mapping={
                "parameter": row[1], "value": str(row[2]), "unit": row[3],
            })
            batch_count += 1
            total_tr += 1
            if batch_count >= self.BATCH_SIZE:
                pipe.execute()
                pipe = r.pipeline()
                batch_count = 0
        pipe.execute()
        tr_cur.close()
        _report(f"  Wstawiono {total_tr:,} wynikow badan. Gotowe.")
        pg.commit()
        pg.autocommit = True

    def create_indexes(self, progress_callback=None):
        def _report(msg):
            if progress_callback:
                progress_callback(msg)

        _report("[PostgreSQL] Tworzenie indeksow...")
        pg = self.cm.get_connector(DatabaseType.POSTGRES).get_connection()
        pg_cur = pg.cursor()
        for idx_sql in SQL_INDEXES:
            try:
                pg_cur.execute(idx_sql)
            except Exception as e:
                _report(f"[PostgreSQL] Indeks pominiety: {e}")
        pg_cur.close()

        _report("[MySQL] Tworzenie indeksow...")
        my = self.cm.get_connector(DatabaseType.MYSQL).get_connection()
        my_cur = my.cursor()
        for table, idx_name, column in MYSQL_INDEXES:
            try:
                my_cur.execute(f"CREATE INDEX {idx_name} ON {table}({column})")
            except Exception as e:
                _report(f"[MySQL] Indeks pominiety: {idx_name}: {e}")
        my_cur.close()

        _report("[MongoDB] Tworzenie indeksow na kolekcji patients...")
        db = self.cm.get_connector(DatabaseType.MONGODB).get_db()
        db.patients.create_index("last_name")
        db.patients.create_index("national_id")
        db.patients.create_index("gender")
        db.patients.create_index("visits.status")
        db.patients.create_index("visits.doctor_id")
        db.patients.create_index("visits.visit_date")
        db.patients.create_index("visits.diagnoses.disease_id")

        _report("Zakonczone tworzenie indeksow.")

    def drop_indexes(self, progress_callback=None):
        def _report(msg):
            if progress_callback:
                progress_callback(msg)

        _report("[PostgreSQL] Usuwanie indeksow...")
        pg = self.cm.get_connector(DatabaseType.POSTGRES).get_connection()
        pg_cur = pg.cursor()
        for idx_sql in SQL_INDEXES:
            idx_name = idx_sql.split("IF NOT EXISTS ")[-1].split(" ON")[0].strip()
            try:
                pg_cur.execute(f"DROP INDEX IF EXISTS {idx_name}")
            except Exception:
                pass
        pg_cur.close()

        _report("[MySQL] Usuwanie indeksow...")
        my = self.cm.get_connector(DatabaseType.MYSQL).get_connection()
        my_cur = my.cursor()
        for table, idx_name, _ in MYSQL_INDEXES:
            try:
                my_cur.execute(f"DROP INDEX {idx_name} ON {table}")
            except Exception:
                pass
        my_cur.close()

        _report("[MongoDB] Usuwanie indeksow...")
        db = self.cm.get_connector(DatabaseType.MONGODB).get_db()
        try:
            db.patients.drop_indexes()
        except Exception:
            pass

        _report("Zakonczone usuwanie indeksow.")

    def clear_all(self):
        try:
            pg = self.cm.get_connector(DatabaseType.POSTGRES).get_connection()
            cur = pg.cursor()
            for tbl in DROP_ORDER:
                cur.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")
            cur.close()
        except Exception:
            pass

        try:
            my = self.cm.get_connector(DatabaseType.MYSQL).get_connection()
            cur = my.cursor()
            cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            for tbl in DROP_ORDER:
                cur.execute(f"DROP TABLE IF EXISTS {tbl}")
            cur.execute("SET FOREIGN_KEY_CHECKS = 1")
            cur.close()
        except Exception:
            pass

        try:
            db = self.cm.get_connector(DatabaseType.MONGODB).get_db()
            db.drop_collection("patients")
        except Exception:
            pass

        try:
            r = self.cm.get_connector(DatabaseType.REDIS).get_connection()
            r.flushdb()
        except Exception:
            pass

    # ── SQL ─────────────────────────────────────────────────────────

    def _seed_sql(self, db_type: DatabaseType, data: GeneratedData, progress_callback=None):
        db_name = db_type.value

        def _report(msg):
            if progress_callback:
                progress_callback(f"[{db_name}] {msg}")

        connector = self.cm.get_connector(db_type)
        conn = connector.get_connection()
        cur = conn.cursor()

        _report("DROP tabel...")
        if db_type == DatabaseType.MYSQL:
            cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            for tbl in DROP_ORDER:
                cur.execute(f"DROP TABLE IF EXISTS {tbl}")
            cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        else:
            for tbl in DROP_ORDER:
                cur.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")

        _report("CREATE tabel...")
        for stmt in SQL_CREATE_TABLES.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)

        _report("Dodawanie kluczy obcych...")
        for fk_sql in SQL_ALTER_FK:
            try:
                cur.execute(fk_sql)
            except Exception:
                pass

        data_map = {
            "departments": data.departments,
            "specializations": data.specializations,
            "diseases": data.diseases,
            "medical_services": data.medical_services,
            "medications": data.medications,
            "patients": data.patients,
            "doctors": data.doctors,
            "visits": data.visits,
            "performed_services": data.performed_services,
            "diagnoses": data.diagnoses,
            "prescriptions": data.prescriptions,
            "prescription_items": data.prescription_items,
            "test_results": data.test_results,
        }

        for table_name, n_cols in TABLE_INSERTS:
            rows = data_map[table_name]
            if not rows:
                continue
            _report(f"Wstawianie: {table_name} ({len(rows)} wierszy)")
            if db_type == DatabaseType.POSTGRES:
                for batch in self._chunked(rows, self.BATCH_SIZE * 4):
                    execute_values(cur, f"INSERT INTO {table_name} VALUES %s", batch)
            else:
                placeholders = ",".join(["%s"] * n_cols)
                sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                for batch in self._chunked(rows, self.BATCH_SIZE):
                    cur.executemany(sql, batch)

        # MySQL: aktualizacja statystyk planera dla porównywalności z PG
        # (PG ma autovacuum/auto-analyze, MySQL/InnoDB nie zawsze).
        if db_type == DatabaseType.MYSQL:
            _report("ANALYZE TABLE (aktualizacja statystyk planera)...")
            for tbl, _ in TABLE_INSERTS:
                try:
                    cur.execute(f"ANALYZE TABLE {tbl}")
                    cur.fetchall()
                except Exception as e:
                    _report(f"ANALYZE {tbl} pominiete: {e}")

        cur.close()
        _report("Gotowe.")

    # ── MongoDB ──────────────────────────────────────────────────────

    def _seed_mongodb(self, data: GeneratedData, progress_callback=None):
        def _report(msg):
            if progress_callback:
                progress_callback(f"[MongoDB] {msg}")

        db = self.cm.get_connector(DatabaseType.MONGODB).get_db()
        _report("Czyszczenie kolekcji patients...")
        db.drop_collection("patients")

        if data.mongo_patients:
            _report(f"Wstawianie {len(data.mongo_patients)} dokumentow...")
            for batch in self._chunked(data.mongo_patients, self.BATCH_SIZE):
                db.patients.insert_many(batch)
        _report("Gotowe.")

    def _seed_mongodb_streaming(self, doc_gen, progress_callback=None):
        """Wstawia dokumenty MongoDB z generatora w partiach (streaming).
        Nie materializuje pelnej listy – redukuje zuzycie RAM o ~60-70% vs _seed_mongodb.
        """
        def _report(msg):
            if progress_callback:
                progress_callback(f"[MongoDB] {msg}")

        db = self.cm.get_connector(DatabaseType.MONGODB).get_db()
        _report("Czyszczenie kolekcji patients...")
        db.drop_collection("patients")

        _report("Wstawianie dokumentow (streaming)...")
        batch = []
        total = 0
        for doc in doc_gen:
            batch.append(doc)
            if len(batch) >= self.BATCH_SIZE:
                db.patients.insert_many(batch)
                total += len(batch)
                batch.clear()
        if batch:
            db.patients.insert_many(batch)
            total += len(batch)
        _report(f"Wstawiono {total} dokumentow. Gotowe.")

    # ── Redis ────────────────────────────────────────────────────────

    def _seed_redis(self, data: GeneratedData, progress_callback=None):
        """
        Seeduje wszystkie struktury Redis potrzebne do benchmarku:
          - visit:status:{id}     STRING  (oryginalnie)
          - session:doctor:{id}   HASH    (oryginalnie)
          - patient:{id}          HASH    (nowe - do benchmarku READ)
          - visit:diag:{vid}      LIST    (nowe - symulacja diagnoz)
          - prescription:{id}     HASH    (nowe - symulacja recept)
          - service:total:{vid}   HASH    (nowe - sumaryczna cena uslug)
          - test:{vid}            HASH    (nowe - wynik badania)
        """
        def _report(msg):
            if progress_callback:
                progress_callback(f"[Redis] {msg}")

        r = self.cm.get_connector(DatabaseType.REDIS).get_connection()
        _report("Czyszczenie bazy (FLUSHDB)...")
        r.flushdb()

        # ── department:{id} HASH ──────────────────────────────────
        if data.departments:
            _report(f"Wstawianie hashy oddzialow ({len(data.departments)})...")
            pipe = r.pipeline()
            for i, dep in enumerate(data.departments):
                pipe.hset(f"department:{dep[0]}", mapping={
                    "name": dep[1],
                    "phone": dep[2],
                })
                if (i + 1) % self.BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        # ── visit:status:{id} STRING + visit:doctor:{id} STRING ──
        if data.redis_visit_statuses:
            _report(f"Wstawianie statusow wizyt ({len(data.redis_visit_statuses)})...")
            pipe = r.pipeline()
            for i, (k, v) in enumerate(data.redis_visit_statuses):
                pipe.set(k, v)
                if (i + 1) % self.BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        # ── visit:doctor:{vid} STRING (mapowanie wizyta→lekarz) ──
        if data.visits:
            _report(f"Wstawianie mapowan visit:doctor ({len(data.visits)})...")
            pipe = r.pipeline()
            for i, v in enumerate(data.visits):
                pipe.set(f"visit:doctor:{v[0]}", str(v[2]))
                if (i + 1) % self.BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        # ── session:doctor:{id} HASH ─────────────────────────────
        if data.redis_doctor_sessions:
            _report(f"Wstawianie sesji lekarzy ({len(data.redis_doctor_sessions)})...")
            pipe = r.pipeline()
            for i, (k, v) in enumerate(data.redis_doctor_sessions):
                pipe.hset(k, mapping=v)
                if (i + 1) % self.BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        # ── patient:{id} HASH ────────────────────────────────────
        if data.patients:
            _report(f"Wstawianie hashy pacjentow ({len(data.patients)})...")
            pipe = r.pipeline()
            for i, p in enumerate(data.patients):
                pipe.hset(f"patient:{p[0]}", mapping={
                    "first_name": p[2],
                    "last_name": p[3],
                    "national_id": p[1],
                    "gender": p[5],
                })
                if (i + 1) % self.BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        # ── patient:visits:{patient_id} SET ──────────────────────
        if data.visits:
            _report(f"Wstawianie mapowan patient:visits ({len(data.visits)})...")
            pipe = r.pipeline()
            batch_count = 0
            for v in data.visits:
                pipe.sadd(f"patient:visits:{v[1]}", str(v[0]))
                batch_count += 1
                if batch_count % self.BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        # ── visit:diag:{visit_id} LIST ───────────────────────────
        if data.diagnoses:
            _report(f"Wstawianie list diagnoz ({len(data.diagnoses)})...")
            pipe = r.pipeline()
            for i, d in enumerate(data.diagnoses):
                # format: "disease_id:diagnosis_type"
                pipe.rpush(f"visit:diag:{d[1]}", f"{d[2]}:{d[3]}")
                if (i + 1) % self.BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        # ── prescription:{id} HASH ───────────────────────────────
        if data.prescriptions:
            _report(f"Wstawianie hashy recept ({len(data.prescriptions)})...")
            pipe = r.pipeline()
            for i, rx in enumerate(data.prescriptions):
                pipe.hset(f"prescription:{rx[0]}", mapping={
                    "visit_id": str(rx[1]),
                    "code": rx[2],
                    "issue_date": str(rx[3]),
                })
                if (i + 1) % self.BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        # ── visit:prescriptions:{visit_id} SET ────────────────────
        if data.prescriptions:
            _report(f"Wstawianie mapowan visit:prescriptions ({len(data.prescriptions)})...")
            pipe = r.pipeline()
            batch_count = 0
            for rx in data.prescriptions:
                pipe.sadd(f"visit:prescriptions:{rx[1]}", str(rx[0]))
                batch_count += 1
                if batch_count % self.BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        # ── service:total:{visit_id} HASH ────────────────────────
        # Agregujemy sumę cen uslug per wizyta w Pythonie przed wstawieniem
        if data.performed_services:
            _report(f"Agregacja i wstawianie sum uslug ({len(data.performed_services)})...")
            service_totals: dict[int, float] = {}
            for ps in data.performed_services:
                service_totals[ps[1]] = service_totals.get(ps[1], 0.0) + float(ps[4])

            pipe = r.pipeline()
            for i, (vid, total) in enumerate(service_totals.items()):
                pipe.hset(f"service:total:{vid}", mapping={
                    "total_price": str(round(total, 2)),
                    "visit_id": str(vid),
                })
                if (i + 1) % self.BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        # ── test:{visit_id} HASH ─────────────────────────────────
        # Jeden wynik badania (pierwszy) per wizyta
        if data.test_results:
            _report(f"Wstawianie wynikow badan do Redis ({len(data.test_results)} rekordow)...")
            seen_visits: set[int] = set()
            pipe = r.pipeline()
            batch_count = 0
            for tr in data.test_results:
                vid = tr[1]
                if vid not in seen_visits:
                    seen_visits.add(vid)
                    pipe.hset(f"test:{vid}", mapping={
                        "parameter": tr[2],
                        "value": str(tr[3]),
                        "unit": tr[4],
                    })
                    batch_count += 1
                    if batch_count % self.BATCH_SIZE == 0:
                        pipe.execute()
                        pipe = r.pipeline()
            pipe.execute()
            _report(f"Zapisano {len(seen_visits)} wynikow badan (1 per wizyta).")

        _report("Gotowe.")
