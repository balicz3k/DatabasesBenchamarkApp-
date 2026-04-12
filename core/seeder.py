"""
core/seeder.py – Masowe wstawianie danych (Bulk Insert) do 4 systemów bazodanowych
oraz tworzenie indeksów i schematów.
"""

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

    def seed_all(self, data: GeneratedData, progress_callback=None):
        self._seed_sql(DatabaseType.POSTGRES, data, progress_callback)
        self._seed_sql(DatabaseType.MYSQL, data, progress_callback)
        self._seed_mongodb(data, progress_callback)
        self._seed_redis(data, progress_callback)

    def create_indexes(self, progress_callback=None):
        def _report(msg):
            if progress_callback:
                progress_callback(msg)

        _report("[PostgreSQL] Tworzenie indeksów...")
        pg = self.cm.get_connector(DatabaseType.POSTGRES).get_connection()
        pg_cur = pg.cursor()
        for idx_sql in SQL_INDEXES:
            try:
                pg_cur.execute(idx_sql)
            except Exception:
                pass
        pg_cur.close()

        _report("[MySQL] Tworzenie indeksów...")
        my = self.cm.get_connector(DatabaseType.MYSQL).get_connection()
        my_cur = my.cursor()
        for table, idx_name, column in MYSQL_INDEXES:
            try:
                my_cur.execute(
                    f"CREATE INDEX {idx_name} ON {table}({column})"
                )
            except Exception:
                pass
        my_cur.close()

        _report("[MongoDB] Tworzenie indeksów na kolekcji patients...")
        db = self.cm.get_connector(DatabaseType.MONGODB).get_db()
        db.patients.create_index("last_name")
        db.patients.create_index("national_id")
        db.patients.create_index("gender")
        db.patients.create_index("visits.status")
        db.patients.create_index("visits.doctor_id")
        db.patients.create_index("visits.visit_date")
        db.patients.create_index("visits.diagnoses.disease_id")

        _report("Zakończono tworzenie indeksów we wszystkich bazach.")

    def drop_indexes(self, progress_callback=None):
        def _report(msg):
            if progress_callback:
                progress_callback(msg)

        _report("[PostgreSQL] Usuwanie indeksów...")
        pg = self.cm.get_connector(DatabaseType.POSTGRES).get_connection()
        pg_cur = pg.cursor()
        for idx_sql in SQL_INDEXES:
            idx_name = idx_sql.split("IF NOT EXISTS ")[-1].split(" ON")[0].strip()
            try:
                pg_cur.execute(f"DROP INDEX IF EXISTS {idx_name}")
            except Exception:
                pass
        pg_cur.close()

        _report("[MySQL] Usuwanie indeksów...")
        my = self.cm.get_connector(DatabaseType.MYSQL).get_connection()
        my_cur = my.cursor()
        for table, idx_name, _ in MYSQL_INDEXES:
            try:
                my_cur.execute(f"DROP INDEX {idx_name} ON {table}")
            except Exception:
                pass
        my_cur.close()

        _report("[MongoDB] Usuwanie indeksów...")
        db = self.cm.get_connector(DatabaseType.MONGODB).get_db()
        try:
            db.patients.drop_indexes()
        except Exception:
            pass

        _report("Zakończono usuwanie indeksów.")

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
            placeholders = ",".join(["%s"] * n_cols)
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            _report(f"Wstawianie: {table_name} ({len(rows)} wierszy)")
            for batch in self._chunked(rows, self.BATCH_SIZE):
                cur.executemany(sql, batch)

        cur.close()
        _report("Gotowe.")

    def _seed_mongodb(self, data: GeneratedData, progress_callback=None):
        def _report(msg):
            if progress_callback:
                progress_callback(f"[MongoDB] {msg}")

        db = self.cm.get_connector(DatabaseType.MONGODB).get_db()
        _report("Czyszczenie kolekcji patients...")
        db.drop_collection("patients")

        if data.mongo_patients:
            _report(f"Wstawianie {len(data.mongo_patients)} dokumentów...")
            for batch in self._chunked(data.mongo_patients, self.BATCH_SIZE):
                db.patients.insert_many(batch)
        _report("Gotowe.")

    def _seed_redis(self, data: GeneratedData, progress_callback=None):
        def _report(msg):
            if progress_callback:
                progress_callback(f"[Redis] {msg}")

        r = self.cm.get_connector(DatabaseType.REDIS).get_connection()
        _report("Czyszczenie bazy (FLUSHDB)...")
        r.flushdb()

        if data.redis_visit_statuses:
            _report(f"Wstawianie statusów wizyt ({len(data.redis_visit_statuses)})...")
            pipe = r.pipeline()
            for i, (k, v) in enumerate(data.redis_visit_statuses):
                pipe.set(k, v)
                if (i + 1) % self.BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        if data.redis_doctor_sessions:
            _report(f"Wstawianie sesji lekarzy ({len(data.redis_doctor_sessions)})...")
            pipe = r.pipeline()
            for i, (k, v) in enumerate(data.redis_doctor_sessions):
                pipe.hset(k, mapping=v)
                if (i + 1) % self.BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline()
            pipe.execute()

        _report("Gotowe.")
