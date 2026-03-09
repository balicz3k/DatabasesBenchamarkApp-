"""
seed_db.py – Masowe wstawianie danych (Bulk Insert) do wszystkich baz.
"""

from db_config import (
    get_pg_connection, get_mysql_connection,
    get_mongo_client, get_redis_client,
)
from data_generator import GeneratedData

BATCH_SIZE = 5000


# ─── DDL – tworzenie tabel ───────────────────────────────────────────────────

SQL_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS departments (
    id   INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS specializations (
    id   INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);
CREATE TABLE IF NOT EXISTS diseases (
    id        INT PRIMARY KEY,
    icd10_code VARCHAR(10) NOT NULL,
    name      VARCHAR(100) NOT NULL
);
CREATE TABLE IF NOT EXISTS medical_services (
    id         INT PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    base_price DECIMAL(10,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS medications (
    id               INT PRIMARY KEY,
    name             VARCHAR(100) NOT NULL,
    active_substance VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS patients (
    id          INT PRIMARY KEY,
    national_id VARCHAR(20) NOT NULL,
    first_name  VARCHAR(50) NOT NULL,
    last_name   VARCHAR(50) NOT NULL,
    birth_date  DATE,
    gender      CHAR(1)
);
CREATE TABLE IF NOT EXISTS doctors (
    id                INT PRIMARY KEY,
    department_id     INT REFERENCES departments(id),
    specialization_id INT REFERENCES specializations(id),
    first_name        VARCHAR(50) NOT NULL,
    last_name         VARCHAR(50) NOT NULL,
    license_number    VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS visits (
    id          INT PRIMARY KEY,
    patient_id  INT REFERENCES patients(id),
    doctor_id   INT REFERENCES doctors(id),
    visit_date  DATE,
    status      VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS performed_services (
    id          INT PRIMARY KEY,
    visit_id    INT REFERENCES visits(id),
    service_id  INT REFERENCES medical_services(id),
    quantity    INT,
    final_price DECIMAL(10,2)
);
CREATE TABLE IF NOT EXISTS diagnoses (
    id              INT PRIMARY KEY,
    visit_id        INT REFERENCES visits(id),
    disease_id      INT REFERENCES diseases(id),
    diagnosis_type  VARCHAR(20),
    notes           TEXT
);
CREATE TABLE IF NOT EXISTS prescriptions (
    id                INT PRIMARY KEY,
    visit_id          INT REFERENCES visits(id),
    prescription_code VARCHAR(30),
    issue_date        DATE
);
CREATE TABLE IF NOT EXISTS prescription_items (
    id              INT PRIMARY KEY,
    prescription_id INT REFERENCES prescriptions(id),
    medication_id   INT REFERENCES medications(id),
    dosage          VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS test_results (
    id              INT PRIMARY KEY,
    visit_id        INT REFERENCES visits(id),
    parameter_name  VARCHAR(50),
    result_value    DECIMAL(10,2),
    unit            VARCHAR(20),
    min_norm        DECIMAL(10,2),
    max_norm        DECIMAL(10,2)
);
"""

# MySQL wymaga nieco innej składni (brak REFERENCES inline w ten sam sposób,
# ale CREATE TABLE IF NOT EXISTS z INT PRIMARY KEY działa identycznie).
MYSQL_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS departments (
    id   INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS specializations (
    id   INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);
CREATE TABLE IF NOT EXISTS diseases (
    id        INT PRIMARY KEY,
    icd10_code VARCHAR(10) NOT NULL,
    name      VARCHAR(100) NOT NULL
);
CREATE TABLE IF NOT EXISTS medical_services (
    id         INT PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    base_price DECIMAL(10,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS medications (
    id               INT PRIMARY KEY,
    name             VARCHAR(100) NOT NULL,
    active_substance VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS patients (
    id          INT PRIMARY KEY,
    national_id VARCHAR(20) NOT NULL,
    first_name  VARCHAR(50) NOT NULL,
    last_name   VARCHAR(50) NOT NULL,
    birth_date  DATE,
    gender      CHAR(1)
);
CREATE TABLE IF NOT EXISTS doctors (
    id                INT PRIMARY KEY,
    department_id     INT,
    specialization_id INT,
    first_name        VARCHAR(50) NOT NULL,
    last_name         VARCHAR(50) NOT NULL,
    license_number    VARCHAR(20),
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (specialization_id) REFERENCES specializations(id)
);
CREATE TABLE IF NOT EXISTS visits (
    id          INT PRIMARY KEY,
    patient_id  INT,
    doctor_id   INT,
    visit_date  DATE,
    status      VARCHAR(20),
    FOREIGN KEY (patient_id) REFERENCES patients(id),
    FOREIGN KEY (doctor_id) REFERENCES doctors(id)
);
CREATE TABLE IF NOT EXISTS performed_services (
    id          INT PRIMARY KEY,
    visit_id    INT,
    service_id  INT,
    quantity    INT,
    final_price DECIMAL(10,2),
    FOREIGN KEY (visit_id) REFERENCES visits(id),
    FOREIGN KEY (service_id) REFERENCES medical_services(id)
);
CREATE TABLE IF NOT EXISTS diagnoses (
    id              INT PRIMARY KEY,
    visit_id        INT,
    disease_id      INT,
    diagnosis_type  VARCHAR(20),
    notes           TEXT,
    FOREIGN KEY (visit_id) REFERENCES visits(id),
    FOREIGN KEY (disease_id) REFERENCES diseases(id)
);
CREATE TABLE IF NOT EXISTS prescriptions (
    id                INT PRIMARY KEY,
    visit_id          INT,
    prescription_code VARCHAR(30),
    issue_date        DATE,
    FOREIGN KEY (visit_id) REFERENCES visits(id)
);
CREATE TABLE IF NOT EXISTS prescription_items (
    id              INT PRIMARY KEY,
    prescription_id INT,
    medication_id   INT,
    dosage          VARCHAR(50),
    FOREIGN KEY (prescription_id) REFERENCES prescriptions(id),
    FOREIGN KEY (medication_id) REFERENCES medications(id)
);
CREATE TABLE IF NOT EXISTS test_results (
    id              INT PRIMARY KEY,
    visit_id        INT,
    parameter_name  VARCHAR(50),
    result_value    DECIMAL(10,2),
    unit            VARCHAR(20),
    min_norm        DECIMAL(10,2),
    max_norm        DECIMAL(10,2),
    FOREIGN KEY (visit_id) REFERENCES visits(id)
);
"""

# Kolejność tabel do DROP (odwrotna do FK)
DROP_ORDER = [
    "test_results", "prescription_items", "prescriptions",
    "diagnoses", "performed_services", "visits", "doctors",
    "patients", "medications", "medical_services",
    "diseases", "specializations", "departments",
]


def _chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


# ─── PostgreSQL ───────────────────────────────────────────────────────────────

def seed_postgresql(data: GeneratedData, progress_callback=None):
    def _report(msg):
        if progress_callback:
            progress_callback("PostgreSQL", msg)

    conn = get_pg_connection()
    cur = conn.cursor()

    _report("Dropping existing tables...")
    for tbl in DROP_ORDER:
        cur.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")

    _report("Creating tables...")
    for stmt in SQL_CREATE_TABLES.split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)

    table_inserts = [
        ("departments", data.departments, "INSERT INTO departments VALUES (%s,%s,%s)"),
        ("specializations", data.specializations, "INSERT INTO specializations VALUES (%s,%s)"),
        ("diseases", data.diseases, "INSERT INTO diseases VALUES (%s,%s,%s)"),
        ("medical_services", data.medical_services, "INSERT INTO medical_services VALUES (%s,%s,%s)"),
        ("medications", data.medications, "INSERT INTO medications VALUES (%s,%s,%s)"),
        ("patients", data.patients, "INSERT INTO patients VALUES (%s,%s,%s,%s,%s,%s)"),
        ("doctors", data.doctors, "INSERT INTO doctors VALUES (%s,%s,%s,%s,%s,%s)"),
        ("visits", data.visits, "INSERT INTO visits VALUES (%s,%s,%s,%s,%s)"),
        ("performed_services", data.performed_services, "INSERT INTO performed_services VALUES (%s,%s,%s,%s,%s)"),
        ("diagnoses", data.diagnoses, "INSERT INTO diagnoses VALUES (%s,%s,%s,%s,%s)"),
        ("prescriptions", data.prescriptions, "INSERT INTO prescriptions VALUES (%s,%s,%s,%s)"),
        ("prescription_items", data.prescription_items, "INSERT INTO prescription_items VALUES (%s,%s,%s,%s)"),
        ("test_results", data.test_results, "INSERT INTO test_results VALUES (%s,%s,%s,%s,%s,%s,%s)"),
    ]

    for name, rows, sql in table_inserts:
        _report(f"Inserting {name} ({len(rows)} rows)...")
        for batch in _chunked(rows, BATCH_SIZE):
            cur.executemany(sql, batch)

    cur.close()
    conn.close()
    _report("Done.")


# ─── MySQL ────────────────────────────────────────────────────────────────────

def seed_mysql(data: GeneratedData, progress_callback=None):
    def _report(msg):
        if progress_callback:
            progress_callback("MySQL", msg)

    conn = get_mysql_connection()
    cur = conn.cursor()

    _report("Disabling FK checks & dropping tables...")
    cur.execute("SET FOREIGN_KEY_CHECKS = 0")
    for tbl in DROP_ORDER:
        cur.execute(f"DROP TABLE IF EXISTS {tbl}")
    cur.execute("SET FOREIGN_KEY_CHECKS = 1")

    _report("Creating tables...")
    for stmt in MYSQL_CREATE_TABLES.split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)

    table_inserts = [
        ("departments", data.departments, "INSERT INTO departments VALUES (%s,%s,%s)"),
        ("specializations", data.specializations, "INSERT INTO specializations VALUES (%s,%s)"),
        ("diseases", data.diseases, "INSERT INTO diseases VALUES (%s,%s,%s)"),
        ("medical_services", data.medical_services, "INSERT INTO medical_services VALUES (%s,%s,%s)"),
        ("medications", data.medications, "INSERT INTO medications VALUES (%s,%s,%s)"),
        ("patients", data.patients, "INSERT INTO patients VALUES (%s,%s,%s,%s,%s,%s)"),
        ("doctors", data.doctors, "INSERT INTO doctors VALUES (%s,%s,%s,%s,%s,%s)"),
        ("visits", data.visits, "INSERT INTO visits VALUES (%s,%s,%s,%s,%s)"),
        ("performed_services", data.performed_services, "INSERT INTO performed_services VALUES (%s,%s,%s,%s,%s)"),
        ("diagnoses", data.diagnoses, "INSERT INTO diagnoses VALUES (%s,%s,%s,%s,%s)"),
        ("prescriptions", data.prescriptions, "INSERT INTO prescriptions VALUES (%s,%s,%s,%s)"),
        ("prescription_items", data.prescription_items, "INSERT INTO prescription_items VALUES (%s,%s,%s,%s)"),
        ("test_results", data.test_results, "INSERT INTO test_results VALUES (%s,%s,%s,%s,%s,%s,%s)"),
    ]

    for name, rows, sql in table_inserts:
        _report(f"Inserting {name} ({len(rows)} rows)...")
        for batch in _chunked(rows, BATCH_SIZE):
            cur.executemany(sql, batch)
        conn.commit()

    cur.close()
    conn.close()
    _report("Done.")


# ─── MongoDB ──────────────────────────────────────────────────────────────────

def seed_mongodb(data: GeneratedData, progress_callback=None):
    def _report(msg):
        if progress_callback:
            progress_callback("MongoDB", msg)

    client, db = get_mongo_client()

    _report("Dropping collection 'patients'...")
    db.drop_collection("patients")

    _report(f"Inserting {len(data.mongo_patients)} documents...")
    if data.mongo_patients:
        for batch in _chunked(data.mongo_patients, BATCH_SIZE):
            db.patients.insert_many(batch)

    client.close()
    _report("Done.")


# ─── Redis ────────────────────────────────────────────────────────────────────

def seed_redis(data: GeneratedData, progress_callback=None):
    def _report(msg):
        if progress_callback:
            progress_callback("Redis", msg)

    r = get_redis_client()

    _report("Flushing database...")
    r.flushdb()

    _report(f"Inserting {len(data.redis_visit_statuses)} visit statuses...")
    pipe = r.pipeline()
    for i, (key, val) in enumerate(data.redis_visit_statuses):
        pipe.set(key, val)
        if (i + 1) % BATCH_SIZE == 0:
            pipe.execute()
            pipe = r.pipeline()
    pipe.execute()

    _report(f"Inserting {len(data.redis_doctor_sessions)} doctor sessions...")
    pipe = r.pipeline()
    for i, (key, mapping) in enumerate(data.redis_doctor_sessions):
        pipe.hset(key, mapping=mapping)
        if (i + 1) % BATCH_SIZE == 0:
            pipe.execute()
            pipe = r.pipeline()
    pipe.execute()

    _report("Done.")


# ─── Orkiestrator ─────────────────────────────────────────────────────────────

def seed_all(data: GeneratedData, progress_callback=None):
    """Wstawia dane do wszystkich 4 baz danych po kolei."""
    seed_postgresql(data, progress_callback)
    seed_mysql(data, progress_callback)
    seed_mongodb(data, progress_callback)
    seed_redis(data, progress_callback)
