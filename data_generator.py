"""
data_generator.py – Generowanie syntetycznych danych medycznych
za pomocą biblioteki Faker.
"""

import random
from dataclasses import dataclass, field
from datetime import date, timedelta
from faker import Faker

fake = Faker("pl_PL")

# ─── Zbiorcza struktura danych ─────────────────────────────────────────────────

@dataclass
class GeneratedData:
    """Kontener na wszystkie wygenerowane dane."""
    # SQL – listy krotek (do executemany)
    departments: list = field(default_factory=list)
    specializations: list = field(default_factory=list)
    diseases: list = field(default_factory=list)
    medical_services: list = field(default_factory=list)
    medications: list = field(default_factory=list)
    patients: list = field(default_factory=list)
    doctors: list = field(default_factory=list)
    visits: list = field(default_factory=list)
    performed_services: list = field(default_factory=list)
    diagnoses: list = field(default_factory=list)
    prescriptions: list = field(default_factory=list)
    prescription_items: list = field(default_factory=list)
    test_results: list = field(default_factory=list)
    # MongoDB
    mongo_patients: list = field(default_factory=list)
    # Redis
    redis_visit_statuses: list = field(default_factory=list)
    redis_doctor_sessions: list = field(default_factory=list)


# ─── Skalowanie ───────────────────────────────────────────────────────────────

SCALE_MAP = {
    10_000: {
        "departments": 10, "specializations": 15, "diseases": 100,
        "medical_services": 50, "medications": 80, "patients": 2_000,
        "doctors": 50, "visits": 10_000,
    },
    100_000: {
        "departments": 20, "specializations": 30, "diseases": 500,
        "medical_services": 200, "medications": 400, "patients": 20_000,
        "doctors": 200, "visits": 100_000,
    },
    1_000_000: {
        "departments": 50, "specializations": 60, "diseases": 2_000,
        "medical_services": 500, "medications": 1_500, "patients": 200_000,
        "doctors": 500, "visits": 1_000_000,
    },
}

VISIT_STATUSES = ["scheduled", "completed", "cancelled", "in_progress"]
DIAGNOSIS_TYPES = ["primary", "secondary", "additional"]
GENDERS = ["M", "F"]
UNITS = ["mg/dL", "mmol/L", "U/L", "g/dL", "%", "mm/h", "10^3/uL"]
PARAM_NAMES = [
    "Hemoglobina", "Glukoza", "Cholesterol", "Kreatynina",
    "Białko CRP", "Leukocyty", "Erytrocyty", "Trombocyty",
    "ALT", "AST", "Bilirubina", "Żelazo", "TSH", "Sód", "Potas",
]


# ─── Generatory poszczególnych tabel ──────────────────────────────────────────

def _gen_departments(n: int) -> list[tuple]:
    return [(i + 1, fake.unique.company()[:60], fake.phone_number()[:20]) for i in range(n)]


def _gen_specializations(n: int) -> list[tuple]:
    specs = [
        "Kardiologia", "Neurologia", "Ortopedia", "Dermatologia",
        "Pediatria", "Onkologia", "Okulistyka", "Chirurgia",
        "Psychiatria", "Urologia", "Ginekologia", "Endokrynologia",
        "Gastroenterologia", "Pulmonologia", "Reumatologia",
        "Nefrologia", "Hematologia", "Radiologia", "Anestezjologia",
        "Laryngologia", "Kardiochirurgia", "Geriatria", "Alergologia",
        "Diabetologia", "Immunologia", "Rehabilitacja", "Neonatologia",
        "Toksykologia", "Medycyna ratunkowa", "Medycyna pracy",
    ]
    while len(specs) < n:
        specs.append(f"Specjalizacja_{len(specs) + 1}")
    return [(i + 1, specs[i]) for i in range(n)]


def _gen_diseases(n: int) -> list[tuple]:
    return [
        (i + 1, f"{chr(65 + i % 26)}{(i // 26):02d}.{i % 10}", fake.sentence(nb_words=3)[:80])
        for i in range(n)
    ]


def _gen_medical_services(n: int) -> list[tuple]:
    return [
        (i + 1, f"Usługa medyczna {i + 1}", round(random.uniform(50, 2000), 2))
        for i in range(n)
    ]


def _gen_medications(n: int) -> list[tuple]:
    return [
        (i + 1, f"Lek_{i + 1}", fake.word().capitalize())
        for i in range(n)
    ]


def _gen_patients(n: int) -> list[tuple]:
    rows = []
    for i in range(n):
        gender = random.choice(GENDERS)
        first_name = fake.first_name_male() if gender == "M" else fake.first_name_female()
        last_name = fake.last_name()
        birth = fake.date_of_birth(minimum_age=1, maximum_age=95)
        national_id = fake.numerify("###########")
        rows.append((i + 1, national_id, first_name, last_name, birth, gender))
    return rows


def _gen_doctors(n: int, n_deps: int, n_specs: int) -> list[tuple]:
    rows = []
    for i in range(n):
        dep_id = random.randint(1, n_deps)
        spec_id = random.randint(1, n_specs)
        first_name = fake.first_name()
        last_name = fake.last_name()
        license_no = fake.numerify("#######")
        rows.append((i + 1, dep_id, spec_id, first_name, last_name, license_no))
    return rows


def _gen_visits(n: int, n_patients: int, n_doctors: int) -> list[tuple]:
    rows = []
    base_date = date.today() - timedelta(days=365 * 2)
    for i in range(n):
        patient_id = random.randint(1, n_patients)
        doctor_id = random.randint(1, n_doctors)
        visit_date = base_date + timedelta(days=random.randint(0, 730))
        status = random.choice(VISIT_STATUSES)
        rows.append((i + 1, patient_id, doctor_id, visit_date, status))
    return rows


def _gen_performed_services(visits: list[tuple], n_services: int) -> list[tuple]:
    rows = []
    sid = 1
    for visit in visits:
        visit_id = visit[0]
        count = random.randint(0, 2)
        for _ in range(count):
            service_id = random.randint(1, n_services)
            quantity = random.randint(1, 3)
            final_price = round(random.uniform(50, 3000), 2)
            rows.append((sid, visit_id, service_id, quantity, final_price))
            sid += 1
    return rows


def _gen_diagnoses(visits: list[tuple], n_diseases: int) -> list[tuple]:
    rows = []
    did = 1
    for visit in visits:
        visit_id = visit[0]
        count = random.randint(0, 2)
        for _ in range(count):
            disease_id = random.randint(1, n_diseases)
            dtype = random.choice(DIAGNOSIS_TYPES)
            notes = fake.sentence(nb_words=6) if random.random() > 0.3 else ""
            rows.append((did, visit_id, disease_id, dtype, notes))
            did += 1
    return rows


def _gen_prescriptions_and_items(
    visits: list[tuple], n_medications: int
) -> tuple[list[tuple], list[tuple]]:
    prescriptions = []
    items = []
    pid = 1
    iid = 1
    for visit in visits:
        visit_id = visit[0]
        if random.random() < 0.5:
            code = fake.bothify("RX-####-????").upper()
            issue_date = visit[3]  # visit_date
            prescriptions.append((pid, visit_id, code, issue_date))
            n_items = random.randint(1, 3)
            for _ in range(n_items):
                med_id = random.randint(1, n_medications)
                dosage = f"{random.choice([1,2,3])}x{random.choice([100,200,500])}mg"
                items.append((iid, pid, med_id, dosage))
                iid += 1
            pid += 1
    return prescriptions, items


def _gen_test_results(visits: list[tuple]) -> list[tuple]:
    rows = []
    tid = 1
    for visit in visits:
        visit_id = visit[0]
        if random.random() < 0.4:
            count = random.randint(1, 3)
            for _ in range(count):
                param = random.choice(PARAM_NAMES)
                value = round(random.uniform(0.1, 500), 2)
                unit = random.choice(UNITS)
                mn = round(random.uniform(0, value * 0.5), 2)
                mx = round(value * 1.5 + random.uniform(0, 50), 2)
                rows.append((tid, visit_id, param, value, unit, mn, mx))
                tid += 1
    return rows


# ─── Budowa dokumentów MongoDB ────────────────────────────────────────────────

def _build_mongo_documents(data: GeneratedData) -> list[dict]:
    """Buduje zagnieżdżone dokumenty pacjentów dla MongoDB."""

    # Indeksowanie po kluczu obcym
    visits_by_patient: dict[int, list] = {}
    for v in data.visits:
        visits_by_patient.setdefault(v[1], []).append(v)

    services_by_visit: dict[int, list] = {}
    for s in data.performed_services:
        services_by_visit.setdefault(s[1], []).append(s)

    diags_by_visit: dict[int, list] = {}
    for d in data.diagnoses:
        diags_by_visit.setdefault(d[1], []).append(d)

    rx_by_visit: dict[int, list] = {}
    for r in data.prescriptions:
        rx_by_visit.setdefault(r[1], []).append(r)

    items_by_rx: dict[int, list] = {}
    for it in data.prescription_items:
        items_by_rx.setdefault(it[1], []).append(it)

    tests_by_visit: dict[int, list] = {}
    for t in data.test_results:
        tests_by_visit.setdefault(t[1], []).append(t)

    documents = []
    for p in data.patients:
        patient_doc = {
            "_id": p[0],
            "national_id": p[1],
            "first_name": p[2],
            "last_name": p[3],
            "birth_date": p[4].isoformat() if isinstance(p[4], date) else str(p[4]),
            "gender": p[5],
            "visits": [],
        }

        for v in visits_by_patient.get(p[0], []):
            visit_doc = {
                "visit_id": v[0],
                "doctor_id": v[2],
                "visit_date": v[3].isoformat() if isinstance(v[3], date) else str(v[3]),
                "status": v[4],
                "performed_services": [
                    {"service_id": s[2], "quantity": s[3], "final_price": s[4]}
                    for s in services_by_visit.get(v[0], [])
                ],
                "diagnoses": [
                    {"disease_id": d[2], "diagnosis_type": d[3], "notes": d[4]}
                    for d in diags_by_visit.get(v[0], [])
                ],
                "prescriptions": [],
                "test_results": [
                    {
                        "parameter_name": t[2], "result_value": t[3],
                        "unit": t[4], "min_norm": t[5], "max_norm": t[6],
                    }
                    for t in tests_by_visit.get(v[0], [])
                ],
            }
            for rx in rx_by_visit.get(v[0], []):
                rx_doc = {
                    "prescription_code": rx[2],
                    "issue_date": rx[3].isoformat() if isinstance(rx[3], date) else str(rx[3]),
                    "items": [
                        {"medication_id": it[2], "dosage": it[3]}
                        for it in items_by_rx.get(rx[0], [])
                    ],
                }
                visit_doc["prescriptions"].append(rx_doc)
            patient_doc["visits"].append(visit_doc)

        documents.append(patient_doc)
    return documents


# ─── Budowa danych Redis ──────────────────────────────────────────────────────

def _build_redis_data(data: GeneratedData):
    statuses = []
    for v in data.visits:
        statuses.append((f"visit:status:{v[0]}", v[4]))

    sessions = []
    for d in data.doctors:
        sessions.append((
            f"session:doctor:{d[0]}",
            {
                "first_name": d[3],
                "last_name": d[4],
                "license_number": d[5],
                "department_id": str(d[1]),
                "specialization_id": str(d[2]),
            },
        ))
    return statuses, sessions


# ─── Główna funkcja generująca ────────────────────────────────────────────────

def generate_all(scale: int, progress_callback=None) -> GeneratedData:
    """
    Generuje wszystkie dane dla podanej skali (10_000 / 100_000 / 1_000_000).
    Opcjonalny callback: progress_callback(message: str).
    """
    cfg = SCALE_MAP[scale]
    data = GeneratedData()

    def _report(msg):
        if progress_callback:
            progress_callback(msg)

    _report("Generating departments & specializations...")
    data.departments = _gen_departments(cfg["departments"])
    data.specializations = _gen_specializations(cfg["specializations"])

    _report("Generating diseases, services, medications...")
    data.diseases = _gen_diseases(cfg["diseases"])
    data.medical_services = _gen_medical_services(cfg["medical_services"])
    data.medications = _gen_medications(cfg["medications"])

    _report("Generating patients...")
    data.patients = _gen_patients(cfg["patients"])

    _report("Generating doctors...")
    data.doctors = _gen_doctors(cfg["doctors"], cfg["departments"], cfg["specializations"])

    _report("Generating visits...")
    data.visits = _gen_visits(cfg["visits"], cfg["patients"], cfg["doctors"])

    _report("Generating performed services...")
    data.performed_services = _gen_performed_services(data.visits, cfg["medical_services"])

    _report("Generating diagnoses...")
    data.diagnoses = _gen_diagnoses(data.visits, cfg["diseases"])

    _report("Generating prescriptions & items...")
    data.prescriptions, data.prescription_items = _gen_prescriptions_and_items(
        data.visits, cfg["medications"]
    )

    _report("Generating test results...")
    data.test_results = _gen_test_results(data.visits)

    _report("Building MongoDB documents...")
    data.mongo_patients = _build_mongo_documents(data)

    _report("Building Redis data...")
    data.redis_visit_statuses, data.redis_doctor_sessions = _build_redis_data(data)

    _report("Data generation complete.")
    return data
