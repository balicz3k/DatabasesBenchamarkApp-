"""
core/generator.py – Generowanie syntetycznych danych medycznych (system szpitalny).
Obsługiwane skale: 10 000 / 100 000 / 500 000 / 1 000 000 wizyt.
"""

import random
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Callable, Optional

from faker import Faker

fake = Faker("pl_PL")


@dataclass
class GeneratedData:
    """Kontener na wszystkie wygenerowane dane (SQL + NoSQL)."""

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

    mongo_patients: list = field(default_factory=list)

    redis_visit_statuses: list = field(default_factory=list)
    redis_doctor_sessions: list = field(default_factory=list)


SCALE_MAP = {
    10_000: {
        "departments": 10,
        "specializations": 15,
        "diseases": 100,
        "medical_services": 50,
        "medications": 80,
        "patients": 2_000,
        "doctors": 50,
        "visits": 10_000,
    },
    100_000: {
        "departments": 20,
        "specializations": 25,
        "diseases": 400,
        "medical_services": 150,
        "medications": 300,
        "patients": 15_000,
        "doctors": 150,
        "visits": 100_000,
    },
    500_000: {
        "departments": 30,
        "specializations": 40,
        "diseases": 800,
        "medical_services": 300,
        "medications": 600,
        "patients": 50_000,
        "doctors": 400,
        "visits": 500_000,
    },
    1_000_000: {
        "departments": 50,
        "specializations": 60,
        "diseases": 1_500,
        "medical_services": 500,
        "medications": 1_000,
        "patients": 100_000,
        "doctors": 800,
        "visits": 1_000_000,
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

SPECIALIZATION_NAMES = [
    "Kardiologia", "Neurologia", "Ortopedia", "Dermatologia",
    "Pediatria", "Onkologia", "Okulistyka", "Chirurgia",
    "Psychiatria", "Urologia", "Ginekologia", "Endokrynologia",
    "Gastroenterologia", "Pulmonologia", "Reumatologia",
    "Nefrologia", "Hematologia", "Radiologia", "Anestezjologia",
    "Laryngologia", "Kardiochirurgia", "Geriatria", "Alergologia",
    "Diabetologia", "Immunologia", "Rehabilitacja", "Neonatologia",
    "Toksykologia", "Medycyna ratunkowa", "Medycyna pracy",
    "Medycyna sportowa", "Transplantologia", "Chirurgia plastyczna",
    "Genetyka kliniczna", "Medycyna nuklearna", "Patomorfologia",
    "Farmakologia kliniczna", "Balneologia", "Epidemiologia",
    "Medycyna rodzinna", "Chirurgia naczyniowa", "Foniatria",
    "Seksuologia", "Medycyna paliatywna", "Chirurgia klatki piersiowej",
    "Chirurgia dziecięca", "Neonatologia kliniczna", "Medycyna morska",
    "Medycyna lotnicza", "Andrologia", "Perinatologia",
    "Onkologia kliniczna", "Intensywna terapia", "Choroby zakaźne",
    "Neurochirurgia", "Otolaryngologia", "Medycyna wewnętrzna",
    "Chirurgia ogólna", "Medycyna sądowa", "Hipertensjologia",
]


ProgressCallback = Optional[Callable[[str], None]]


class DataGenerator:
    """Generuje komplet danych dla podanej skali wizyt."""

    def __init__(self, scale: int):
        if scale not in SCALE_MAP:
            raise ValueError(
                f"Nieobsługiwana skala: {scale}. Dostępne: {list(SCALE_MAP.keys())}"
            )
        self.scale = scale
        self.cfg = SCALE_MAP[scale]

    def generate(self, progress_callback: ProgressCallback = None) -> GeneratedData:
        data = GeneratedData()

        def _report(msg: str):
            if progress_callback:
                progress_callback(msg)

        _report("Generowanie tabel słownikowych...")
        data.departments = self._gen_departments(self.cfg["departments"])
        data.specializations = self._gen_specializations(self.cfg["specializations"])
        data.diseases = self._gen_diseases(self.cfg["diseases"])
        data.medical_services = self._gen_medical_services(self.cfg["medical_services"])
        data.medications = self._gen_medications(self.cfg["medications"])

        _report("Generowanie pacjentów...")
        data.patients = self._gen_patients(self.cfg["patients"])

        _report("Generowanie lekarzy...")
        data.doctors = self._gen_doctors(
            self.cfg["doctors"], self.cfg["departments"], self.cfg["specializations"]
        )

        _report(f"Generowanie {self.cfg['visits']} wizyt...")
        data.visits = self._gen_visits(
            self.cfg["visits"], self.cfg["patients"], self.cfg["doctors"]
        )

        _report("Generowanie wykonanych usług...")
        data.performed_services = self._gen_performed_services(
            data.visits, self.cfg["medical_services"]
        )

        _report("Generowanie diagnoz...")
        data.diagnoses = self._gen_diagnoses(data.visits, self.cfg["diseases"])

        _report("Generowanie recept i pozycji recept...")
        data.prescriptions, data.prescription_items = (
            self._gen_prescriptions_and_items(data.visits, self.cfg["medications"])
        )

        _report("Generowanie wyników badań...")
        data.test_results = self._gen_test_results(data.visits)

        _report("Budowanie dokumentów MongoDB...")
        data.mongo_patients = self._build_mongo_documents(data)

        _report("Budowanie struktur Redis...")
        data.redis_visit_statuses, data.redis_doctor_sessions = (
            self._build_redis_data(data)
        )

        _report("Zakończono generowanie danych.")
        return data

    # ── Generatory tabel SQL ─────────────────────────────────────────

    @staticmethod
    def _gen_departments(n: int) -> list[tuple]:
        return [
            (i + 1, fake.unique.company()[:60], fake.phone_number()[:20])
            for i in range(n)
        ]

    @staticmethod
    def _gen_specializations(n: int) -> list[tuple]:
        names = SPECIALIZATION_NAMES[:n]
        while len(names) < n:
            names.append(f"Specjalizacja_{len(names) + 1}")
        return [(i + 1, names[i]) for i in range(n)]

    @staticmethod
    def _gen_diseases(n: int) -> list[tuple]:
        return [
            (
                i + 1,
                f"{chr(65 + i % 26)}{(i // 26):02d}.{i % 10}",
                fake.sentence(nb_words=3)[:80],
            )
            for i in range(n)
        ]

    @staticmethod
    def _gen_medical_services(n: int) -> list[tuple]:
        return [
            (i + 1, f"Usługa medyczna {i + 1}", round(random.uniform(50, 2000), 2))
            for i in range(n)
        ]

    @staticmethod
    def _gen_medications(n: int) -> list[tuple]:
        return [
            (i + 1, f"Lek_{i + 1}", fake.word().capitalize())
            for i in range(n)
        ]

    @staticmethod
    def _gen_patients(n: int) -> list[tuple]:
        rows = []
        for i in range(n):
            gender = random.choice(GENDERS)
            first = fake.first_name_male() if gender == "M" else fake.first_name_female()
            rows.append((
                i + 1,
                fake.numerify("###########"),
                first,
                fake.last_name(),
                fake.date_of_birth(minimum_age=1, maximum_age=95),
                gender,
            ))
        return rows

    @staticmethod
    def _gen_doctors(n: int, n_deps: int, n_specs: int) -> list[tuple]:
        rows = []
        for i in range(n):
            rows.append((
                i + 1,
                random.randint(1, n_deps),
                random.randint(1, n_specs),
                fake.first_name(),
                fake.last_name(),
                fake.numerify("#######"),
            ))
        return rows

    @staticmethod
    def _gen_visits(n: int, n_patients: int, n_doctors: int) -> list[tuple]:
        base_date = date.today() - timedelta(days=730)
        rows = []
        for i in range(n):
            rows.append((
                i + 1,
                random.randint(1, n_patients),
                random.randint(1, n_doctors),
                base_date + timedelta(days=random.randint(0, 730)),
                random.choice(VISIT_STATUSES),
            ))
        return rows

    @staticmethod
    def _gen_performed_services(visits: list[tuple], n_services: int) -> list[tuple]:
        rows = []
        sid = 1
        for v in visits:
            for _ in range(random.randint(0, 2)):
                rows.append((
                    sid,
                    v[0],
                    random.randint(1, n_services),
                    random.randint(1, 3),
                    round(random.uniform(50, 3000), 2),
                ))
                sid += 1
        return rows

    @staticmethod
    def _gen_diagnoses(visits: list[tuple], n_diseases: int) -> list[tuple]:
        rows = []
        did = 1
        for v in visits:
            for _ in range(random.randint(0, 2)):
                rows.append((
                    did,
                    v[0],
                    random.randint(1, n_diseases),
                    random.choice(DIAGNOSIS_TYPES),
                    fake.sentence(nb_words=5) if random.random() > 0.3 else "",
                ))
                did += 1
        return rows

    @staticmethod
    def _gen_prescriptions_and_items(
        visits: list[tuple], n_medications: int
    ) -> tuple[list[tuple], list[tuple]]:
        prescriptions = []
        items = []
        pid = 1
        iid = 1
        for v in visits:
            if random.random() < 0.4:
                code = fake.bothify("RX-####-????").upper()
                issue_date = v[3]
                prescriptions.append((pid, v[0], code, issue_date))
                for _ in range(random.randint(1, 3)):
                    dosage = f"{random.choice([1, 2, 3])}x{random.choice([100, 200, 500])}mg"
                    items.append((
                        iid, pid, random.randint(1, n_medications), dosage
                    ))
                    iid += 1
                pid += 1
        return prescriptions, items

    @staticmethod
    def _gen_test_results(visits: list[tuple]) -> list[tuple]:
        rows = []
        tid = 1
        for v in visits:
            if random.random() < 0.3:
                for _ in range(random.randint(1, 3)):
                    param = random.choice(PARAM_NAMES)
                    value = round(random.uniform(0.1, 500), 2)
                    unit = random.choice(UNITS)
                    mn = round(random.uniform(0, value * 0.5), 2)
                    mx = round(value * 1.5 + random.uniform(0, 50), 2)
                    rows.append((tid, v[0], param, value, unit, mn, mx))
                    tid += 1
        return rows

    # ── Budowa dokumentów MongoDB ────────────────────────────────────

    @staticmethod
    def _build_mongo_documents(data: GeneratedData) -> list[dict]:
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
            doc = {
                "_id": p[0],
                "national_id": p[1],
                "first_name": p[2],
                "last_name": p[3],
                "birth_date": (
                    p[4].isoformat() if isinstance(p[4], date) else str(p[4])
                ),
                "gender": p[5],
                "visits": [],
            }
            for v in visits_by_patient.get(p[0], []):
                visit_doc = {
                    "visit_id": v[0],
                    "doctor_id": v[2],
                    "visit_date": (
                        v[3].isoformat() if isinstance(v[3], date) else str(v[3])
                    ),
                    "status": v[4],
                    "performed_services": [
                        {
                            "service_id": s[2],
                            "quantity": s[3],
                            "final_price": s[4],
                        }
                        for s in services_by_visit.get(v[0], [])
                    ],
                    "diagnoses": [
                        {
                            "disease_id": d[2],
                            "diagnosis_type": d[3],
                            "notes": d[4],
                        }
                        for d in diags_by_visit.get(v[0], [])
                    ],
                    "prescriptions": [],
                    "test_results": [
                        {
                            "parameter_name": t[2],
                            "result_value": t[3],
                            "unit": t[4],
                            "min_norm": t[5],
                            "max_norm": t[6],
                        }
                        for t in tests_by_visit.get(v[0], [])
                    ],
                }
                for rx in rx_by_visit.get(v[0], []):
                    rx_doc = {
                        "prescription_code": rx[2],
                        "issue_date": (
                            rx[3].isoformat()
                            if isinstance(rx[3], date)
                            else str(rx[3])
                        ),
                        "items": [
                            {"medication_id": it[2], "dosage": it[3]}
                            for it in items_by_rx.get(rx[0], [])
                        ],
                    }
                    visit_doc["prescriptions"].append(rx_doc)
                doc["visits"].append(visit_doc)
            documents.append(doc)
        return documents

    # ── Budowa danych Redis ──────────────────────────────────────────

    @staticmethod
    def _build_redis_data(
        data: GeneratedData,
    ) -> tuple[list[tuple], list[tuple]]:
        statuses = [(f"visit:status:{v[0]}", v[4]) for v in data.visits]
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
