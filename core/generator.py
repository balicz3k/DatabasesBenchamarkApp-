"""
core/generator.py – Generowanie syntetycznych danych medycznych (system szpitalny).
Skala oznacza SUMARYCZNĄ liczbę rekordów we wszystkich tabelach.
Obsługiwane skale: 10 000 / 100 000 / 500 000 / 1 000 000 / 10 000 000 rekordów.

Dane generowane bez zewnętrznych zależności.
Używa polskich list imion i nazwisk oraz wbudowanego modułu random.

Uwaga dotycząca skali 10M (~2M wizyt):
  Generowanie i seeding wymaga ok. 2–4 GB RAM. Dla scale ≥ 5M aktywowany jest
  tryb streaming chunkami po 500k wizyt (oszczędność pamięci).
"""

import gc
import random
import string
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Callable, Optional

try:
    import resource as _resource
    def _ram_mb() -> int:
        return _resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss // 1024
except ImportError:
    import psutil as _psutil
    def _ram_mb() -> int:
        return _psutil.Process().memory_info().rss // (1024 * 1024)

# ── Polskie imiona i nazwiska ───────────────────────────────────────────────

MALE_NAMES = [
    "Adam", "Bartosz", "Cezary", "Damian", "Emil", "Filip", "Grzegorz",
    "Hubert", "Igor", "Jan", "Kamil", "Leszek", "Marek", "Norbert",
    "Oskar", "Piotr", "Rafał", "Sławomir", "Tomasz", "Urszul",
    "Waldemar", "Zbigniew", "Artur", "Bogdan", "Dariusz", "Edward",
    "Franciszek", "Henryk", "Jakub", "Karol", "Łukasz", "Michał",
    "Paweł", "Robert", "Sebastian", "Tadeusz", "Wiktor", "Andrzej",
    "Benedykt", "Czesław", "Dominik", "Ernest", "Gustaw", "Ireneusz",
    "Józef", "Kazimierz", "Mateusz", "Nikodem", "Olaf", "Przemysław",
]

FEMALE_NAMES = [
    "Anna", "Barbara", "Celina", "Dorota", "Elżbieta", "Felicja", "Grażyna",
    "Halina", "Irena", "Jolanta", "Katarzyna", "Lidia", "Małgorzata",
    "Natalia", "Oliwia", "Paulina", "Regina", "Sylwia", "Teresa",
    "Urszula", "Wanda", "Zofia", "Agnieszka", "Beata", "Danuta",
    "Edyta", "Gabriela", "Helena", "Izabela", "Joanna", "Kamila",
    "Lucyna", "Monika", "Nadia", "Patrycja", "Renata", "Stanisława",
    "Wiktoria", "Aleksandra", "Bożena", "Ewa", "Genowefa", "Honorata",
    "Julia", "Klaudia", "Maria", "Nikola", "Patrycja", "Roksana",
    "Tamara",
]

LAST_NAMES_MALE = [
    "Kowalski", "Nowak", "Wiśniewski", "Dąbrowski", "Lewandowski",
    "Wójcik", "Kamiński", "Kowalczyk", "Zieliński", "Szymański",
    "Woźniak", "Kozłowski", "Jankowski", "Wojciechowski", "Kwiatkowski",
    "Kaczmarek", "Mazur", "Krawczyk", "Piotrowski", "Grabowski",
    "Nowakowski", "Pawłowski", "Michalski", "Nowicki", "Adamczyk",
    "Dudek", "Zając", "Wieczorek", "Jabłoński", "Król",
    "Majewski", "Olszewski", "Jaworski", "Wróbel", "Malinowski",
    "Pawlak", "Witkowski", "Walczak", "Stępień", "Górski",
    "Rutkowski", "Michalak", "Sikora", "Ostrowski", "Baran",
    "Duda", "Szewczyk", "Tomaszewski", "Pietrzak", "Marciniak",
]

LAST_NAMES_FEMALE = [
    name + "a" if not name.endswith("a") else name
    for name in LAST_NAMES_MALE
]

DEPT_TYPES = [
    "Kardiologii", "Neurologii", "Ortopedii", "Dermatologii",
    "Pediatrii", "Onkologii", "Okulistyki", "Chirurgii",
    "Psychiatrii", "Urologii", "Ginekologii", "Endokrynologii",
    "Gastroenterologii", "Pulmonologii", "Reumatologii",
    "Nefrologii", "Hematologii", "Radiologii", "Anestezjologii",
    "Laryngologii", "Geriatrii", "Alergologii", "Diabetologii",
    "Immunologii", "Rehabilitacji", "Neonatologii", "Toksykologii",
    "Medycyny ratunkowej", "Medycyny pracy", "Transplantologii",
    "Chirurgii plastycznej", "Genetyki", "Neurochirurgii",
    "Chorób zakaźnych", "Intensywnej terapii", "Medycyny paliatywnej",
    "Foniatrii", "Seksuologii", "Perinatologii", "Andrologii",
    "Medycyny sądowej", "Epidemiologii", "Medycyny rodzinnej",
    "Chirurgii naczyniowej", "Patomorfologii", "Farmakologii",
    "Balneologii", "Medycyny lotniczej", "Medycyny morskiej", "Medycyny sportowej",
]

DISEASE_WORDS = [
    "Nadciśnienie tętnicze", "Cukrzyca typu 2", "Choroba wieńcowa",
    "Migotanie przedsionków", "Niewydolność serca", "Astma oskrzelowa",
    "POChP", "Zapalenie płuc", "Choroba refluksowa", "Wrzód trawienny",
    "Kamica żółciowa", "Zapalenie wyrostka", "Niedokrwistość",
    "Niedoczynność tarczycy", "Nadczynność tarczycy", "Osteoporoza",
    "Artretyzm", "Reumatoidalne zapalenie stawów", "Dna moczanowa",
    "Kamienie nerkowe", "Przewlekła choroba nerek", "Udar mózgu",
    "Epilepsja", "Parkinson", "Alzheimer", "Demencja",
    "Depresja", "Zaburzenia lękowe", "Schizofrenia", "Bezsenność",
    "Zapalenie spojówek", "Jaskra", "Zaćma", "Zwyrodnienie plamki",
    "Zapalenie ucha", "Zapalenie zatok", "Migrenowe bóle głowy",
    "Przepuklina krążka", "Stenoza kręgosłupa", "Złamanie osteoporotyczne",
    "Łuszczyca", "Egzema", "Trądzik", "Pokrzywka", "Czerniak",
    "Rak płuca", "Rak jelita grubego", "Rak prostaty", "Rak piersi",
    "Białaczka", "Chłoniak", "Szpiczak", "Anemia aplastyczna",
    "Zapalenie wątroby B", "Zapalenie wątroby C", "Marskość wątroby",
    "Trzustka zaporowa", "Nieswoiste zapalenie jelit", "Celiakia",
    "Alergia pokarmowa", "Atopowe zapalenie skóry", "Toczeń",
    "Twardzina układowa", "Zapalenie naczyń", "Amyloidoza",
    "Porfirie", "Hemofilia", "Trombofilia", "Małopłytkowość",
    "Cystic fibrosis", "Dystrofia mięśniowa", "Stwardnienie zanikowe",
    "Stwardnienie rozsiane", "Padaczka skroniowa", "Neuropatia cukrzycowa",
    "Retinopatia cukrzycowa", "Nefropatia cukrzycowa", "Stopa cukrzycowa",
    "Chromanie przestankowe", "Tętniak aorty", "Zakrzepica żylna",
    "Zatorowość płucna", "Nadciśnienie płucne", "Kardiomiopatia",
]

DIAG_NOTES = [
    "Pacjent wymaga dalszej obserwacji.", "Zalecono kontrolę za 3 miesiące.",
    "Przepisano leczenie farmakologiczne.", "Skierowano na badania dodatkowe.",
    "Stan pacjenta stabilny.", "Wymaga hospitalizacji.",
    "Zalecono zmianę trybu życia.", "Skierowano do specjalisty.",
    "Brak powikłań.", "Rokowanie dobre przy odpowiednim leczeniu.",
    "Pacjent nie wyraził zgody na zabieg.", "Wskazana dieta niskosodowa.",
    "Monitorowanie ciśnienia tętniczego.", "Zalecono rehabilitację.",
    "Wyniki badań w granicach normy.", "Konieczna kontrola glikemii.",
    "Przepisano insulinę.", "Zwiększono dawkę leków.",
    "Zmniejszono dawkę leków.", "Odstawiono dotychczasowe leczenie.",
    "", "", "",  # puste notatki są realistyczne
]

ACTIVE_SUBSTANCES = [
    "Metoprolol", "Amlodipina", "Lisinopril", "Atorwastatyna",
    "Metformina", "Bisoprolol", "Omeprazol", "Pantoprazol",
    "Escitalopram", "Sertralin", "Amoksycylina", "Azytromycyna",
    "Ibuprofen", "Paracetamol", "Tramadol", "Diazepam",
    "Furosemid", "Spironolakton", "Warfaryna", "Rywaroksaban",
    "Clopidogrel", "Aspiryna", "Digoksyna", "Levotyroksyna",
    "Prednizon", "Deksametazon", "Insulin glargine", "Insulin lispro",
    "Gabapentyna", "Pregabalina", "Karbamazepina", "Lewetyracetam",
    "Haloperidol", "Klozapina", "Rysperydon", "Olanzapina",
    "Allopurinol", "Kolchicyna", "Metotreksat", "Sulfasalazyna",
    "Hydrochlorotiazyd", "Ramipryl", "Walsartan", "Karwedilol",
    "Tamsulosina", "Sildenafil", "Finasteryd", "Anastrozol",
    "Tamoksyfen", "Docetaksel",
]


# ── Stałe domenowe ──────────────────────────────────────────────────────────

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


# ── SCALE_MAP ────────────────────────────────────────────────────────────────
# UWAGA: Klucz `scale` oznacza SUMARYCZNĄ liczbę rekordów we WSZYSTKICH tabelach.
# Tabele zachowują proporcje (visits ≈ 0.205 × total; diagnozy/usługi ≈ visits;
# prescriptions ≈ 0.4 × visits; prescription_items ≈ 0.8 × visits;
# test_results ≈ 0.6 × visits; patients ≈ 0.016 × total; doctors ≈ 8e-5 × total).
# Realna liczba wyniesie ok. 0.97–1.00 × klucza scale.

SCALE_MAP = {
    10_000: {
        "departments": 10,
        "specializations": 15,
        "diseases": 100,
        "medical_services": 50,
        "medications": 80,
        "patients": 400,
        "doctors": 20,
        "visits": 2_000,
    },
    100_000: {
        "departments": 15,
        "specializations": 20,
        "diseases": 120,
        "medical_services": 60,
        "medications": 100,
        "patients": 1_600,
        "doctors": 20,
        "visits": 20_000,
    },
    500_000: {
        "departments": 10,
        "specializations": 15,
        "diseases": 100,
        "medical_services": 50,
        "medications": 80,
        "patients": 8_000,
        "doctors": 40,
        "visits": 100_000,
    },
    1_000_000: {
        "departments": 15,
        "specializations": 20,
        "diseases": 150,
        "medical_services": 75,
        "medications": 120,
        "patients": 16_000,
        "doctors": 80,
        "visits": 200_000,
    },
    10_000_000: {
        "departments": 30,
        "specializations": 40,
        "diseases": 500,
        "medical_services": 200,
        "medications": 400,
        "patients": 160_000,
        "doctors": 800,
        "visits": 2_000_000,
    },
}


ProgressCallback = Optional[Callable[[str], None]]


# ── Pomocnicze funkcje generujące ───────────────────────────────────────────

def _rand_pesel() -> str:
    """Losowy 11-cyfrowy numer PESEL-like."""
    return "".join(str(random.randint(0, 9)) for _ in range(11))


def _rand_phone() -> str:
    """Losowy numer telefonu w formacie polskim."""
    return f"+48 {random.randint(100, 999)} {random.randint(100, 999)} {random.randint(100, 999)}"


def _rand_license() -> str:
    """Losowy numer licencji lekarza."""
    return "".join(str(random.randint(0, 9)) for _ in range(7))


def _rand_date(start_year: int = 1930, end_year: int = 2008) -> date:
    """Losowa data urodzenia."""
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def _rand_rx_code() -> str:
    """Losowy kod recepty."""
    digits = "".join(str(random.randint(0, 9)) for _ in range(4))
    letters = "".join(random.choice(string.ascii_uppercase) for _ in range(4))
    return f"RX-{digits}-{letters}"


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


class DataGenerator:
    """Generuje komplet danych dla podanej skali wizyt."""

    def __init__(self, scale: int):
        if scale not in SCALE_MAP:
            raise ValueError(
                f"Nieobsługiwana skala: {scale}. Dostępne: {list(SCALE_MAP.keys())}"
            )
        self.scale = scale
        self.cfg = SCALE_MAP[scale]

    def generate(self, progress_callback: ProgressCallback = None, seed: int = 42) -> GeneratedData:
        random.seed(seed)
        data = GeneratedData()

        def _report(msg: str):
            if progress_callback:
                progress_callback(msg)

        def _report_ram(label: str):
            try:
                _report(f"  [RAM] {label}: ~{_ram_mb()} MB")
            except Exception:
                pass

        _report("Generowanie tabel słownikowych...")
        data.departments = self._gen_departments(self.cfg["departments"])
        data.specializations = self._gen_specializations(self.cfg["specializations"])
        data.diseases = self._gen_diseases(self.cfg["diseases"])
        data.medical_services = self._gen_medical_services(self.cfg["medical_services"])
        data.medications = self._gen_medications(self.cfg["medications"])

        _report("Generowanie pacjentów...")
        data.patients = self._gen_patients(self.cfg["patients"])
        _report_ram("po pacjentach")

        _report("Generowanie lekarzy...")
        data.doctors = self._gen_doctors(
            self.cfg["doctors"], self.cfg["departments"], self.cfg["specializations"]
        )

        _report(f"Generowanie {self.cfg['visits']:,} wizyt...")
        data.visits = self._gen_visits(
            self.cfg["visits"], self.cfg["patients"], self.cfg["doctors"]
        )
        _report_ram("po wizytach")

        _report("Generowanie wykonanych usług...")
        data.performed_services = self._gen_performed_services(
            data.visits, self.cfg["medical_services"]
        )
        _report_ram("po usługach")
        gc.collect()

        _report("Generowanie diagnoz...")
        data.diagnoses = self._gen_diagnoses(data.visits, self.cfg["diseases"])
        _report_ram("po diagnozach")
        gc.collect()

        _report("Generowanie recept i pozycji recept...")
        data.prescriptions, data.prescription_items = (
            self._gen_prescriptions_and_items(data.visits, self.cfg["medications"])
        )
        _report_ram("po receptach")
        gc.collect()

        _report("Generowanie wyników badań...")
        data.test_results = self._gen_test_results(data.visits)
        _report_ram("po badaniach")
        gc.collect()

        _report("Zakończono generowanie danych SQL.")
        _report_ram("końcowe")
        return data

    # ── Generatory tabel SQL ─────────────────────────────────────────────────

    @staticmethod
    def _gen_departments(n: int) -> list[tuple]:
        dept_list = DEPT_TYPES[:n]
        while len(dept_list) < n:
            dept_list.append(f"Oddzial_{len(dept_list) + 1}")
        return [
            (i + 1, f"Oddzial {dept_list[i]}", _rand_phone())
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
        rows = []
        disease_pool = DISEASE_WORDS * ((n // len(DISEASE_WORDS)) + 1)
        for i in range(n):
            icd = f"{chr(65 + i % 26)}{(i // 26):02d}.{i % 10}"
            name = disease_pool[i] if i < len(disease_pool) else f"Choroba_{i+1}"
            if i >= len(DISEASE_WORDS):
                name = f"{name} (wariant {i // len(DISEASE_WORDS)})"
            rows.append((i + 1, icd, name[:80]))
        return rows

    @staticmethod
    def _gen_medical_services(n: int) -> list[tuple]:
        return [
            (i + 1, f"Usluga medyczna {i + 1}", round(random.uniform(50, 2000), 2))
            for i in range(n)
        ]

    @staticmethod
    def _gen_medications(n: int) -> list[tuple]:
        substance_pool = ACTIVE_SUBSTANCES * ((n // len(ACTIVE_SUBSTANCES)) + 1)
        return [
            (i + 1, f"Lek_{i + 1}", substance_pool[i % len(substance_pool)])
            for i in range(n)
        ]

    @staticmethod
    def _gen_patients(n: int) -> list[tuple]:
        rows = []
        for i in range(n):
            gender = random.choice(GENDERS)
            if gender == "M":
                first = random.choice(MALE_NAMES)
                last = random.choice(LAST_NAMES_MALE)
            else:
                first = random.choice(FEMALE_NAMES)
                last = random.choice(LAST_NAMES_FEMALE)
            rows.append((
                i + 1,
                _rand_pesel(),
                first,
                last,
                _rand_date(1930, 2008),
                gender,
            ))
        return rows

    @staticmethod
    def _gen_doctors(n: int, n_deps: int, n_specs: int) -> list[tuple]:
        rows = []
        for i in range(n):
            gender = random.choice(GENDERS)
            first = random.choice(MALE_NAMES if gender == "M" else FEMALE_NAMES)
            last = random.choice(LAST_NAMES_MALE if gender == "M" else LAST_NAMES_FEMALE)
            rows.append((
                i + 1,
                random.randint(1, n_deps),
                random.randint(1, n_specs),
                first,
                last,
                _rand_license(),
            ))
        return rows

    @staticmethod
    def _gen_visits(n: int, n_patients: int, n_doctors: int) -> list[tuple]:
        base_date = date.today() - timedelta(days=730)
        # Cap: max wizyt na pacjenta (ogranicza rozmiar dokumentu Mongo
        # – BSON ma limit 16 MB, bez capa pojedyncze patient.visits
        # może go przekroczyć przy 5 mln wizyt × few hot patients).
        MAX_VISITS_PER_PATIENT = 200
        visit_counts: dict[int, int] = {}
        rows = []
        for i in range(n):
            for _ in range(20):
                pid = random.randint(1, n_patients)
                if visit_counts.get(pid, 0) < MAX_VISITS_PER_PATIENT:
                    break
            visit_counts[pid] = visit_counts.get(pid, 0) + 1
            rows.append((
                i + 1,
                pid,
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
                    random.choice(DIAG_NOTES) if random.random() > 0.3 else "",
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
                code = _rand_rx_code()
                issue_date = v[3]
                prescriptions.append((pid, v[0], code, issue_date))
                for _ in range(random.randint(1, 3)):
                    dosage = f"{random.choice([1, 2, 3])}x{random.choice([100, 200, 500])}mg"
                    items.append((iid, pid, random.randint(1, n_medications), dosage))
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

    # ── Streaming (large scales ≥ 5 M) ─────────────────────────────────────

    def generate_base_data(self, seed: int = 42) -> "GeneratedData":
        """Generuje tylko dane bazowe bez wizyt i tabel podrzędnych.
        Używany przy streamingowym seedowaniu dla skal >= 5 M.
        """
        random.seed(seed)
        data = GeneratedData()
        data.departments = self._gen_departments(self.cfg["departments"])
        data.specializations = self._gen_specializations(self.cfg["specializations"])
        data.diseases = self._gen_diseases(self.cfg["diseases"])
        data.medical_services = self._gen_medical_services(self.cfg["medical_services"])
        data.medications = self._gen_medications(self.cfg["medications"])
        data.patients = self._gen_patients(self.cfg["patients"])
        data.doctors = self._gen_doctors(
            self.cfg["doctors"], self.cfg["departments"], self.cfg["specializations"]
        )
        return data

    def generate_visits_streaming(self, chunk_size: int = 500_000):
        """Generator – zwraca paczki wizyt bez materializacji całej listy.
        Każda iteracja yields tuple:
          (visits, diagnoses, services, prescriptions, rx_items, test_results)
        IDs są globalnie unikalne w całym zbiorze.
        """
        n_total = self.cfg["visits"]
        n_patients = self.cfg["patients"]
        n_doctors = self.cfg["doctors"]
        n_services = self.cfg["medical_services"]
        n_diseases = self.cfg["diseases"]
        n_meds = self.cfg["medications"]
        base_date = date.today() - timedelta(days=730)

        visit_id = 1
        diag_id = 1
        svc_id = 1
        rx_id = 1
        rxi_id = 1
        tr_id = 1

        while visit_id <= n_total:
            n_chunk = min(chunk_size, n_total - visit_id + 1)

            visits = [
                (
                    visit_id + i,
                    random.randint(1, n_patients),
                    random.randint(1, n_doctors),
                    base_date + timedelta(days=random.randint(0, 730)),
                    random.choice(VISIT_STATUSES),
                )
                for i in range(n_chunk)
            ]

            diagnoses = []
            for v in visits:
                for _ in range(random.randint(0, 2)):
                    diagnoses.append((
                        diag_id, v[0], random.randint(1, n_diseases),
                        random.choice(DIAGNOSIS_TYPES),
                        random.choice(DIAG_NOTES) if random.random() > 0.3 else "",
                    ))
                    diag_id += 1

            services = []
            for v in visits:
                for _ in range(random.randint(0, 2)):
                    services.append((
                        svc_id, v[0], random.randint(1, n_services),
                        random.randint(1, 3), round(random.uniform(50, 3000), 2),
                    ))
                    svc_id += 1

            prescriptions = []
            rx_items = []
            for v in visits:
                if random.random() < 0.4:
                    prescriptions.append((rx_id, v[0], _rand_rx_code(), v[3]))
                    for _ in range(random.randint(1, 3)):
                        dosage = (
                            f"{random.choice([1, 2, 3])}x"
                            f"{random.choice([100, 200, 500])}mg"
                        )
                        rx_items.append((rxi_id, rx_id, random.randint(1, n_meds), dosage))
                        rxi_id += 1
                    rx_id += 1

            test_results = []
            for v in visits:
                if random.random() < 0.3:
                    for _ in range(random.randint(1, 3)):
                        param = random.choice(PARAM_NAMES)
                        value = round(random.uniform(0.1, 500), 2)
                        unit = random.choice(UNITS)
                        mn = round(random.uniform(0, value * 0.5), 2)
                        mx = round(value * 1.5 + random.uniform(0, 50), 2)
                        test_results.append((tr_id, v[0], param, value, unit, mn, mx))
                        tr_id += 1

            yield visits, diagnoses, services, prescriptions, rx_items, test_results
            visit_id += n_chunk

    # ── Budowa dokumentów MongoDB ────────────────────────────────────────────

    @staticmethod
    def _build_lookup_dicts(data: "GeneratedData"):
        """Buduje słowniki pomocnicze do budowy dokumentów MongoDB.
        Zwraca tuple (visits_by_patient, services_by_visit, diags_by_visit,
                       rx_by_visit, items_by_rx, tests_by_visit).
        Słowniki trzymają referencje do istniejących krotek (brak duplikacji pamięci).
        """
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

        return (visits_by_patient, services_by_visit, diags_by_visit,
                rx_by_visit, items_by_rx, tests_by_visit)

    @staticmethod
    def _mongo_doc_generator(data: "GeneratedData"):
        """Generator – zwraca dokumenty MongoDB jeden po drugim bez materializacji listy.
        Idealny do streamingowego wstawiania przy dużych skalach (500k–10M),
        ponieważ nigdy nie trzyma w pamięci jednocześnie wszystkich dokumentów.
        """
        (visits_by_patient, services_by_visit, diags_by_visit,
         rx_by_visit, items_by_rx, tests_by_visit) = DataGenerator._build_lookup_dicts(data)

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
                        {"service_id": s[2], "quantity": s[3], "final_price": s[4]}
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
                            rx[3].isoformat() if isinstance(rx[3], date) else str(rx[3])
                        ),
                        "items": [
                            {"medication_id": it[2], "dosage": it[3]}
                            for it in items_by_rx.get(rx[0], [])
                        ],
                    }
                    visit_doc["prescriptions"].append(rx_doc)
                doc["visits"].append(visit_doc)
            yield doc

    @staticmethod
    def _build_mongo_documents(data: "GeneratedData") -> list[dict]:
        """Buduje pełną listę dokumentów MongoDB (dla małych skal lub testów).
        Dla dużych skal (≥500k) użyj _mongo_doc_generator() zamiast tej metody.
        """
        return list(DataGenerator._mongo_doc_generator(data))

    # ── Budowa danych Redis ──────────────────────────────────────────────────

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
