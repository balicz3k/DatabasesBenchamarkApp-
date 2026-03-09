"""
benchmark.py – 24 scenariusze CRUD (6×C, 6×R, 6×U, 6×D)
dla PostgreSQL, MySQL, MongoDB i Redis.
Każdy scenariusz wykonywany 3× – zapisywany jest uśredniony czas.
"""

import csv
import os
import random
import time
from datetime import date

from db_config import (
    get_pg_connection, get_mysql_connection,
    get_mongo_client, get_redis_client,
)

RESULTS_FILE = "results.csv"
RUNS = 3


# ═══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _time_it(func, *args, **kwargs):
    """Wykonuje func i zwraca (wynik, czas_s)."""
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return result, elapsed


def _avg_time(func, *args, **kwargs):
    """Wykonuje func RUNS razy i zwraca średni czas."""
    times = []
    for _ in range(RUNS):
        _, t = _time_it(func, *args, **kwargs)
        times.append(t)
    return sum(times) / len(times)


def _random_id(max_id: int) -> int:
    return random.randint(1, max(1, max_id))


# ═══════════════════════════════════════════════════════════════════════════════
#  PostgreSQL / MySQL – wspólna logika (parametr placeholder %s dla obu)
# ═══════════════════════════════════════════════════════════════════════════════

def _sql_scenarios(get_conn, db_name: str, scale: int):
    """Uruchamia 24 scenariuszy na relacyjnej bazie (PG lub MySQL)."""
    results = []
    max_patient = scale // 5
    max_doctor = scale // 200
    max_visit = scale

    def _run(op_type, name, func):
        avg = _avg_time(func)
        results.append((db_name, scale, op_type, name, avg))

    # ---------- CREATE (6) ----------

    def c1_insert_patient():
        conn = get_conn(); cur = conn.cursor()
        pid = random.randint(10_000_000, 99_999_999)
        cur.execute(
            "INSERT INTO patients (id, national_id, first_name, last_name, birth_date, gender) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (pid, "99999999999", "Test", "Patient", "2000-01-01", "M"),
        )
        conn.commit(); cur.execute("DELETE FROM patients WHERE id=%s", (pid,)); conn.commit()
        cur.close(); conn.close()

    def c2_insert_visit():
        conn = get_conn(); cur = conn.cursor()
        vid = random.randint(10_000_000, 99_999_999)
        cur.execute(
            "INSERT INTO visits (id, patient_id, doctor_id, visit_date, status) "
            "VALUES (%s,%s,%s,%s,%s)",
            (vid, _random_id(max_patient), _random_id(max_doctor), "2025-06-01", "scheduled"),
        )
        conn.commit(); cur.execute("DELETE FROM visits WHERE id=%s", (vid,)); conn.commit()
        cur.close(); conn.close()

    def c3_insert_diagnosis():
        conn = get_conn(); cur = conn.cursor()
        did = random.randint(10_000_000, 99_999_999)
        cur.execute(
            "INSERT INTO diagnoses (id, visit_id, disease_id, diagnosis_type, notes) "
            "VALUES (%s,%s,%s,%s,%s)",
            (did, _random_id(max_visit), _random_id(100), "primary", "bench test"),
        )
        conn.commit(); cur.execute("DELETE FROM diagnoses WHERE id=%s", (did,)); conn.commit()
        cur.close(); conn.close()

    def c4_insert_prescription():
        conn = get_conn(); cur = conn.cursor()
        pid = random.randint(10_000_000, 99_999_999)
        iid = random.randint(10_000_000, 99_999_999)
        cur.execute(
            "INSERT INTO prescriptions (id, visit_id, prescription_code, issue_date) "
            "VALUES (%s,%s,%s,%s)",
            (pid, _random_id(max_visit), "RX-BENCH-0001", "2025-06-01"),
        )
        cur.execute(
            "INSERT INTO prescription_items (id, prescription_id, medication_id, dosage) "
            "VALUES (%s,%s,%s,%s)",
            (iid, pid, _random_id(80), "1x500mg"),
        )
        conn.commit()
        cur.execute("DELETE FROM prescription_items WHERE id=%s", (iid,))
        cur.execute("DELETE FROM prescriptions WHERE id=%s", (pid,))
        conn.commit(); cur.close(); conn.close()

    def c5_insert_service():
        conn = get_conn(); cur = conn.cursor()
        sid = random.randint(10_000_000, 99_999_999)
        cur.execute(
            "INSERT INTO performed_services (id, visit_id, service_id, quantity, final_price) "
            "VALUES (%s,%s,%s,%s,%s)",
            (sid, _random_id(max_visit), _random_id(50), 1, 199.99),
        )
        conn.commit(); cur.execute("DELETE FROM performed_services WHERE id=%s", (sid,)); conn.commit()
        cur.close(); conn.close()

    def c6_insert_test_result():
        conn = get_conn(); cur = conn.cursor()
        tid = random.randint(10_000_000, 99_999_999)
        cur.execute(
            "INSERT INTO test_results (id, visit_id, parameter_name, result_value, unit, min_norm, max_norm) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (tid, _random_id(max_visit), "Hemoglobina", 13.5, "g/dL", 12.0, 16.0),
        )
        conn.commit(); cur.execute("DELETE FROM test_results WHERE id=%s", (tid,)); conn.commit()
        cur.close(); conn.close()

    _run("CREATE", "insert_patient", c1_insert_patient)
    _run("CREATE", "insert_visit", c2_insert_visit)
    _run("CREATE", "insert_diagnosis", c3_insert_diagnosis)
    _run("CREATE", "insert_prescription", c4_insert_prescription)
    _run("CREATE", "insert_service", c5_insert_service)
    _run("CREATE", "insert_test_result", c6_insert_test_result)

    # ---------- READ (6) – z JOINami ----------

    def r1_read_patient_by_id():
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT * FROM patients WHERE id = %s", (_random_id(max_patient),))
        cur.fetchone(); cur.close(); conn.close()

    def r2_read_visits_with_doctor():
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "SELECT v.id, v.visit_date, v.status, d.first_name, d.last_name "
            "FROM visits v JOIN doctors d ON v.doctor_id = d.id "
            "WHERE v.patient_id = %s LIMIT 50",
            (_random_id(max_patient),),
        )
        cur.fetchall(); cur.close(); conn.close()

    def r3_read_visit_diagnoses():
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "SELECT dg.id, ds.icd10_code, ds.name, dg.diagnosis_type, dg.notes "
            "FROM diagnoses dg JOIN diseases ds ON dg.disease_id = ds.id "
            "WHERE dg.visit_id = %s",
            (_random_id(max_visit),),
        )
        cur.fetchall(); cur.close(); conn.close()

    def r4_read_patient_full_history():
        conn = get_conn(); cur = conn.cursor()
        pid = _random_id(max_patient)
        cur.execute(
            "SELECT v.id, v.visit_date, v.status, d.first_name AS doc_fname, "
            "d.last_name AS doc_lname, ps.final_price, ms.name AS service_name, "
            "dg.diagnosis_type, ds.name AS disease_name "
            "FROM visits v "
            "JOIN doctors d ON v.doctor_id = d.id "
            "LEFT JOIN performed_services ps ON ps.visit_id = v.id "
            "LEFT JOIN medical_services ms ON ps.service_id = ms.id "
            "LEFT JOIN diagnoses dg ON dg.visit_id = v.id "
            "LEFT JOIN diseases ds ON dg.disease_id = ds.id "
            "WHERE v.patient_id = %s LIMIT 200",
            (pid,),
        )
        cur.fetchall(); cur.close(); conn.close()

    def r5_read_services_with_prices():
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "SELECT ps.id, ms.name, ms.base_price, ps.quantity, ps.final_price "
            "FROM performed_services ps "
            "JOIN medical_services ms ON ps.service_id = ms.id "
            "WHERE ps.visit_id = %s",
            (_random_id(max_visit),),
        )
        cur.fetchall(); cur.close(); conn.close()

    def r6_read_prescriptions_with_meds():
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "SELECT p.prescription_code, pi.dosage, m.name AS med_name, m.active_substance "
            "FROM prescriptions p "
            "JOIN prescription_items pi ON pi.prescription_id = p.id "
            "JOIN medications m ON pi.medication_id = m.id "
            "WHERE p.visit_id = %s",
            (_random_id(max_visit),),
        )
        cur.fetchall(); cur.close(); conn.close()

    _run("READ", "read_patient_by_id", r1_read_patient_by_id)
    _run("READ", "read_visits_with_doctor", r2_read_visits_with_doctor)
    _run("READ", "read_visit_diagnoses", r3_read_visit_diagnoses)
    _run("READ", "read_patient_full_history", r4_read_patient_full_history)
    _run("READ", "read_services_with_prices", r5_read_services_with_prices)
    _run("READ", "read_prescriptions_with_meds", r6_read_prescriptions_with_meds)

    # ---------- UPDATE (6) ----------

    def u1_update_patient_name():
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "UPDATE patients SET last_name = %s WHERE id = %s",
            ("Benchmarkowy", _random_id(max_patient)),
        )
        conn.commit(); cur.close(); conn.close()

    def u2_update_visit_status():
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "UPDATE visits SET status = %s WHERE id = %s",
            ("completed", _random_id(max_visit)),
        )
        conn.commit(); cur.close(); conn.close()

    def u3_update_service_price():
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "UPDATE performed_services SET final_price = %s WHERE visit_id = %s",
            (999.99, _random_id(max_visit)),
        )
        conn.commit(); cur.close(); conn.close()

    def u4_update_diagnosis_notes():
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "UPDATE diagnoses SET notes = %s WHERE visit_id = %s",
            ("Updated note", _random_id(max_visit)),
        )
        conn.commit(); cur.close(); conn.close()

    def u5_update_doctor_license():
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "UPDATE doctors SET license_number = %s WHERE id = %s",
            ("NEW-LIC-0001", _random_id(max_doctor)),
        )
        conn.commit(); cur.close(); conn.close()

    def u6_update_department_phone():
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "UPDATE departments SET phone = %s WHERE id = %s",
            ("+48 000 000 000", _random_id(10)),
        )
        conn.commit(); cur.close(); conn.close()

    _run("UPDATE", "update_patient_name", u1_update_patient_name)
    _run("UPDATE", "update_visit_status", u2_update_visit_status)
    _run("UPDATE", "update_service_price", u3_update_service_price)
    _run("UPDATE", "update_diagnosis_notes", u4_update_diagnosis_notes)
    _run("UPDATE", "update_doctor_license", u5_update_doctor_license)
    _run("UPDATE", "update_department_phone", u6_update_department_phone)

    # ---------- DELETE (6) ----------

    def d1_delete_test_result():
        conn = get_conn(); cur = conn.cursor()
        # Wstaw i usuń
        tid = random.randint(10_000_000, 99_999_999)
        cur.execute(
            "INSERT INTO test_results (id, visit_id, parameter_name, result_value, unit, min_norm, max_norm) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (tid, _random_id(max_visit), "BenchParam", 1.0, "mg/dL", 0.5, 2.0),
        )
        conn.commit()
        start = time.perf_counter()
        cur.execute("DELETE FROM test_results WHERE id = %s", (tid,))
        conn.commit()
        elapsed = time.perf_counter() - start
        cur.close(); conn.close()
        return elapsed

    def d2_delete_prescription_item():
        conn = get_conn(); cur = conn.cursor()
        pid = random.randint(10_000_000, 99_999_999)
        iid = random.randint(10_000_000, 99_999_999)
        cur.execute(
            "INSERT INTO prescriptions (id, visit_id, prescription_code, issue_date) "
            "VALUES (%s,%s,%s,%s)",
            (pid, _random_id(max_visit), "RX-DEL-BENCH", "2025-01-01"),
        )
        cur.execute(
            "INSERT INTO prescription_items (id, prescription_id, medication_id, dosage) "
            "VALUES (%s,%s,%s,%s)",
            (iid, pid, _random_id(80), "1x100mg"),
        )
        conn.commit()
        start = time.perf_counter()
        cur.execute("DELETE FROM prescription_items WHERE id = %s", (iid,))
        conn.commit()
        elapsed = time.perf_counter() - start
        cur.execute("DELETE FROM prescriptions WHERE id = %s", (pid,))
        conn.commit(); cur.close(); conn.close()
        return elapsed

    def d3_delete_diagnosis():
        conn = get_conn(); cur = conn.cursor()
        did = random.randint(10_000_000, 99_999_999)
        cur.execute(
            "INSERT INTO diagnoses (id, visit_id, disease_id, diagnosis_type, notes) "
            "VALUES (%s,%s,%s,%s,%s)",
            (did, _random_id(max_visit), _random_id(100), "primary", "to delete"),
        )
        conn.commit()
        start = time.perf_counter()
        cur.execute("DELETE FROM diagnoses WHERE id = %s", (did,))
        conn.commit()
        elapsed = time.perf_counter() - start
        cur.close(); conn.close()
        return elapsed

    def d4_delete_service():
        conn = get_conn(); cur = conn.cursor()
        sid = random.randint(10_000_000, 99_999_999)
        cur.execute(
            "INSERT INTO performed_services (id, visit_id, service_id, quantity, final_price) "
            "VALUES (%s,%s,%s,%s,%s)",
            (sid, _random_id(max_visit), _random_id(50), 1, 100.0),
        )
        conn.commit()
        start = time.perf_counter()
        cur.execute("DELETE FROM performed_services WHERE id = %s", (sid,))
        conn.commit()
        elapsed = time.perf_counter() - start
        cur.close(); conn.close()
        return elapsed

    def d5_delete_visit():
        conn = get_conn(); cur = conn.cursor()
        vid = random.randint(10_000_000, 99_999_999)
        cur.execute(
            "INSERT INTO visits (id, patient_id, doctor_id, visit_date, status) "
            "VALUES (%s,%s,%s,%s,%s)",
            (vid, _random_id(max_patient), _random_id(max_doctor), "2025-01-01", "scheduled"),
        )
        conn.commit()
        start = time.perf_counter()
        cur.execute("DELETE FROM visits WHERE id = %s", (vid,))
        conn.commit()
        elapsed = time.perf_counter() - start
        cur.close(); conn.close()
        return elapsed

    def d6_delete_patient():
        conn = get_conn(); cur = conn.cursor()
        pid = random.randint(10_000_000, 99_999_999)
        cur.execute(
            "INSERT INTO patients (id, national_id, first_name, last_name, birth_date, gender) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (pid, "00000000000", "Del", "Patient", "1990-01-01", "F"),
        )
        conn.commit()
        start = time.perf_counter()
        cur.execute("DELETE FROM patients WHERE id = %s", (pid,))
        conn.commit()
        elapsed = time.perf_counter() - start
        cur.close(); conn.close()
        return elapsed

    # Dla DELETE uśredniamy sami (bo mierzymy tylko fazę DELETE)
    for name, func in [
        ("delete_test_result", d1_delete_test_result),
        ("delete_prescription_item", d2_delete_prescription_item),
        ("delete_diagnosis", d3_delete_diagnosis),
        ("delete_service", d4_delete_service),
        ("delete_visit", d5_delete_visit),
        ("delete_patient", d6_delete_patient),
    ]:
        times = [func() for _ in range(RUNS)]
        avg = sum(times) / len(times)
        results.append((db_name, scale, "DELETE", name, avg))

    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  MongoDB
# ═══════════════════════════════════════════════════════════════════════════════

def _mongo_scenarios(scale: int):
    results = []
    max_patient = scale // 5

    def _run(op_type, name, func):
        avg = _avg_time(func)
        results.append(("MongoDB", scale, op_type, name, avg))

    # CREATE
    def c1():
        client, db = get_mongo_client()
        db.patients.insert_one({
            "_id": random.randint(10_000_000, 99_999_999),
            "national_id": "00000000000", "first_name": "Mongo",
            "last_name": "Test", "birth_date": "2000-01-01", "gender": "M", "visits": [],
        })
        client.close()

    def c2():
        client, db = get_mongo_client()
        pid = _random_id(max_patient)
        db.patients.update_one(
            {"_id": pid},
            {"$push": {"visits": {
                "visit_id": random.randint(10_000_000, 99_999_999),
                "doctor_id": 1, "visit_date": "2025-06-01", "status": "scheduled",
                "performed_services": [], "diagnoses": [], "prescriptions": [], "test_results": [],
            }}},
        )
        client.close()

    def c3():
        client, db = get_mongo_client()
        pid = _random_id(max_patient)
        db.patients.update_one(
            {"_id": pid, "visits.0": {"$exists": True}},
            {"$push": {"visits.0.diagnoses": {
                "disease_id": 1, "diagnosis_type": "primary", "notes": "bench",
            }}},
        )
        client.close()

    def c4():
        client, db = get_mongo_client()
        pid = _random_id(max_patient)
        db.patients.update_one(
            {"_id": pid, "visits.0": {"$exists": True}},
            {"$push": {"visits.0.prescriptions": {
                "prescription_code": "RX-BENCH", "issue_date": "2025-06-01",
                "items": [{"medication_id": 1, "dosage": "1x500mg"}],
            }}},
        )
        client.close()

    def c5():
        client, db = get_mongo_client()
        pid = _random_id(max_patient)
        db.patients.update_one(
            {"_id": pid, "visits.0": {"$exists": True}},
            {"$push": {"visits.0.performed_services": {
                "service_id": 1, "quantity": 1, "final_price": 199.99,
            }}},
        )
        client.close()

    def c6():
        client, db = get_mongo_client()
        pid = _random_id(max_patient)
        db.patients.update_one(
            {"_id": pid, "visits.0": {"$exists": True}},
            {"$push": {"visits.0.test_results": {
                "parameter_name": "Hemoglobina", "result_value": 13.5,
                "unit": "g/dL", "min_norm": 12.0, "max_norm": 16.0,
            }}},
        )
        client.close()

    _run("CREATE", "insert_patient", c1)
    _run("CREATE", "insert_visit", c2)
    _run("CREATE", "insert_diagnosis", c3)
    _run("CREATE", "insert_prescription", c4)
    _run("CREATE", "insert_service", c5)
    _run("CREATE", "insert_test_result", c6)

    # READ
    def r1():
        client, db = get_mongo_client()
        db.patients.find_one({"_id": _random_id(max_patient)})
        client.close()

    def r2():
        client, db = get_mongo_client()
        list(db.patients.find(
            {"_id": _random_id(max_patient)},
            {"visits.visit_date": 1, "visits.status": 1, "visits.doctor_id": 1},
        ))
        client.close()

    def r3():
        client, db = get_mongo_client()
        list(db.patients.find(
            {"_id": _random_id(max_patient)},
            {"visits.diagnoses": 1},
        ))
        client.close()

    def r4():
        client, db = get_mongo_client()
        db.patients.find_one({"_id": _random_id(max_patient)})
        client.close()

    def r5():
        client, db = get_mongo_client()
        list(db.patients.aggregate([
            {"$unwind": "$visits"},
            {"$unwind": "$visits.performed_services"},
            {"$limit": 50},
            {"$project": {
                "visits.performed_services.service_id": 1,
                "visits.performed_services.final_price": 1,
            }},
        ]))
        client.close()

    def r6():
        client, db = get_mongo_client()
        list(db.patients.aggregate([
            {"$unwind": "$visits"},
            {"$unwind": "$visits.prescriptions"},
            {"$limit": 50},
            {"$project": {
                "visits.prescriptions.prescription_code": 1,
                "visits.prescriptions.items": 1,
            }},
        ]))
        client.close()

    _run("READ", "read_patient_by_id", r1)
    _run("READ", "read_visits_with_doctor", r2)
    _run("READ", "read_visit_diagnoses", r3)
    _run("READ", "read_patient_full_history", r4)
    _run("READ", "read_services_with_prices", r5)
    _run("READ", "read_prescriptions_with_meds", r6)

    # UPDATE
    def u1():
        client, db = get_mongo_client()
        db.patients.update_one(
            {"_id": _random_id(max_patient)},
            {"$set": {"last_name": "Benchmarkowy"}},
        )
        client.close()

    def u2():
        client, db = get_mongo_client()
        db.patients.update_one(
            {"_id": _random_id(max_patient), "visits.0": {"$exists": True}},
            {"$set": {"visits.0.status": "completed"}},
        )
        client.close()

    def u3():
        client, db = get_mongo_client()
        db.patients.update_one(
            {"_id": _random_id(max_patient), "visits.0.performed_services.0": {"$exists": True}},
            {"$set": {"visits.0.performed_services.0.final_price": 999.99}},
        )
        client.close()

    def u4():
        client, db = get_mongo_client()
        db.patients.update_one(
            {"_id": _random_id(max_patient), "visits.0.diagnoses.0": {"$exists": True}},
            {"$set": {"visits.0.diagnoses.0.notes": "Updated note"}},
        )
        client.close()

    _run("UPDATE", "update_patient_name", u1)
    _run("UPDATE", "update_visit_status", u2)
    _run("UPDATE", "update_service_price", u3)
    _run("UPDATE", "update_diagnosis_notes", u4)
    # update_doctor_license & update_department_phone – N/A for MongoDB
    results.append(("MongoDB", scale, "UPDATE", "update_doctor_license", None))
    results.append(("MongoDB", scale, "UPDATE", "update_department_phone", None))

    # DELETE
    def d1():
        client, db = get_mongo_client()
        db.patients.update_one(
            {"_id": _random_id(max_patient), "visits.0.test_results.0": {"$exists": True}},
            {"$pop": {"visits.0.test_results": -1}},
        )
        client.close()

    def d2():
        client, db = get_mongo_client()
        db.patients.update_one(
            {"_id": _random_id(max_patient), "visits.0.prescriptions.0.items.0": {"$exists": True}},
            {"$pop": {"visits.0.prescriptions.0.items": -1}},
        )
        client.close()

    def d3():
        client, db = get_mongo_client()
        db.patients.update_one(
            {"_id": _random_id(max_patient), "visits.0.diagnoses.0": {"$exists": True}},
            {"$pop": {"visits.0.diagnoses": -1}},
        )
        client.close()

    def d4():
        client, db = get_mongo_client()
        db.patients.update_one(
            {"_id": _random_id(max_patient), "visits.0.performed_services.0": {"$exists": True}},
            {"$pop": {"visits.0.performed_services": -1}},
        )
        client.close()

    def d5():
        client, db = get_mongo_client()
        db.patients.update_one(
            {"_id": _random_id(max_patient), "visits.0": {"$exists": True}},
            {"$pop": {"visits": -1}},
        )
        client.close()

    def d6():
        client, db = get_mongo_client()
        pid = random.randint(10_000_000, 99_999_999)
        db.patients.insert_one({"_id": pid, "national_id": "DEL", "first_name": "D",
                                 "last_name": "D", "visits": []})
        db.patients.delete_one({"_id": pid})
        client.close()

    _run("DELETE", "delete_test_result", d1)
    _run("DELETE", "delete_prescription_item", d2)
    _run("DELETE", "delete_diagnosis", d3)
    _run("DELETE", "delete_service", d4)
    _run("DELETE", "delete_visit", d5)
    _run("DELETE", "delete_patient", d6)

    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  Redis
# ═══════════════════════════════════════════════════════════════════════════════

def _redis_scenarios(scale: int):
    results = []
    max_visit = scale
    max_doctor = scale // 200

    def _run(op_type, name, func):
        avg = _avg_time(func)
        results.append(("Redis", scale, op_type, name, avg))

    # CREATE
    def c1():
        r = get_redis_client()
        pid = random.randint(10_000_000, 99_999_999)
        r.set(f"patient:{pid}", '{"first_name":"Test","last_name":"Redis"}')

    def c2():
        r = get_redis_client()
        vid = random.randint(10_000_000, 99_999_999)
        r.set(f"visit:status:{vid}", "scheduled")

    def c3():
        r = get_redis_client()
        did = random.randint(10_000_000, 99_999_999)
        r.set(f"diag:{did}", '{"disease_id":1}')

    def c4():
        r = get_redis_client()
        rxid = random.randint(10_000_000, 99_999_999)
        r.set(f"rx:{rxid}", '{"code":"RX-BENCH"}')

    def c5():
        r = get_redis_client()
        sid = random.randint(10_000_000, 99_999_999)
        r.set(f"svc:{sid}", '{"service_id":1}')

    def c6():
        r = get_redis_client()
        tid = random.randint(10_000_000, 99_999_999)
        r.set(f"tr:{tid}", '{"param":"Hemoglobina"}')

    _run("CREATE", "insert_patient", c1)
    _run("CREATE", "insert_visit", c2)
    _run("CREATE", "insert_diagnosis", c3)
    _run("CREATE", "insert_prescription", c4)
    _run("CREATE", "insert_service", c5)
    _run("CREATE", "insert_test_result", c6)

    # READ
    def r1():
        r = get_redis_client()
        r.get(f"patient:{_random_id(scale // 5)}")

    def r2():
        r = get_redis_client()
        r.hgetall(f"session:doctor:{_random_id(max_doctor)}")

    def r3():
        r = get_redis_client()
        r.get(f"diag:{_random_id(1000)}")

    def r4():
        r = get_redis_client()
        pipe = r.pipeline()
        pid = _random_id(scale // 5)
        pipe.get(f"patient:{pid}")
        pipe.get(f"visit:status:{_random_id(max_visit)}")
        pipe.execute()

    def r5():
        r = get_redis_client()
        r.get(f"svc:{_random_id(1000)}")

    def r6():
        r = get_redis_client()
        r.get(f"rx:{_random_id(1000)}")

    _run("READ", "read_patient_by_id", r1)
    _run("READ", "read_visits_with_doctor", r2)
    _run("READ", "read_visit_diagnoses", r3)
    _run("READ", "read_patient_full_history", r4)
    _run("READ", "read_services_with_prices", r5)
    _run("READ", "read_prescriptions_with_meds", r6)

    # UPDATE
    def u1():
        r = get_redis_client()
        r.set(f"patient:{_random_id(scale // 5)}", '{"last_name":"Benchmarkowy"}')

    def u2():
        r = get_redis_client()
        r.set(f"visit:status:{_random_id(max_visit)}", "completed")

    _run("UPDATE", "update_patient_name", u1)
    _run("UPDATE", "update_visit_status", u2)
    # N/A for Redis
    results.append(("Redis", scale, "UPDATE", "update_service_price", None))
    results.append(("Redis", scale, "UPDATE", "update_diagnosis_notes", None))

    def u5():
        r = get_redis_client()
        r.hset(f"session:doctor:{_random_id(max_doctor)}", "license_number", "NEW-LIC-0001")

    _run("UPDATE", "update_doctor_license", u5)
    results.append(("Redis", scale, "UPDATE", "update_department_phone", None))

    # DELETE
    def d1():
        r = get_redis_client()
        tid = random.randint(10_000_000, 99_999_999)
        r.set(f"tr:{tid}", "temp"); r.delete(f"tr:{tid}")

    def d2():
        r = get_redis_client()
        iid = random.randint(10_000_000, 99_999_999)
        r.set(f"rx_item:{iid}", "temp"); r.delete(f"rx_item:{iid}")

    def d3():
        r = get_redis_client()
        did = random.randint(10_000_000, 99_999_999)
        r.set(f"diag:{did}", "temp"); r.delete(f"diag:{did}")

    def d4():
        r = get_redis_client()
        sid = random.randint(10_000_000, 99_999_999)
        r.set(f"svc:{sid}", "temp"); r.delete(f"svc:{sid}")

    def d5():
        r = get_redis_client()
        vid = random.randint(10_000_000, 99_999_999)
        r.set(f"visit:status:{vid}", "temp"); r.delete(f"visit:status:{vid}")

    def d6():
        r = get_redis_client()
        pid = random.randint(10_000_000, 99_999_999)
        r.set(f"patient:{pid}", "temp"); r.delete(f"patient:{pid}")

    _run("DELETE", "delete_test_result", d1)
    _run("DELETE", "delete_prescription_item", d2)
    _run("DELETE", "delete_diagnosis", d3)
    _run("DELETE", "delete_service", d4)
    _run("DELETE", "delete_visit", d5)
    _run("DELETE", "delete_patient", d6)

    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  Główna funkcja benchmarkowa
# ═══════════════════════════════════════════════════════════════════════════════

def run_all_benchmarks(scale: int, progress_callback=None):
    """
    Uruchamia 24 scenariuszy CRUD × 4 bazy danych.
    progress_callback(message: str) – opcjonalny callback do UI.
    Zwraca listę wyników i zapisuje do results.csv.
    """
    all_results = []

    def _report(msg):
        if progress_callback:
            progress_callback(msg)

    # PostgreSQL
    _report("Running PostgreSQL benchmarks...")
    try:
        all_results.extend(_sql_scenarios(get_pg_connection, "PostgreSQL", scale))
    except Exception as e:
        _report(f"PostgreSQL error: {e}")

    # MySQL
    _report("Running MySQL benchmarks...")
    try:
        all_results.extend(_sql_scenarios(get_mysql_connection, "MySQL", scale))
    except Exception as e:
        _report(f"MySQL error: {e}")

    # MongoDB
    _report("Running MongoDB benchmarks...")
    try:
        all_results.extend(_mongo_scenarios(scale))
    except Exception as e:
        _report(f"MongoDB error: {e}")

    # Redis
    _report("Running Redis benchmarks...")
    try:
        all_results.extend(_redis_scenarios(scale))
    except Exception as e:
        _report(f"Redis error: {e}")

    # Zapis do CSV
    _report("Saving results to results.csv...")
    file_exists = os.path.isfile(RESULTS_FILE)
    with open(RESULTS_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists or os.path.getsize(RESULTS_FILE) == 0:
            writer.writerow(["Database", "Scale", "Operation_Type", "Scenario_Name", "Average_Time_Seconds"])
        for row in all_results:
            writer.writerow(row)

    _report("Benchmarks complete!")
    return all_results
