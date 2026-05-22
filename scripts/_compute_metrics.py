import pandas as pd

df = pd.read_csv('results/results_all.csv')
df['ms'] = df['Average_Time_Seconds'] * 1000

def t(db, op, scale, idx):
    s = df[(df['Database']==db) & (df['Scenario_Name']==op) & (df['Scale']==scale) & (df['Indexed']==idx)]
    return s['ms'].iloc[0] if len(s) else None

scales = [500000, 1000000, 10000000]

print("=== A) S_read (no_idx / idx) ===")
for op, dbs in [
    ('select_visits_with_doctor', ['PostgreSQL','MySQL','MongoDB']),
    ('select_visit_diagnoses', ['PostgreSQL']),
    ('select_patient_full_history', ['PostgreSQL']),
    ('select_aggregated_costs', ['PostgreSQL']),
    ('select_prescriptions_with_meds', ['PostgreSQL']),
]:
    for db in dbs:
        row = [f"{op:35} {db:11}"]
        for sc in scales:
            tn = t(db, op, sc, False)
            ti = t(db, op, sc, True)
            r = tn/ti if tn and ti else float('nan')
            row.append(f"{r:6.2f}x")
        print(" | ".join(row))

print("\n=== B) K_write (idx / no_idx) ===")
for op, dbs in [
    ('insert_patient', ['PostgreSQL']),
    ('insert_visit', ['PostgreSQL','MySQL']),
    ('update_visit_status', ['PostgreSQL']),
    ('update_service_price', ['PostgreSQL']),
    ('update_diagnosis_notes', ['PostgreSQL']),
]:
    for db in dbs:
        row = [f"{op:35} {db:11}"]
        for sc in scales:
            tn = t(db, op, sc, False)
            ti = t(db, op, sc, True)
            r = ti/tn if tn and ti else float('nan')
            row.append(f"{r:6.2f}x")
        print(" | ".join(row))

print("\n=== C) Absolute @ 10M (ms) [no_idx / idx] ===")
sc = 10000000
pairs = [
    ('PostgreSQL', ['delete_visit_cascade','select_visit_diagnoses','select_aggregated_costs','select_prescriptions_with_meds','update_service_price','update_diagnosis_notes','insert_visit','insert_patient']),
    ('MySQL', ['select_visits_with_doctor','insert_visit','update_visit_status','update_service_price','insert_patient']),
    ('MongoDB', ['select_visits_with_doctor','insert_visit']),
]
for db, ops in pairs:
    for op in ops:
        tn = t(db, op, sc, False)
        ti = t(db, op, sc, True)
        tn_s = f"{tn:8.2f}" if tn is not None else "   --   "
        ti_s = f"{ti:8.2f}" if ti is not None else "   --   "
        print(f"  {db:11} {op:35} {tn_s} / {ti_s} ms")

print("\n=== D) Redis @ 10M (all ops) ===")
r = df[(df['Database']=='Redis') & (df['Scale']==sc) & (df['Indexed']==False)][['Scenario_Name','ms']].sort_values('ms')
print(r.to_string(index=False))
print(f"min={r['ms'].min():.2f}  max={r['ms'].max():.2f}")

print("\n=== E) PG no-idx READ at scales (ms) ===")
for op in ['select_visits_with_doctor','select_visit_diagnoses','select_patient_full_history','select_aggregated_costs','select_prescriptions_with_meds']:
    row = [f"{op:35} PG"]
    for sc2 in scales:
        tn = t('PostgreSQL', op, sc2, False)
        ti = t('PostgreSQL', op, sc2, True)
        row.append(f"{tn:7.2f}/{ti:6.2f}")
    print(" | ".join(row))
