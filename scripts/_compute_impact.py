import csv
rows = list(csv.DictReader(open('results/results_all.csv')))
data = {}
for r in rows:
    key = (r['Database'], r['Scale'], r['Indexed'], r['Scenario_Name'])
    data[key] = float(r['Median_Time_Seconds'])*1000

def pct(db, sc, scale='1000000'):
    no = data.get((db, scale, 'False', sc))
    yes = data.get((db, scale, 'True', sc))
    if not no or not yes:
        return 'n/d'
    delta = (yes - no) / no * 100
    sign = '+' if delta >= 0 else ''
    return f'{sign}{delta:.0f}%'

scenarios = [
    'insert_patient', 'insert_visit',
    'select_visits_with_doctor', 'select_patient_full_history', 'select_aggregated_costs',
    'update_service_price', 'update_diagnosis_notes',
    'delete_patient', 'delete_visit_cascade'
]
for sc in scenarios:
    print(f"{sc:<32} PG={pct('PostgreSQL',sc):>8}  MySQL={pct('MySQL',sc):>8}  Mongo={pct('MongoDB',sc):>8}")
