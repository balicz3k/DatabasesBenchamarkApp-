import csv
with open('results/results_all.csv',encoding='utf-8') as f:
    r = csv.DictReader(f)
    for row in r:
        if row['Database']=='PostgreSQL' and row['Operation_Type']=='CREATE':
            print(f"scale={row['Scale']:>8} {row['Scenario_Name']:35} idx={row['Indexed']:6} avg={float(row['Average_Time_Seconds'])*1000:10.3f}ms")
