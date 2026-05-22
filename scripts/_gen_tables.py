"""Generuje wartości tabel CREATE/READ/UPDATE/DELETE z results_all.csv
dla aktualnych skal 500000/1000000/10000000."""
import csv
from collections import defaultdict

SCALES = ["500000", "1000000", "10000000"]
DBS = ["PostgreSQL", "MySQL", "MongoDB", "Redis"]

data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))  # [op][scen][scale][(db,idx)] = ms

with open("results/results_all.csv", encoding="utf-8") as f:
    r = csv.DictReader(f)
    for row in r:
        ms = float(row["Average_Time_Seconds"]) * 1000
        key = (row["Database"], row["Indexed"])
        data[row["Operation_Type"]][row["Scenario_Name"]][row["Scale"]][key] = ms


def fmt(v):
    if v is None:
        return "---"
    if v >= 100:
        return f"{v:.1f}"
    return f"{v:.2f}"


def print_op_table(op, idx_filter):
    print(f"\n## {op} idx={idx_filter}")
    scenarios = sorted(data[op].keys())
    for sc in scenarios:
        print(f"\n{sc}")
        for scale in SCALES:
            vals = []
            for db in DBS:
                v = data[op][sc][scale].get((db, idx_filter))
                vals.append(fmt(v))
            print(f"  {scale:>10}  PG={vals[0]:>8}  My={vals[1]:>8}  Mo={vals[2]:>8}  Re={vals[3]:>8}")


for op in ["CREATE", "READ", "UPDATE", "DELETE"]:
    for idxf in ["False", "True"]:
        print_op_table(op, idxf)

# Coefficients
print("\n\n## S_read = bez/z, K_write = z/bez")
for op in ["READ", "CREATE", "UPDATE", "DELETE"]:
    print(f"\n[{op}]")
    for sc in sorted(data[op].keys()):
        for db in DBS:
            ratios = []
            for scale in SCALES:
                bez = data[op][sc][scale].get((db, "False"))
                zi = data[op][sc][scale].get((db, "True"))
                if bez and zi:
                    if op == "READ":
                        ratios.append(f"{bez/zi:.2f}x")
                    else:
                        ratios.append(f"{zi/bez:.2f}x")
                else:
                    ratios.append("---")
            print(f"  {sc:35} {db:11} : {ratios[0]:>8} -> {ratios[1]:>8} -> {ratios[2]:>8}")
