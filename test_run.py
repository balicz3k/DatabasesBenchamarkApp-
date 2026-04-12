import sys
from core.database import ConnectionManager
from core.generator import DataGenerator
from core.seeder import DatabaseSeeder
from core.benchmark import BenchmarkEngine

cm = ConnectionManager()

print("Status baz:", cm.ping_all())

print("Generowanie...")
gen = DataGenerator(10_000)
data = gen.generate()

print("Wstawianie...")
seeder = DatabaseSeeder(cm)
seeder.seed_all(data)

print("Benchmark 1...")
bench = BenchmarkEngine(cm, 10_000)
bench.run_benchmarks(is_indexed=False)

print("Indeksy...")
seeder.create_indexes()

print("Benchmark 2...")
bench.run_benchmarks(is_indexed=True)

print("Explain...")
bench.generate_explain()

print("ALL TESTS PASSED")
sys.exit(0)
