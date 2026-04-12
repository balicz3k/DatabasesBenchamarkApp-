"""
app.py -- GUI (Tkinter) do interaktywnego sterowania benchmarkami.
Alternatywa dla CLI run_all.py.
"""

import os
import threading
import tkinter as tk
from tkinter import ttk, messagebox
import queue

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd

from core.database import ConnectionManager, DatabaseType
from core.generator import DataGenerator
from core.seeder import DatabaseSeeder
from core.benchmark import (
    BenchmarkEngine,
    RESULTS_FILE_NO_INDEX,
    RESULTS_FILE_INDEXED,
    RESULTS_DIR,
)

BG = "#1e1e2e"
BG_CARD = "#2a2a3d"
FG = "#cdd6f4"
GREEN = "#a6e3a1"
RED = "#f38ba8"
BLUE = "#89b4fa"
MAUVE = "#cba6f7"
YELLOW = "#f9e2af"
TEAL = "#94e2d5"
FONT = "Segoe UI"


class GUIApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("ZTDB - Benchmark Baz Danych")
        self.root.configure(bg=BG)
        self.root.geometry("1000x800")

        self.running = True
        self.msg_queue: queue.Queue = queue.Queue()
        self.cm = ConnectionManager()
        self.current_scale = 10_000

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self._build_styles()
        self._build_ui()
        self._poll_queue()
        self._check_connections()

    def _on_close(self):
        self.running = False
        if hasattr(self, "_poll_id"):
            self.root.after_cancel(self._poll_id)
        self.root.withdraw()
        try:
            DatabaseSeeder(self.cm).clear_all()
        except Exception:
            pass
        self.root.destroy()
        os._exit(0)

    def _build_styles(self):
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TNotebook", background=BG, borderwidth=0)
        style.configure(
            "TNotebook.Tab",
            background=BG_CARD, foreground=FG,
            font=(FONT, 10, "bold"), padding=[15, 8],
        )
        style.map(
            "TNotebook.Tab",
            background=[("selected", MAUVE)],
            foreground=[("selected", "#11111b")],
        )
        style.configure("TFrame", background=BG)

    def _build_ui(self):
        tk.Label(
            self.root, text="ZTDB - Benchmark i Analiza Indeksow",
            font=(FONT, 18, "bold"), bg=BG, fg=MAUVE,
        ).pack(pady=(15, 5))

        nb = ttk.Notebook(self.root)
        nb.pack(fill="both", expand=True, padx=20, pady=10)

        tab1 = ttk.Frame(nb)
        nb.add(tab1, text="1. Start & Generowanie")
        self._build_tab1(tab1)

        tab2 = ttk.Frame(nb)
        nb.add(tab2, text="2. Test Bez Indeksow")
        self._build_tab2(tab2)

        tab3 = ttk.Frame(nb)
        nb.add(tab3, text="3. Indeksy & Test 2")
        self._build_tab3(tab3)

        tab4 = ttk.Frame(nb)
        nb.add(tab4, text="4. Analiza (Wykresy & EXPLAIN)")
        self._build_tab4(tab4)

    def _build_tab1(self, parent):
        f1 = tk.LabelFrame(
            parent, text="Status Polaczen", bg=BG, fg=YELLOW,
            font=(FONT, 11, "bold"), bd=1, padx=10, pady=10,
        )
        f1.pack(fill="x", padx=10, pady=10)

        self.status_labels = {}
        for db in DatabaseType:
            f = tk.Frame(f1, bg=BG)
            f.pack(side="left", expand=True)
            tk.Label(f, text=db.value, font=(FONT, 10, "bold"), bg=BG, fg=FG).pack()
            lbl = tk.Label(f, text="Sprawdzanie...", font=(FONT, 9), bg=BG, fg=YELLOW)
            lbl.pack()
            self.status_labels[db.value] = lbl

        f2 = tk.LabelFrame(
            parent, text="Generowanie Danych", bg=BG, fg=YELLOW,
            font=(FONT, 11, "bold"), bd=1, padx=10, pady=10,
        )
        f2.pack(fill="x", padx=10, pady=10)

        row = tk.Frame(f2, bg=BG)
        row.pack(pady=10)
        for scale, txt in [(10_000, "10k"), (100_000, "100k"), (500_000, "500k")]:
            tk.Button(
                row, text=f"Wstaw {txt}", font=(FONT, 10, "bold"),
                bg=BLUE, fg="#11111b",
                command=lambda s=scale: self._start_seed(s),
            ).pack(side="left", padx=10)

        self.gen_status = tk.Label(
            f2, text="Gotowy", font=(FONT, 10), bg=BG, fg="#6c7086",
        )
        self.gen_status.pack(pady=5)

    def _build_tab2(self, parent):
        f = tk.LabelFrame(
            parent, text="Benchmark BEZ INDEKSOW", bg=BG, fg=YELLOW,
            font=(FONT, 11, "bold"), bd=1, padx=10, pady=20,
        )
        f.pack(fill="x", padx=10, pady=10)

        tk.Button(
            f, text="Uruchom Test Bez Indeksow", font=(FONT, 11, "bold"),
            bg=MAUVE, fg="#11111b",
            command=lambda: self._start_benchmark(False),
        ).pack(pady=10)

        self.bench1_status = tk.Label(f, text="", font=(FONT, 10), bg=BG, fg="#6c7086")
        self.bench1_status.pack()

    def _build_tab3(self, parent):
        f1 = tk.LabelFrame(
            parent, text="Krok 1: Tworzenie Indeksow", bg=BG, fg=YELLOW,
            font=(FONT, 11, "bold"), bd=1, padx=10, pady=10,
        )
        f1.pack(fill="x", padx=10, pady=10)

        tk.Button(
            f1, text="Stworz Indeksy", font=(FONT, 11, "bold"),
            bg=TEAL, fg="#11111b", command=self._start_indexing,
        ).pack(pady=10)
        self.idx_status = tk.Label(f1, text="", font=(FONT, 10), bg=BG, fg="#6c7086")
        self.idx_status.pack()

        f2 = tk.LabelFrame(
            parent, text="Krok 2: Benchmark Z INDEKSAMI", bg=BG, fg=YELLOW,
            font=(FONT, 11, "bold"), bd=1, padx=10, pady=10,
        )
        f2.pack(fill="x", padx=10, pady=10)

        tk.Button(
            f2, text="Uruchom Test Z Indeksami", font=(FONT, 11, "bold"),
            bg=MAUVE, fg="#11111b",
            command=lambda: self._start_benchmark(True),
        ).pack(pady=10)
        self.bench2_status = tk.Label(f2, text="", font=(FONT, 10), bg=BG, fg="#6c7086")
        self.bench2_status.pack()

    def _build_tab4(self, parent):
        f1 = tk.LabelFrame(
            parent, text="Wykresy Porownawcze", bg=BG, fg=YELLOW,
            font=(FONT, 11, "bold"), bd=1, padx=10, pady=10,
        )
        f1.pack(fill="x", padx=10, pady=10)

        row = tk.Frame(f1, bg=BG)
        row.pack(pady=5)
        tk.Label(row, text="Baza danych:", bg=BG, fg=FG, font=(FONT, 10)).pack(side="left", padx=5)
        self.chart_db_var = tk.StringVar(value=DatabaseType.POSTGRES.value)
        ttk.Combobox(
            row, textvariable=self.chart_db_var,
            values=[db.value for db in DatabaseType],
            state="readonly", width=15,
        ).pack(side="left", padx=5)

        tk.Button(
            f1, text="Pokaz Porownanie (Przed / Po)", font=(FONT, 11, "bold"),
            bg=BLUE, fg="#11111b", command=self._show_charts,
        ).pack(pady=10)

        f2 = tk.LabelFrame(
            parent, text="Generowanie Planow EXPLAIN", bg=BG, fg=YELLOW,
            font=(FONT, 11, "bold"), bd=1, padx=10, pady=10,
        )
        f2.pack(fill="x", padx=10, pady=10)
        tk.Button(
            f2, text="Drukuj EXPLAIN do pliku", font=(FONT, 11, "bold"),
            bg=TEAL, fg="#11111b", command=self._start_explain,
        ).pack(pady=10)
        self.explain_status = tk.Label(f2, text="", font=(FONT, 10), bg=BG, fg="#6c7086")
        self.explain_status.pack()

    # ── Queue & callbacks ────────────────────────────────────────────

    def _poll_queue(self):
        if not self.running:
            return
        while not self.msg_queue.empty():
            msg_type, payload = self.msg_queue.get_nowait()
            handlers = {
                "status": self._handle_status,
                "gen": lambda p: self.gen_status.config(text=p, fg=YELLOW),
                "gen_done": lambda p: self.gen_status.config(text=p, fg=GREEN),
                "bench1": lambda p: self.bench1_status.config(text=p, fg=YELLOW),
                "bench1_done": lambda p: self.bench1_status.config(text=p, fg=GREEN),
                "idx": lambda p: self.idx_status.config(text=p, fg=YELLOW),
                "idx_done": lambda p: self.idx_status.config(text=p, fg=GREEN),
                "bench2": lambda p: self.bench2_status.config(text=p, fg=YELLOW),
                "bench2_done": lambda p: self.bench2_status.config(text=p, fg=GREEN),
                "exp": lambda p: self.explain_status.config(text=p, fg=GREEN),
            }
            handler = handlers.get(msg_type)
            if handler:
                handler(payload)
        self._poll_id = self.root.after(150, self._poll_queue)

    def _handle_status(self, payload):
        for db_name, ok in payload.items():
            lbl = self.status_labels.get(db_name)
            if lbl:
                lbl.config(
                    text="OK Polaczono" if ok else "BLAD",
                    fg=GREEN if ok else RED,
                )

    def _check_connections(self):
        def worker():
            self.msg_queue.put(("status", self.cm.ping_all()))
        threading.Thread(target=worker, daemon=True).start()

    def _start_seed(self, scale: int):
        self.current_scale = scale

        def worker():
            try:
                self.msg_queue.put(("gen", "Generowanie w pamieci..."))
                gen = DataGenerator(scale)
                data = gen.generate(lambda m: self.msg_queue.put(("gen", m)))
                self.msg_queue.put(("gen", "Wstawianie do baz..."))
                DatabaseSeeder(self.cm).seed_all(
                    data, lambda m: self.msg_queue.put(("gen", m))
                )
                self.msg_queue.put(("gen_done", "OK Wstawiono dane!"))
            except Exception as e:
                self.msg_queue.put(("gen", f"Blad: {e}"))

        threading.Thread(target=worker, daemon=True).start()

    def _start_benchmark(self, is_indexed: bool):
        lbl = "bench2" if is_indexed else "bench1"

        def worker():
            try:
                engine = BenchmarkEngine(self.cm, self.current_scale)
                engine.run_benchmarks(
                    is_indexed, lambda m: self.msg_queue.put((lbl, m))
                )
                self.msg_queue.put((f"{lbl}_done", "OK Zakonczone!"))
            except Exception as e:
                self.msg_queue.put((lbl, f"Blad: {e}"))

        threading.Thread(target=worker, daemon=True).start()

    def _start_indexing(self):
        def worker():
            try:
                DatabaseSeeder(self.cm).create_indexes(
                    lambda m: self.msg_queue.put(("idx", m))
                )
                self.msg_queue.put(("idx_done", "OK Indeksy utworzone!"))
            except Exception as e:
                self.msg_queue.put(("idx", f"Blad: {e}"))

        threading.Thread(target=worker, daemon=True).start()

    def _start_explain(self):
        def worker():
            try:
                engine = BenchmarkEngine(self.cm, self.current_scale)
                engine.generate_explain(lambda m: self.msg_queue.put(("exp", m)))
            except Exception as e:
                self.msg_queue.put(("exp", f"Blad: {e}"))

        threading.Thread(target=worker, daemon=True).start()

    def _show_charts(self):
        selected_db = self.chart_db_var.get()
        if not os.path.isfile(RESULTS_FILE_NO_INDEX):
            messagebox.showinfo("Brak danych", "Najpierw wykonaj Test Bez Indeksow.")
            return

        df_no = pd.read_csv(RESULTS_FILE_NO_INDEX)
        has_indexed = os.path.isfile(RESULTS_FILE_INDEXED)
        df_idx = pd.read_csv(RESULTS_FILE_INDEXED) if has_indexed else pd.DataFrame()

        win = tk.Toplevel(self.root)
        win.title(f"Porownanie Wydajnosci - {selected_db}")
        win.configure(bg="#181825")
        win.geometry("1400x900")

        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.patch.set_facecolor("#181825")
        for ax in axes.flatten():
            ax.set_facecolor("#1e1e2e")
            ax.tick_params(colors="#cdd6f4", labelsize=8)
            for spine in ax.spines.values():
                spine.set_color("#45475a")

        ops = ["CREATE", "READ", "UPDATE", "DELETE"]
        for i, op in enumerate(ops):
            ax = axes[i // 2][i % 2]
            ax.set_title(op, color="#cdd6f4", fontsize=12)

            subset = df_no[
                (df_no["Operation_Type"] == op) & (df_no["Database"] == selected_db)
            ]
            if subset.empty:
                continue

            scenarios = subset["Scenario_Name"].unique()
            x = range(len(scenarios))
            vals_no = [
                subset[subset["Scenario_Name"] == s]["Average_Time_Seconds"].values[0]
                for s in scenarios
            ]
            ax.bar(
                [xi - 0.2 for xi in x], vals_no, 0.4,
                label="Brak Indeksow", color="#f38ba8",
            )

            if not df_idx.empty:
                subset_idx = df_idx[
                    (df_idx["Operation_Type"] == op)
                    & (df_idx["Database"] == selected_db)
                ]
                if not subset_idx.empty:
                    vals_idx = [
                        subset_idx[subset_idx["Scenario_Name"] == s][
                            "Average_Time_Seconds"
                        ].values[0]
                        for s in scenarios
                        if s in subset_idx["Scenario_Name"].values
                    ]
                    ax.bar(
                        [xi + 0.2 for xi in range(len(vals_idx))],
                        vals_idx, 0.4,
                        label="Z Indeksami", color="#a6e3a1",
                    )

            labels = [s.replace("_", "\n") for s in scenarios]
            ax.set_xticks(list(x))
            ax.set_xticklabels(labels, color="#a6adc8", rotation=25)
            if i == 0:
                ax.legend(fontsize=8)

        fig.tight_layout()
        canvas = FigureCanvasTkAgg(fig, master=win)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)


if __name__ == "__main__":
    root = tk.Tk()
    GUIApp(root)
    root.mainloop()
