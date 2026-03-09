"""
app.py – Główna aplikacja GUI (Tkinter) do benchmarkowania baz danych.
"""

import threading
import tkinter as tk
from tkinter import ttk, messagebox
import queue
import os

import pandas as pd
import matplotlib

matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.ticker as mticker

from db_config import ping_all
from data_generator import generate_all
from seed_db import seed_all
from benchmark import run_all_benchmarks, RESULTS_FILE


# ═══════════════════════════════════════════════════════════════════════════════
#  Kolory i style
# ═══════════════════════════════════════════════════════════════════════════════

BG = "#1e1e2e"
BG_CARD = "#2a2a3d"
FG = "#cdd6f4"
FG_DIM = "#6c7086"
GREEN = "#a6e3a1"
RED = "#f38ba8"
BLUE = "#89b4fa"
MAUVE = "#cba6f7"
PEACH = "#fab387"
YELLOW = "#f9e2af"
TEAL = "#94e2d5"
FONT_FAMILY = "Segoe UI"


# ═══════════════════════════════════════════════════════════════════════════════
#  Klasa aplikacji
# ═══════════════════════════════════════════════════════════════════════════════

class BenchmarkApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("ZTDB – Benchmark Baz Danych")
        self.root.configure(bg=BG)
        self.root.geometry("920x760")
        self.root.minsize(800, 700)

        self.msg_queue: queue.Queue = queue.Queue()
        self.current_scale: int | None = None

        self._build_ui()
        self._poll_queue()
        self._check_connections()

    # ─── UI ───────────────────────────────────────────────────────────────────

    def _build_ui(self):
        # Tytuł
        title = tk.Label(
            self.root, text="⚡  ZTDB — Benchmark Baz Danych",
            font=(FONT_FAMILY, 18, "bold"), bg=BG, fg=MAUVE,
        )
        title.pack(pady=(18, 6))

        subtitle = tk.Label(
            self.root,
            text="PostgreSQL  ·  MySQL  ·  MongoDB  ·  Redis",
            font=(FONT_FAMILY, 10), bg=BG, fg=FG_DIM,
        )
        subtitle.pack(pady=(0, 14))

        container = tk.Frame(self.root, bg=BG)
        container.pack(fill="both", expand=True, padx=24, pady=(0, 18))

        # ── Sekcja 1: Status Połączeń ──
        self._section_label(container, "1 ─ Status Połączeń")
        status_frame = tk.Frame(container, bg=BG_CARD, bd=0, highlightthickness=1,
                                highlightbackground="#45475a")
        status_frame.pack(fill="x", pady=(0, 14), ipady=8)

        self.status_labels: dict[str, tk.Label] = {}
        db_names = ["PostgreSQL", "MySQL", "MongoDB", "Redis"]
        for i, name in enumerate(db_names):
            f = tk.Frame(status_frame, bg=BG_CARD)
            f.pack(side="left", expand=True, padx=10, pady=6)
            tk.Label(f, text=name, font=(FONT_FAMILY, 10, "bold"),
                     bg=BG_CARD, fg=FG).pack()
            lbl = tk.Label(f, text="Sprawdzanie…", font=(FONT_FAMILY, 9),
                           bg=BG_CARD, fg=YELLOW)
            lbl.pack()
            self.status_labels[name] = lbl

        # ── Sekcja 2: Generowanie Danych ──
        self._section_label(container, "2 ─ Generowanie Danych")
        gen_frame = tk.Frame(container, bg=BG_CARD, bd=0, highlightthickness=1,
                             highlightbackground="#45475a")
        gen_frame.pack(fill="x", pady=(0, 14), ipady=8)

        btn_row = tk.Frame(gen_frame, bg=BG_CARD)
        btn_row.pack(pady=6)
        for scale, label_text in [(10_000, "10k"), (100_000, "100k"), (1_000_000, "1M")]:
            btn = tk.Button(
                btn_row, text=f"  Wstaw {label_text}  ",
                font=(FONT_FAMILY, 10, "bold"), bg=BLUE, fg="#11111b",
                activebackground=TEAL, activeforeground="#11111b",
                relief="flat", cursor="hand2", bd=0,
                command=lambda s=scale: self._start_seed(s),
            )
            btn.pack(side="left", padx=8)

        self.gen_status = tk.Label(gen_frame, text="", font=(FONT_FAMILY, 9),
                                   bg=BG_CARD, fg=FG_DIM)
        self.gen_status.pack(pady=(2, 4))

        # ── Sekcja 3: Testy Wydajnościowe ──
        self._section_label(container, "3 ─ Testy Wydajnościowe (24 CRUD)")
        bench_frame = tk.Frame(container, bg=BG_CARD, bd=0, highlightthickness=1,
                               highlightbackground="#45475a")
        bench_frame.pack(fill="x", pady=(0, 14), ipady=8)

        self.bench_btn = tk.Button(
            bench_frame, text="  ▶  Uruchom 24 Scenariusze CRUD  ",
            font=(FONT_FAMILY, 11, "bold"), bg=MAUVE, fg="#11111b",
            activebackground=PEACH, activeforeground="#11111b",
            relief="flat", cursor="hand2", bd=0,
            command=self._start_benchmark,
        )
        self.bench_btn.pack(pady=6)

        self.bench_status = tk.Label(bench_frame, text="", font=(FONT_FAMILY, 9),
                                      bg=BG_CARD, fg=FG_DIM)
        self.bench_status.pack(pady=(2, 4))

        # ── Sekcja 4: Wizualizacja Wyników ──
        self._section_label(container, "4 ─ Wizualizacja Wyników")
        viz_frame = tk.Frame(container, bg=BG_CARD, bd=0, highlightthickness=1,
                             highlightbackground="#45475a")
        viz_frame.pack(fill="x", pady=(0, 4), ipady=8)

        self.viz_btn = tk.Button(
            viz_frame, text="  📊  Pokaż Wykresy  ",
            font=(FONT_FAMILY, 11, "bold"), bg=TEAL, fg="#11111b",
            activebackground=GREEN, activeforeground="#11111b",
            relief="flat", cursor="hand2", bd=0,
            command=self._show_charts,
        )
        self.viz_btn.pack(pady=6)

    def _section_label(self, parent, text):
        tk.Label(parent, text=text, font=(FONT_FAMILY, 11, "bold"),
                 bg=BG, fg=PEACH, anchor="w").pack(fill="x", pady=(6, 2))

    # ─── Komunikacja z wątkami ────────────────────────────────────────────────

    def _poll_queue(self):
        """Cyklicznie sprawdza kolejkę komunikatów z wątków."""
        while not self.msg_queue.empty():
            msg_type, payload = self.msg_queue.get_nowait()
            if msg_type == "status":
                self._update_statuses(payload)
            elif msg_type == "gen":
                self.gen_status.config(text=payload, fg=FG)
            elif msg_type == "gen_done":
                self.gen_status.config(text=payload, fg=GREEN)
            elif msg_type == "bench":
                self.bench_status.config(text=payload, fg=FG)
            elif msg_type == "bench_done":
                self.bench_status.config(text=payload, fg=GREEN)
                self.bench_btn.config(state="normal")
            elif msg_type == "error":
                self.gen_status.config(text=payload, fg=RED)
                self.bench_status.config(text=payload, fg=RED)
        self.root.after(150, self._poll_queue)

    # ─── 1. Sprawdzanie połączeń ──────────────────────────────────────────────

    def _check_connections(self):
        def _worker():
            result = ping_all()
            self.msg_queue.put(("status", result))

        threading.Thread(target=_worker, daemon=True).start()

    def _update_statuses(self, result: dict[str, bool]):
        for name, ok in result.items():
            lbl = self.status_labels[name]
            if ok:
                lbl.config(text="✔  Połączono", fg=GREEN)
            else:
                lbl.config(text="✘  Błąd", fg=RED)

    # ─── 2. Generowanie danych ────────────────────────────────────────────────

    def _start_seed(self, scale: int):
        self.current_scale = scale
        self.gen_status.config(text=f"Generowanie danych ({scale:,}) …", fg=YELLOW)

        def _worker():
            try:
                self.msg_queue.put(("gen", f"Generowanie danych ({scale:,}) …"))
                data = generate_all(
                    scale,
                    progress_callback=lambda msg: self.msg_queue.put(("gen", msg)),
                )
                self.msg_queue.put(("gen", "Wstawianie danych do baz…"))
                seed_all(
                    data,
                    progress_callback=lambda db, msg: self.msg_queue.put(
                        ("gen", f"[{db}] {msg}")
                    ),
                )
                self.msg_queue.put(("gen_done", f"✔ Zakończono wstawianie ({scale:,})."))
            except Exception as e:
                self.msg_queue.put(("error", f"Błąd: {e}"))

        threading.Thread(target=_worker, daemon=True).start()

    # ─── 3. Benchmark ────────────────────────────────────────────────────────

    def _start_benchmark(self):
        if self.current_scale is None:
            messagebox.showwarning(
                "Brak danych",
                "Najpierw wstaw dane (10k / 100k / 1M), aby uruchomić benchmark.",
            )
            return

        self.bench_btn.config(state="disabled")
        scale = self.current_scale
        self.bench_status.config(text=f"Trwa benchmark ({scale:,}) …", fg=YELLOW)

        def _worker():
            try:
                run_all_benchmarks(
                    scale,
                    progress_callback=lambda msg: self.msg_queue.put(("bench", msg)),
                )
                self.msg_queue.put(("bench_done", f"✔ Benchmark zakończony – wyniki w results.csv"))
            except Exception as e:
                self.msg_queue.put(("error", f"Błąd benchmarku: {e}"))
                self.msg_queue.put(("bench_done", "Benchmark przerwany z błędem."))

        threading.Thread(target=_worker, daemon=True).start()

    # ─── 4. Wizualizacja ─────────────────────────────────────────────────────

    def _show_charts(self):
        if not os.path.isfile(RESULTS_FILE):
            messagebox.showinfo("Brak danych", "Plik results.csv nie istnieje.\nUruchom benchmark.")
            return

        df = pd.read_csv(RESULTS_FILE)
        df = df.dropna(subset=["Average_Time_Seconds"])

        if df.empty:
            messagebox.showinfo("Brak danych", "results.csv jest pusty lub nie zawiera wyników.")
            return

        # Nowe okno
        win = tk.Toplevel(self.root)
        win.title("Wyniki Benchmarku")
        win.configure(bg="#181825")
        win.geometry("1200x800")

        op_types = ["CREATE", "READ", "UPDATE", "DELETE"]
        db_colors = {
            "PostgreSQL": "#89b4fa",
            "MySQL": "#f9e2af",
            "MongoDB": "#a6e3a1",
            "Redis": "#f38ba8",
        }

        fig, axes = plt.subplots(2, 2, figsize=(14, 9))
        fig.patch.set_facecolor("#181825")
        fig.suptitle("Porównanie wydajności baz danych", fontsize=15,
                     fontweight="bold", color="#cdd6f4", y=0.98)

        for idx, op in enumerate(op_types):
            ax = axes[idx // 2][idx % 2]
            ax.set_facecolor("#1e1e2e")
            subset = df[df["Operation_Type"] == op]
            if subset.empty:
                ax.set_title(op, color="#cdd6f4", fontsize=12, fontweight="bold")
                ax.text(0.5, 0.5, "Brak danych", transform=ax.transAxes,
                        ha="center", va="center", color="#6c7086", fontsize=11)
                continue

            scenarios = subset["Scenario_Name"].unique()
            databases = subset["Database"].unique()
            x = range(len(scenarios))
            width = 0.18
            offsets = [i - (len(databases) - 1) / 2 for i in range(len(databases))]

            for i, db in enumerate(databases):
                db_data = subset[subset["Database"] == db]
                vals = [
                    db_data[db_data["Scenario_Name"] == s]["Average_Time_Seconds"].values
                    for s in scenarios
                ]
                vals = [v[0] if len(v) > 0 else 0 for v in vals]
                color = db_colors.get(db, "#cdd6f4")
                ax.bar(
                    [xi + offsets[i] * width for xi in x],
                    vals, width, label=db, color=color, alpha=0.88,
                    edgecolor="#11111b", linewidth=0.5,
                )

            ax.set_title(op, color="#cdd6f4", fontsize=12, fontweight="bold")
            ax.set_xticks(list(x))
            short_labels = [s.replace("_", "\n") for s in scenarios]
            ax.set_xticklabels(short_labels, fontsize=7, color="#a6adc8", rotation=30, ha="right")
            ax.set_ylabel("Czas (s)", color="#a6adc8", fontsize=9)
            ax.tick_params(colors="#6c7086", labelsize=8)
            ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.4f"))
            ax.legend(fontsize=7, facecolor="#2a2a3d", edgecolor="#45475a",
                      labelcolor="#cdd6f4", loc="upper left")
            for spine in ax.spines.values():
                spine.set_color("#45475a")

        fig.tight_layout(rect=[0, 0, 1, 0.95])

        canvas = FigureCanvasTkAgg(fig, master=win)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  Punkt wejścia
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    root = tk.Tk()
    app = BenchmarkApp(root)
    root.mainloop()
