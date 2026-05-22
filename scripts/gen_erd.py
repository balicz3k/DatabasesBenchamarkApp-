#!/usr/bin/env python3
"""
Generate a clean, professional ERD diagram for the hospital database schema.
Output: erd_diagram.png (300 DPI, suitable for presentation)
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
from matplotlib.path import Path
import matplotlib.patheffects as pe
import os

# ── Colour palette ────────────────────────────────────────────────────────────
C = {
    'bg':        '#F8F9FA',
    'hdr_dict':  '#1565C0',   # dictionary tables: rich blue
    'hdr_main':  '#2E7D32',   # main entities: rich green
    'hdr_vis':   '#4A148C',   # visits hub: deep purple
    'hdr_trans': '#BF360C',   # transactional: deep orange-red
    'row_pk':    '#FFFDE7',   # PK row: warm yellow
    'row_fk':    '#E3F2FD',   # FK row: light blue
    'row_norm':  '#FFFFFF',   # normal row: white
    'grid':      '#E0E7EF',   # background grid
    'border':    '#263238',   # table borders
    'divider':   '#CFD8DC',   # row dividers
    'lbl':       '#212121',   # label text
    'pk_txt':    '#E65100',   # PK text color
    'fk_txt':    '#01579B',   # FK text color
    'type_txt':  '#78909C',   # type annotation
    'line_std':  '#546E7A',   # standard FK line
    'line_cas':  '#C62828',   # ON DELETE CASCADE line
    'shadow':    '#B0BEC5',   # box shadow
}

HEADER_H = 0.60   # height of table header bar
ROW_H    = 0.46   # height of each attribute row
COL_W    = 5.40   # uniform table width

CATEGORY_HDR = {
    'dict':   C['hdr_dict'],
    'main':   C['hdr_main'],
    'visits': C['hdr_vis'],
    'trans':  C['hdr_trans'],
}

# ── Schema definition ─────────────────────────────────────────────────────────
# (category, [(col_name, type_str, 'PK'|'FK'|'')])
TABLES = {
    'departments':       ('dict',  [
        ('id',          'INT',          'PK'),
        ('name',        'VARCHAR(100)', ''),
        ('phone',       'VARCHAR(20)',  ''),
    ]),
    'specializations':   ('dict',  [
        ('id',          'INT',          'PK'),
        ('name',        'VARCHAR(100)', ''),
    ]),
    'diseases':          ('dict',  [
        ('id',          'INT',          'PK'),
        ('icd10_code',  'VARCHAR(10)',  ''),
        ('name',        'VARCHAR(100)', ''),
    ]),
    'medical_services':  ('dict',  [
        ('id',          'INT',          'PK'),
        ('name',        'VARCHAR(100)', ''),
        ('base_price',  'DECIMAL(10,2)',''),
    ]),
    'medications':       ('dict',  [
        ('id',          'INT',          'PK'),
        ('name',        'VARCHAR(100)', ''),
        ('active_substance', 'VARCHAR(100)', ''),
    ]),
    'patients':          ('main',  [
        ('id',          'INT',          'PK'),
        ('national_id', 'VARCHAR(20)',  ''),
        ('first_name',  'VARCHAR(50)',  ''),
        ('last_name',   'VARCHAR(50)',  ''),
        ('birth_date',  'DATE',         ''),
        ('gender',      'CHAR(1)',       ''),
    ]),
    'doctors':           ('main',  [
        ('id',            'INT',        'PK'),
        ('department_id', 'INT',        'FK'),
        ('specialization_id', 'INT',    'FK'),
        ('first_name',    'VARCHAR(50)',''),
        ('last_name',     'VARCHAR(50)',''),
        ('license_number','VARCHAR(20)',''),
    ]),
    'visits':            ('visits', [
        ('id',          'INT',          'PK'),
        ('patient_id',  'INT',          'FK'),
        ('doctor_id',   'INT',          'FK'),
        ('visit_date',  'DATE',         ''),
        ('status',      'VARCHAR(20)',  ''),
    ]),
    'performed_services': ('trans', [
        ('id',          'INT',          'PK'),
        ('visit_id',    'INT',          'FK'),
        ('service_id',  'INT',          'FK'),
        ('quantity',    'INT',          ''),
        ('final_price', 'DECIMAL(10,2)',''),
    ]),
    'diagnoses':         ('trans',  [
        ('id',            'INT',        'PK'),
        ('visit_id',      'INT',        'FK'),
        ('disease_id',    'INT',        'FK'),
        ('diagnosis_type','VARCHAR(20)',''),
        ('notes',         'TEXT',       ''),
    ]),
    'prescriptions':     ('trans',  [
        ('id',              'INT',        'PK'),
        ('visit_id',        'INT',        'FK'),
        ('prescription_code','VARCHAR(30)',''),
        ('issue_date',      'DATE',       ''),
    ]),
    'prescription_items': ('trans', [
        ('id',              'INT',        'PK'),
        ('prescription_id', 'INT',        'FK'),
        ('medication_id',   'INT',        'FK'),
        ('dosage',          'VARCHAR(50)',''),
    ]),
    'test_results':      ('trans',  [
        ('id',            'INT',         'PK'),
        ('visit_id',      'INT',         'FK'),
        ('parameter_name','VARCHAR(50)', ''),
        ('result_value',  'DECIMAL(10,2)',''),
        ('unit',          'VARCHAR(20)', ''),
        ('min_norm',      'DECIMAL(10,2)',''),
        ('max_norm',      'DECIMAL(10,2)',''),
    ]),
}

# FK relationships: (from_table, fk_column, to_table, on_delete_cascade)
RELATIONS = [
    ('doctors',           'department_id',     'departments',     False),
    ('doctors',           'specialization_id', 'specializations', False),
    ('visits',            'patient_id',        'patients',        False),
    ('visits',            'doctor_id',         'doctors',         False),
    ('performed_services','visit_id',          'visits',          True),
    ('performed_services','service_id',        'medical_services',False),
    ('diagnoses',         'visit_id',          'visits',          True),
    ('diagnoses',         'disease_id',        'diseases',        False),
    ('prescriptions',     'visit_id',          'visits',          True),
    ('prescription_items','prescription_id',   'prescriptions',   True),
    ('prescription_items','medication_id',     'medications',     False),
    ('test_results',      'visit_id',          'visits',          True),
]

# ── Layout ────────────────────────────────────────────────────────────────────
#
#  Col  0      1      2      3      4
#  x=  0.3    6.6   13.0   19.4   25.8
#
#  Row 0 (y=23.5): departments  specializations  diseases  medical_services  medications
#  Row 1 (y=17.5): patients     doctors
#  Row 2 (y=14.0):                               visits
#  Row 3 (y=14.0):                                         (test_results, right side)
#  Row 4 (y= 8.0): diagnoses    perf_services    prescriptions
#  Row 5 (y= 2.0):                               prescription_items

X_COLS = [0.3, 6.6, 13.0, 19.4, 25.8]
POS = {
    # Row 0 — Dictionary tables
    'departments':        (X_COLS[0], 23.5),
    'specializations':    (X_COLS[1], 23.8),   # raised slightly (fewer rows)
    'diseases':           (X_COLS[2], 23.5),
    'medical_services':   (X_COLS[3], 23.5),
    'medications':        (X_COLS[4], 23.5),
    # Row 1 — Main entities
    'patients':           (X_COLS[0], 17.8),
    'doctors':            (X_COLS[1], 17.8),
    # Row 2 — Hub
    'visits':             (X_COLS[2], 14.5),
    # Row 2b — Test results (right column, same vertical band as visits)
    'test_results':       (X_COLS[3], 14.0),
    # Row 3 — Transactional children
    'diagnoses':          (X_COLS[0],  8.0),
    'performed_services': (X_COLS[1],  8.0),
    'prescriptions':      (X_COLS[2],  8.0),
    # Row 4 — Sub-transactional
    'prescription_items': (X_COLS[3],  2.5),
}


def table_height(tname):
    _, cols = TABLES[tname]
    return HEADER_H + len(cols) * ROW_H


def col_row_y(tname, col_name):
    """Return y-center of the row for col_name in tname."""
    _, cols = TABLES[tname]
    x, top = POS[tname]
    for i, (cname, _, _) in enumerate(cols):
        if cname == col_name:
            return top - HEADER_H - (i + 0.5) * ROW_H
    return top - table_height(tname) / 2


def edge_x(tname, side):
    """Return left or right x-edge of a table."""
    x, _ = POS[tname]
    return x if side == 'left' else x + COL_W


def draw_table(ax, tname):
    cat, cols = TABLES[tname]
    x, top = POS[tname]
    h = table_height(tname)
    hdr_color = CATEGORY_HDR[cat]

    # Drop shadow
    ax.add_patch(FancyBboxPatch(
        (x + 0.07, top - h - 0.07), COL_W, h,
        boxstyle='round,pad=0.06',
        facecolor=C['shadow'], edgecolor='none', alpha=0.45, zorder=1,
    ))

    # Header
    ax.add_patch(FancyBboxPatch(
        (x, top - HEADER_H), COL_W, HEADER_H,
        boxstyle='round,pad=0.0',
        facecolor=hdr_color, edgecolor='none', zorder=2,
    ))
    ax.text(x + COL_W / 2, top - HEADER_H / 2, tname,
            ha='center', va='center',
            fontsize=8.5, fontweight='bold', color='white', zorder=4,
            fontfamily='monospace')

    # Attribute rows
    for i, (cname, ctype, ckey) in enumerate(cols):
        ry = top - HEADER_H - (i + 1) * ROW_H
        row_bg = (C['row_pk'] if ckey == 'PK'
                  else C['row_fk'] if ckey == 'FK'
                  else C['row_norm'])
        ax.add_patch(mpatches.Rectangle(
            (x, ry), COL_W, ROW_H,
            facecolor=row_bg, edgecolor='none', zorder=2,
        ))
        # Row divider
        ax.plot([x, x + COL_W], [ry + ROW_H, ry + ROW_H],
                color=C['divider'], lw=0.6, zorder=3)

        # PK / FK badge
        if ckey == 'PK':
            badge_txt, badge_fg = 'PK', C['pk_txt']
            ax.add_patch(FancyBboxPatch(
                (x + 0.10, ry + 0.08), 0.50, ROW_H - 0.16,
                boxstyle='round,pad=0.02',
                facecolor='#FFF8E1', edgecolor=C['pk_txt'], lw=0.6, zorder=3,
            ))
            ax.text(x + 0.35, ry + ROW_H / 2, 'PK',
                    ha='center', va='center',
                    fontsize=5.5, fontweight='bold', color=C['pk_txt'], zorder=4)
            col_x = x + 0.72
        elif ckey == 'FK':
            ax.add_patch(FancyBboxPatch(
                (x + 0.10, ry + 0.08), 0.50, ROW_H - 0.16,
                boxstyle='round,pad=0.02',
                facecolor='#E1F5FE', edgecolor=C['fk_txt'], lw=0.6, zorder=3,
            ))
            ax.text(x + 0.35, ry + ROW_H / 2, 'FK',
                    ha='center', va='center',
                    fontsize=5.5, fontweight='bold', color=C['fk_txt'], zorder=4)
            col_x = x + 0.72
        else:
            col_x = x + 0.22

        txt_color = (C['pk_txt'] if ckey == 'PK'
                     else C['fk_txt'] if ckey == 'FK'
                     else C['lbl'])
        ax.text(col_x, ry + ROW_H / 2, cname,
                ha='left', va='center',
                fontsize=7.0, fontweight='bold' if ckey else 'normal',
                color=txt_color, zorder=4, fontfamily='monospace')
        ax.text(x + COL_W - 0.12, ry + ROW_H / 2, ctype,
                ha='right', va='center',
                fontsize=5.8, style='italic',
                color=C['type_txt'], zorder=4)

    # Outer border (rounded)
    ax.add_patch(FancyBboxPatch(
        (x, top - h), COL_W, h,
        boxstyle='round,pad=0.03',
        facecolor='none', edgecolor=C['border'], linewidth=1.1, zorder=5,
    ))


def draw_relation(ax, from_t, fk_col, to_t, cascade):
    """
    Draw an elbow connector from fk_col row of from_t to the 'id' row of to_t.
    Uses orthogonal routing to stay clean.
    """
    color = C['line_cas'] if cascade else C['line_std']
    lw = 1.3 if cascade else 1.1
    alpha = 0.85

    fx, fy_t = POS[from_t]
    tx, ty_t = POS[to_t]
    f_center = fx + COL_W / 2
    t_center = tx + COL_W / 2

    y_from = col_row_y(from_t, fk_col)
    y_to   = col_row_y(to_t, 'id')

    # Decide exit side: FK table exits on the side closer to PK table
    if f_center <= t_center:
        x_from = edge_x(from_t, 'right')
        x_to   = edge_x(to_t,   'left')
    else:
        x_from = edge_x(from_t, 'left')
        x_to   = edge_x(to_t,   'right')

    # Routing: two-segment elbow
    # horizontal gap between tables
    x_mid = (x_from + x_to) / 2.0

    # If tables are in the same column (x close), route via side offset
    if abs(x_from - x_to) < 0.5:
        # Same-side, go around
        offset = -1.0 if f_center < 15 else 1.0
        verts = [
            (x_from, y_from),
            (x_from + offset * 1.0, y_from),
            (x_from + offset * 1.0, y_to),
            (x_to,   y_to),
        ]
    else:
        verts = [
            (x_from, y_from),
            (x_mid,  y_from),
            (x_mid,  y_to),
            (x_to,   y_to),
        ]

    codes = [Path.MOVETO] + [Path.LINETO] * (len(verts) - 1)
    path = Path(verts, codes)
    patch = mpatches.PathPatch(
        path, facecolor='none', edgecolor=color,
        lw=lw, alpha=alpha, zorder=1,
        capstyle='round', joinstyle='round',
    )
    ax.add_patch(patch)

    # Arrow head at destination
    dx = x_to - verts[-2][0]
    dy = y_to - verts[-2][1]
    if abs(dx) > abs(dy):
        ddx, ddy = (0.06 if dx > 0 else -0.06), 0.0
    else:
        ddx, ddy = 0.0, (0.06 if dy > 0 else -0.06)
    ax.annotate('', xy=(x_to, y_to),
                xytext=(x_to - ddx * 8, y_to - ddy * 8),
                arrowprops=dict(arrowstyle='-|>', color=color,
                                lw=lw, mutation_scale=10),
                zorder=2)


def main():
    FIG_W, FIG_H = 36, 26
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H), dpi=72)
    ax.set_xlim(-0.3, 31.5)
    ax.set_ylim(-0.8, 25.5)
    ax.set_aspect('equal')
    ax.axis('off')
    fig.patch.set_facecolor(C['bg'])
    ax.set_facecolor(C['bg'])

    # Subtle background tiling
    ax.add_patch(mpatches.Rectangle((-0.3, -0.8), 32, 27,
                                    facecolor=C['grid'], edgecolor='none', zorder=0))

    # White content panel
    ax.add_patch(FancyBboxPatch((-0.1, -0.6), 31.7, 26.5,
                                boxstyle='round,pad=0.1',
                                facecolor='#FFFFFF', edgecolor='#B0BEC5',
                                lw=1.5, zorder=0, alpha=0.6))

    # ── Relations (drawn first, behind tables) ─────────────────────────────
    for from_t, fk_col, to_t, cascade in RELATIONS:
        draw_relation(ax, from_t, fk_col, to_t, cascade)

    # ── Tables ─────────────────────────────────────────────────────────────
    for tname in TABLES:
        draw_table(ax, tname)

    # ── Title ──────────────────────────────────────────────────────────────
    ax.text(15.6, 25.1,
            'Schemat relacyjny – System Informatyczny Szpitala',
            ha='center', va='center',
            fontsize=14, fontweight='bold', color=C['border'],
            zorder=6)
    ax.text(15.6, 24.65,
            '13 tabel  ·  12 relacji klucz obcy  ·  7 kaskadowych usunięć',
            ha='center', va='center',
            fontsize=8.5, color='#546E7A', style='italic', zorder=6)

    # ── Legend ─────────────────────────────────────────────────────────────
    legend_items_cat = [
        ('Tabele słownikowe',  C['hdr_dict']),
        ('Encje główne',       C['hdr_main']),
        ('Wizyty (centrum)',   C['hdr_vis']),
        ('Tabele transakcyjne', C['hdr_trans']),
    ]
    legend_items_key = [
        ('Klucz główny (PK)', C['row_pk'],  C['pk_txt']),
        ('Klucz obcy (FK)',   C['row_fk'],  C['fk_txt']),
    ]
    legend_items_line = [
        ('Relacja FK',               C['line_std'], '-'),
        ('FK + ON DELETE CASCADE',   C['line_cas'], '--'),
    ]

    ly = -0.35
    lx = 0.2
    for label, color in legend_items_cat:
        ax.add_patch(FancyBboxPatch((lx, ly - 0.18), 0.45, 0.36,
                                    boxstyle='round,pad=0.03',
                                    facecolor=color, edgecolor='none', zorder=6))
        ax.text(lx + 0.58, ly, label,
                va='center', fontsize=7.0, color=C['lbl'], zorder=6)
        lx += 5.5

    lx = 23.0
    for label, bg, fg in legend_items_key:
        ax.add_patch(FancyBboxPatch((lx, ly - 0.18), 0.45, 0.36,
                                    boxstyle='round,pad=0.03',
                                    facecolor=bg, edgecolor=fg, lw=0.8, zorder=6))
        ax.text(lx + 0.58, ly, label,
                va='center', fontsize=7.0, color=C['lbl'], zorder=6)
        lx += 4.8

    # ── Save ───────────────────────────────────────────────────────────────
    out_dir = os.path.join(os.path.dirname(__file__), '..', 'presentation_assets')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, 'erd_diagram.png')

    plt.savefig(out_path, dpi=250, bbox_inches='tight',
                facecolor=C['bg'], edgecolor='none')
    print(f'ERD saved → {os.path.abspath(out_path)}')
    plt.close()


if __name__ == '__main__':
    main()
