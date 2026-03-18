import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import os
import pathlib

# ─────────────────────────────────────────────
# CONFIGURACIÓN GLOBAL DE ESTILO
# ─────────────────────────────────────────────
plt.rcParams.update({
    'figure.facecolor': '#0F1117',
    'axes.facecolor':   '#1A1D27',
    'axes.edgecolor':   '#2E3250',
    'axes.labelcolor':  '#C8CCDF',
    'xtick.color':      '#8A8FA8',
    'ytick.color':      '#8A8FA8',
    'text.color':       '#E2E5F0',
    'grid.color':       '#2E3250',
    'grid.linestyle':   '--',
    'grid.alpha':       0.6,
    'font.family':      'DejaVu Sans',
    'font.size':        10,
})

# Paleta de colores consistente
COLOR_PRINCIPAL  = '#FF4C6A'   # rojo alarma (fraude)
COLOR_ACENTO     = '#F7B731'   # dorado (resaltar)
COLOR_SEGURO     = '#26D0A0'   # verde (comparación legítima)
COLOR_FONDO_BAR  = '#2E3250'
GRADIENTE = ['#FF4C6A', '#FF7A5A', '#FFA04A', '#F7B731', '#D4E157',
             '#80E27E', '#26D0A0', '#00BCD4', '#7986CB', '#CE93D8']

# ─────────────────────────────────────────────
# LECTURA Y LIMPIEZA
# ─────────────────────────────────────────────
BASE_DIR = pathlib.Path(__file__).resolve().parent
CSV_FILE = str(BASE_DIR / "dataset_eventos_50000_v2_sucio.csv")
print(f"Leyendo dataset: {CSV_FILE}...")
df = pd.read_csv(CSV_FILE, encoding="utf-8-sig")

def limpiar_numero(val):
    if pd.isna(val): return 0.0
    val_str = str(val).replace('$', '').replace(' ', '')
    if ',' in val_str and '.' in val_str:
        if val_str.rfind(',') > val_str.rfind('.'):
            val_str = val_str.replace('.', '').replace(',', '.')
        else:
            val_str = val_str.replace(',', '')
    elif ',' in val_str:
        val_str = val_str.replace(',', '.')
    try:
        return float(val_str)
    except:
        return 0.0

def limpiar_texto(val):
    if pd.isna(val): return "Desconocido"
    return str(val).strip().title()

def preparar_datos_limpios(df):
    df['monto_transaccion'] = df['monto_transaccion'].apply(limpiar_numero)
    df['es_fraude'] = df['es_fraude'].apply(
        lambda x: 1 if str(x).lower() in ['1','true','si','yes'] else 0)
    for col in ['ciudad', 'categoria_tarjeta', 'nombre_comercio']:
        if col in df.columns:
            df[col] = df[col].apply(limpiar_texto)
    return df

df = preparar_datos_limpios(df)
df['fecha_transaccion'] = pd.to_datetime(df['fecha_transaccion'], errors='coerce')
df['mes']       = df['fecha_transaccion'].dt.to_period('M')
df['trimestre'] = df['fecha_transaccion'].dt.to_period('Q')

fraudes    = df[df['es_fraude'] == 1]
legitimas  = df[df['es_fraude'] == 0]

total_tx         = len(df)
total_fraudes    = len(fraudes)
tasa_fraude      = total_fraudes / total_tx * 100
monto_total_fraud = fraudes['monto_transaccion'].sum()

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def fmt_millones(x, pos=None):
    if x >= 1_000_000:
        return f'${x/1_000_000:.1f}M'
    elif x >= 1_000:
        return f'${x/1_000:.0f}K'
    return f'${x:.0f}'

def agregar_etiquetas_barras(ax, serie, fmt='$', color='white', fontsize=8.5):
    """Escribe el valor sobre cada barra."""
    for i, v in enumerate(serie):
        if fmt == '$':
            label = fmt_millones(v)
        else:
            label = f'{v:,}'
        ax.text(i, v * 1.015, label,
                ha='center', va='bottom',
                color=color, fontsize=fontsize, fontweight='bold')

def titulo_con_subtitulo(fig, titulo, subtitulo, y_titulo=0.97):
    fig.text(0.5, y_titulo,   titulo,    ha='center', fontsize=15,
             fontweight='bold', color='#E2E5F0')
    fig.text(0.5, y_titulo - 0.035, subtitulo, ha='center',
             fontsize=9.5, color='#8A8FA8', style='italic')


# ══════════════════════════════════════════════════════════════
# GRÁFICA 1 — Top 10 ciudades por monto fraudulento
# ══════════════════════════════════════════════════════════════
ciudad_monto  = (fraudes.groupby('ciudad')['monto_transaccion']
                 .sum().sort_values(ascending=False).head(10))
ciudad_conteo = (fraudes.groupby('ciudad').size()
                 .reindex(ciudad_monto.index))
n_ciudades = len(ciudad_monto)   # ← dinámico: puede ser < 10

fig, axes = plt.subplots(1, 2, figsize=(16, 6), facecolor='#0F1117')
titulo_con_subtitulo(fig,
    f'🏙️  Top {n_ciudades} Ciudades por Actividad Fraudulenta',
    f'Monto total analizado: {fmt_millones(monto_total_fraud)}  |  '
    f'Total fraudes: {total_fraudes:,}  |  Tasa: {tasa_fraude:.1f}%')

# — Panel izquierdo: monto —
ax1 = axes[0]
colores_barra = GRADIENTE[:n_ciudades]
barras = ax1.bar(range(n_ciudades), ciudad_monto.values, color=colores_barra,
                 edgecolor='#0F1117', linewidth=0.8, zorder=3)
ax1.set_title('Monto Total Defraudado (USD)', color='#E2E5F0',
              fontsize=11, fontweight='bold', pad=10)
ax1.set_xticks(range(n_ciudades))
ax1.set_xticklabels(ciudad_monto.index, rotation=40, ha='right', fontsize=8.5)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_millones))
ax1.set_ylabel('Monto (USD)', labelpad=8)
ax1.grid(axis='y', zorder=0)
ax1.set_axisbelow(True)
agregar_etiquetas_barras(ax1, ciudad_monto.values, fmt='$')
# Resaltar la #1
barras[0].set_edgecolor(COLOR_ACENTO)
barras[0].set_linewidth(2.5)
offset_flecha = min(1, n_ciudades - 1)   # evita salirse si hay pocas ciudades
ax1.annotate('  ⚠ Mayor\n  riesgo',
             xy=(0, ciudad_monto.values[0]),
             xytext=(offset_flecha, ciudad_monto.values[0] * 0.92),
             arrowprops=dict(arrowstyle='->', color=COLOR_ACENTO, lw=1.5),
             color=COLOR_ACENTO, fontsize=8.5, fontweight='bold')

# — Panel derecho: número de fraudes —
ax2 = axes[1]
ax2.barh(range(n_ciudades)[::-1], ciudad_conteo.values,
         color=[COLOR_PRINCIPAL if i == 0 else COLOR_FONDO_BAR
                for i in range(n_ciudades)],
         edgecolor='#0F1117', linewidth=0.6, zorder=3)
ax2.set_yticks(range(n_ciudades)[::-1])
ax2.set_yticklabels(ciudad_monto.index, fontsize=8.5)
ax2.set_title('Número de Transacciones Fraudulentas', color='#E2E5F0',
              fontsize=11, fontweight='bold', pad=10)
ax2.set_xlabel('Cantidad de fraudes', labelpad=8)
ax2.grid(axis='x', zorder=0)
ax2.set_axisbelow(True)
for i, v in enumerate(ciudad_conteo.values):
    ax2.text(v + ciudad_conteo.values.max() * 0.01, (n_ciudades - 1) - i,
             f'{v:,}', va='center', color='#E2E5F0', fontsize=8.5)

plt.tight_layout(rect=[0, 0, 1, 0.93])
plt.savefig('grafica1_ciudades.png', dpi=150, bbox_inches='tight',
            facecolor='#0F1117')
plt.show()


# ══════════════════════════════════════════════════════════════
# GRÁFICA 2 — Evolución trimestral
# ══════════════════════════════════════════════════════════════
fraudes_trim = fraudes.groupby('trimestre').size()
legit_trim   = legitimas.groupby('trimestre').size().reindex(
                fraudes_trim.index, fill_value=0)
monto_trim   = fraudes.groupby('trimestre')['monto_transaccion'].sum()
etiquetas_x  = fraudes_trim.index.astype(str)
x_pos        = range(len(etiquetas_x))

fig, axes = plt.subplots(2, 1, figsize=(14, 9), facecolor='#0F1117',
                          sharex=True, gridspec_kw={'hspace': 0.35})
titulo_con_subtitulo(fig,
    '📈  Evolución Trimestral del Fraude',
    'Comparativa fraudes vs. transacciones legítimas — monto acumulado por período')

# — Panel superior: conteo doble línea —
ax1 = axes[0]
ax1.fill_between(x_pos, fraudes_trim.values, alpha=0.15, color=COLOR_PRINCIPAL)
ax1.plot(x_pos, fraudes_trim.values, marker='o', color=COLOR_PRINCIPAL,
         linewidth=2.2, markersize=7, label='Fraudes', zorder=4)
ax1.plot(x_pos, legit_trim.values,   marker='s', color=COLOR_SEGURO,
         linewidth=2.2, markersize=7, label='Legítimas', zorder=4, alpha=0.75)
# Anotar máximo de fraudes
idx_max = fraudes_trim.values.argmax()
ax1.annotate(f'  Pico: {fraudes_trim.values[idx_max]:,}',
             xy=(idx_max, fraudes_trim.values[idx_max]),
             xytext=(idx_max + 0.4, fraudes_trim.values[idx_max] * 1.05),
             color=COLOR_ACENTO, fontsize=8.5, fontweight='bold',
             arrowprops=dict(arrowstyle='->', color=COLOR_ACENTO, lw=1.3))
for i, v in enumerate(fraudes_trim.values):
    ax1.text(i, v + fraudes_trim.values.max() * 0.02, str(v),
             ha='center', color=COLOR_PRINCIPAL, fontsize=8, fontweight='bold')
ax1.set_title('Cantidad de Transacciones por Trimestre', color='#E2E5F0',
              fontsize=11, fontweight='bold', pad=8)
ax1.set_ylabel('N° de transacciones', labelpad=8)
ax1.legend(loc='upper left', framealpha=0.3, fontsize=9)
ax1.grid(axis='y')

# — Panel inferior: monto fraudulento —
ax2 = axes[1]
colores_trim = [COLOR_PRINCIPAL if v == monto_trim.values.max()
                else COLOR_FONDO_BAR for v in monto_trim.values]
bars = ax2.bar(x_pos, monto_trim.values, color=colores_trim,
               edgecolor='#0F1117', linewidth=0.7, zorder=3)
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_millones))
agregar_etiquetas_barras(ax2, monto_trim.values, fmt='$')
ax2.set_title('Monto Total Defraudado por Trimestre (USD)', color='#E2E5F0',
              fontsize=11, fontweight='bold', pad=8)
ax2.set_ylabel('Monto (USD)', labelpad=8)
ax2.set_xticks(x_pos)
ax2.set_xticklabels(etiquetas_x, rotation=35, ha='right', fontsize=9)
ax2.set_xlabel('Trimestre', labelpad=8)
ax2.grid(axis='y', zorder=0)
ax2.set_axisbelow(True)

plt.savefig('grafica2_trimestral.png', dpi=150, bbox_inches='tight',
            facecolor='#0F1117')
plt.show()


# ══════════════════════════════════════════════════════════════
# GRÁFICA 3 — Fraude por tipo de tarjeta
# ══════════════════════════════════════════════════════════════
fraude_tarjeta = fraudes.groupby('categoria_tarjeta')['monto_transaccion'].sum()
fraude_conteo  = fraudes.groupby('categoria_tarjeta').size()
total_monto_t  = fraude_tarjeta.sum()

fig, axes = plt.subplots(1, 3, figsize=(18, 6), facecolor='#0F1117')
titulo_con_subtitulo(fig,
    '💳  Análisis de Fraude por Tipo de Tarjeta',
    'Distribución de monto, volumen y ticket promedio por categoría')

colores_pie = GRADIENTE[:len(fraude_tarjeta)]

# — Donut: monto —
ax1 = axes[0]
wedges, texts, autotexts = ax1.pie(
    fraude_tarjeta.values,
    labels=None,
    autopct='%1.1f%%',
    colors=colores_pie,
    startangle=140,
    pctdistance=0.78,
    wedgeprops=dict(width=0.6, edgecolor='#0F1117', linewidth=2))
for at in autotexts:
    at.set_color('white'); at.set_fontsize(9); at.set_fontweight('bold')
ax1.text(0, 0, fmt_millones(total_monto_t), ha='center', va='center',
         color='white', fontsize=12, fontweight='bold')
ax1.text(0, -0.22, 'total fraude', ha='center', va='center',
         color='#8A8FA8', fontsize=8)
ax1.set_title('Distribución de Monto\nDefraudado', color='#E2E5F0',
              fontsize=11, fontweight='bold')
ax1.legend(wedges, fraude_tarjeta.index, loc='lower center',
           bbox_to_anchor=(0.5, -0.18), ncol=2, fontsize=8,
           framealpha=0.2)

# — Barras: número de fraudes —
ax2 = axes[1]
ax2.bar(fraude_conteo.index, fraude_conteo.values,
        color=colores_pie, edgecolor='#0F1117', linewidth=0.8, zorder=3)
for i, v in enumerate(fraude_conteo.values):
    ax2.text(i, v + fraude_conteo.values.max() * 0.01,
             f'{v:,}', ha='center', color='white',
             fontsize=9, fontweight='bold')
ax2.set_title('Número de\nTransacciones Fraudulentas', color='#E2E5F0',
              fontsize=11, fontweight='bold')
ax2.set_ylabel('Cantidad', labelpad=8)
ax2.set_xticklabels(fraude_conteo.index, rotation=30, ha='right', fontsize=8.5)
ax2.grid(axis='y', zorder=0); ax2.set_axisbelow(True)

# — Barras horizontales: ticket promedio —
ax3 = axes[2]
ticket_prom = fraude_tarjeta / fraude_conteo
ticket_ord  = ticket_prom.sort_values(ascending=True)
ax3.barh(range(len(ticket_ord)), ticket_ord.values,
         color=[colores_pie[list(fraude_tarjeta.index).index(c)]
                for c in ticket_ord.index],
         edgecolor='#0F1117', linewidth=0.8, zorder=3)
ax3.set_yticks(range(len(ticket_ord)))
ax3.set_yticklabels(ticket_ord.index, fontsize=9)
for i, v in enumerate(ticket_ord.values):
    ax3.text(v + ticket_ord.values.max() * 0.01, i,
             fmt_millones(v), va='center', color='white', fontsize=9)
ax3.set_title('Monto Promedio\nPor Transacción Fraudulenta', color='#E2E5F0',
              fontsize=11, fontweight='bold')
ax3.set_xlabel('Promedio USD', labelpad=8)
ax3.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_millones))
ax3.grid(axis='x', zorder=0); ax3.set_axisbelow(True)

plt.tight_layout(rect=[0, 0, 1, 0.93])
plt.savefig('grafica3_tarjetas.png', dpi=150, bbox_inches='tight',
            facecolor='#0F1117')
plt.show()


# ══════════════════════════════════════════════════════════════
# GRÁFICA 4 — Top 5 comercios con más fraude
# ══════════════════════════════════════════════════════════════
fraude_com_cnt = (fraudes.groupby('nombre_comercio').size()
                  .sort_values(ascending=False).head(5))
fraude_com_mnt = (fraudes.groupby('nombre_comercio')['monto_transaccion']
                  .sum().reindex(fraude_com_cnt.index))
fraude_com_avg = fraude_com_mnt / fraude_com_cnt

fig, axes = plt.subplots(1, 2, figsize=(15, 6), facecolor='#0F1117')
titulo_con_subtitulo(fig,
    '🏪  Top 5 Comercios con Mayor Actividad Fraudulenta',
    'Número de incidentes vs. monto total — tamaño del problema por establecimiento')

colores_com = [COLOR_PRINCIPAL] + [COLOR_FONDO_BAR] * 4

# — Panel izquierdo: conteo con línea de promedio —
ax1 = axes[0]
bars1 = ax1.bar(range(5), fraude_com_cnt.values,
                color=colores_com, edgecolor='#0F1117', linewidth=0.8, zorder=3)
prom_cnt = fraude_com_cnt.values.mean()
ax1.axhline(prom_cnt, color=COLOR_ACENTO, linestyle='--', linewidth=1.5,
            label=f'Promedio: {prom_cnt:.0f}', zorder=5)
for i, v in enumerate(fraude_com_cnt.values):
    ax1.text(i, v + fraude_com_cnt.values.max() * 0.01,
             f'{v:,}', ha='center', color='white', fontsize=9.5, fontweight='bold')
ax1.set_title('Número de Transacciones Fraudulentas', color='#E2E5F0',
              fontsize=11, fontweight='bold', pad=10)
ax1.set_xticks(range(5))
ax1.set_xticklabels(fraude_com_cnt.index, rotation=30, ha='right', fontsize=9)
ax1.set_ylabel('Cantidad de fraudes', labelpad=8)
ax1.legend(framealpha=0.3, fontsize=9)
ax1.grid(axis='y', zorder=0); ax1.set_axisbelow(True)

# — Panel derecho: monto + promedio como scatter —
ax2 = axes[1]
bars2 = ax2.bar(range(5), fraude_com_mnt.values,
                color=colores_com, edgecolor='#0F1117', linewidth=0.8, zorder=3,
                label='Monto total')
ax2_twin = ax2.twinx()
ax2_twin.set_facecolor('#1A1D27')
ax2_twin.plot(range(5), fraude_com_avg.values, color=COLOR_ACENTO,
              marker='D', markersize=9, linewidth=2, zorder=6,
              label='Promedio por fraude')
ax2_twin.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_millones))
ax2_twin.set_ylabel('Promedio por transacción (USD)',
                    color=COLOR_ACENTO, labelpad=8)
ax2_twin.tick_params(axis='y', colors=COLOR_ACENTO)

agregar_etiquetas_barras(ax2, fraude_com_mnt.values, fmt='$')
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_millones))
ax2.set_xticks(range(5))
ax2.set_xticklabels(fraude_com_mnt.index, rotation=30, ha='right', fontsize=9)
ax2.set_title('Monto Total Defraudado + Ticket Promedio', color='#E2E5F0',
              fontsize=11, fontweight='bold', pad=10)
ax2.set_ylabel('Monto total (USD)', labelpad=8)
ax2.grid(axis='y', zorder=0); ax2.set_axisbelow(True)

lines1, labs1 = ax2.get_legend_handles_labels()
lines2, labs2 = ax2_twin.get_legend_handles_labels()
ax2.legend(lines1 + lines2, labs1 + labs2, loc='upper right',
           framealpha=0.3, fontsize=9)

plt.tight_layout(rect=[0, 0, 1, 0.93])
plt.savefig('grafica4_comercios.png', dpi=150, bbox_inches='tight',
            facecolor='#0F1117')
plt.show()


# ══════════════════════════════════════════════════════════════
# GRÁFICA 5 — Monto fraudulento mensual + tendencia
# ══════════════════════════════════════════════════════════════
fraude_mes     = fraudes.groupby('mes')['monto_transaccion'].sum()
fraude_mes_cnt = fraudes.groupby('mes').size().reindex(fraude_mes.index)
etiquetas_mes  = fraude_mes.index.astype(str)
x_mes          = np.arange(len(etiquetas_mes))

# Línea de tendencia lineal
z   = np.polyfit(x_mes, fraude_mes.values, 1)
p   = np.poly1d(z)
tendencia = p(x_mes)
signo_tendencia = '▲ Tendencia alcista' if z[0] > 0 else '▼ Tendencia bajista'
color_tend = COLOR_PRINCIPAL if z[0] > 0 else COLOR_SEGURO

fig, axes = plt.subplots(2, 1, figsize=(15, 9), facecolor='#0F1117',
                          gridspec_kw={'height_ratios': [2.5, 1], 'hspace': 0.35})
titulo_con_subtitulo(fig,
    '📅  Evolución Mensual del Fraude',
    f'Monto acumulado y número de incidentes — {signo_tendencia}')

# — Panel principal: monto con área —
ax1 = axes[0]
ax1.fill_between(x_mes, fraude_mes.values, alpha=0.12, color=COLOR_PRINCIPAL)
ax1.plot(x_mes, fraude_mes.values, marker='s', color=COLOR_PRINCIPAL,
         linewidth=2, markersize=6, zorder=4, label='Monto mensual')
ax1.plot(x_mes, tendencia, '--', color=color_tend, linewidth=2, alpha=0.85,
         zorder=5, label=signo_tendencia)

# Anotaciones de máx/mín
idx_max = fraude_mes.values.argmax()
idx_min = fraude_mes.values.argmin()
for idx, etiq, col, offset in [
    (idx_max, f'Máx\n{fmt_millones(fraude_mes.values[idx_max])}',
     COLOR_ACENTO, 0.06),
    (idx_min, f'Mín\n{fmt_millones(fraude_mes.values[idx_min])}',
     COLOR_SEGURO, -0.08)]:
    ax1.annotate(etiq,
                 xy=(idx, fraude_mes.values[idx]),
                 xytext=(idx, fraude_mes.values[idx] *
                         (1 + offset + 0.04 * (1 if offset > 0 else -1))),
                 ha='center', color=col, fontsize=8.5, fontweight='bold',
                 arrowprops=dict(arrowstyle='->', color=col, lw=1.3))

ax1.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_millones))
ax1.set_ylabel('Monto defraudado (USD)', labelpad=8)
ax1.set_title('Monto Total Mensual de Fraudes', color='#E2E5F0',
              fontsize=11, fontweight='bold', pad=8)
ax1.legend(loc='upper left', framealpha=0.3, fontsize=9)
ax1.grid(axis='y')
ax1.set_xticks(x_mes)
ax1.set_xticklabels([])

# — Panel inferior: conteo mensual como barras —
ax2 = axes[1]
ax2.bar(x_mes, fraude_mes_cnt.values,
        color=[COLOR_PRINCIPAL if i in [idx_max, idx_min] else COLOR_FONDO_BAR
               for i in range(len(x_mes))],
        edgecolor='#0F1117', linewidth=0.6, zorder=3)
ax2.set_ylabel('N° de fraudes', labelpad=8)
ax2.set_title('Número de Incidentes por Mes', color='#E2E5F0',
              fontsize=11, fontweight='bold', pad=8)
ax2.set_xticks(x_mes)
ax2.set_xticklabels(etiquetas_mes, rotation=40, ha='right', fontsize=8)
ax2.set_xlabel('Mes', labelpad=8)
ax2.grid(axis='y', zorder=0); ax2.set_axisbelow(True)
for i, v in enumerate(fraude_mes_cnt.values):
    ax2.text(i, v + fraude_mes_cnt.values.max() * 0.01,
             str(v), ha='center', color='#C8CCDF', fontsize=7.5)

plt.savefig('grafica5_mensual.png', dpi=150, bbox_inches='tight',
            facecolor='#0F1117')
plt.show()

print("\n✅ Todas las gráficas generadas y guardadas correctamente.")