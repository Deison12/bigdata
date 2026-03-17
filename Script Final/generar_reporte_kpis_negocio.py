import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import pathlib

# Configuración
BASE_DIR = pathlib.Path(__file__).resolve().parent
CSV_FILE = str(BASE_DIR / "dataset_eventos_50000_v2_sucio.csv")

print(f"Leyendo dataset: {CSV_FILE}...")
df = pd.read_csv(CSV_FILE, encoding="utf-8-sig")

# --- Limpieza (igual que tu código) ---
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
    df['es_fraude'] = df['es_fraude'].apply(lambda x: 1 if str(x).lower() in ['1','true','si','yes'] else 0)

    for col in ['ciudad','categoria_tarjeta','nombre_comercio']:
        if col in df.columns:
            df[col] = df[col].apply(limpiar_texto)

    return df

df = preparar_datos_limpios(df)

# Fechas
df['fecha_transaccion'] = pd.to_datetime(df['fecha_transaccion'], errors='coerce')
df['mes'] = df['fecha_transaccion'].dt.to_period('M')
df['trimestre'] = df['fecha_transaccion'].dt.to_period('Q')

fraudes = df[df['es_fraude'] == 1]

# =========================================================
# 📊 1. Top ciudades por fraude
# =========================================================
ciudad_monto = fraudes.groupby('ciudad')['monto_transaccion'].sum().sort_values(ascending=False).head(10)

plt.figure()
plt.bar(ciudad_monto.index, ciudad_monto.values)
plt.title('Top 10 Ciudades por Monto Fraudulento')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# =========================================================
# 📊 2. Evolución trimestral
# =========================================================
fraudes_trim = fraudes.groupby('trimestre').size()

plt.figure()
plt.plot(fraudes_trim.index.astype(str), fraudes_trim.values, marker='o')
plt.title('Fraudes por Trimestre')
plt.xticks(rotation=45)
plt.grid()
plt.tight_layout()
plt.show()

# =========================================================
# 📊 3. Fraude por tipo de tarjeta
# =========================================================
fraude_tarjeta = fraudes.groupby('categoria_tarjeta')['monto_transaccion'].sum()

plt.figure()
plt.pie(fraude_tarjeta.values, labels=fraude_tarjeta.index, autopct='%1.1f%%')
plt.title('Fraude por Tipo de Tarjeta')
plt.tight_layout()
plt.show()

# =========================================================
# 📊 4. Comercios con más fraude
# =========================================================
fraude_com = fraudes.groupby('nombre_comercio').size().sort_values(ascending=False).head(5)

plt.figure()
plt.bar(fraude_com.index, fraude_com.values)
plt.title('Top Comercios con Fraude')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# =========================================================
# 📊 5. Fraude por mes
# =========================================================
fraude_mes = fraudes.groupby('mes')['monto_transaccion'].sum()

plt.figure()
plt.plot(fraude_mes.index.astype(str), fraude_mes.values, marker='s')
plt.title('Monto Fraudulento por Mes')
plt.xticks(rotation=45)
plt.grid()
plt.tight_layout()
plt.show()