# generar_dimensiones_colombia_oficial.py

import pandas as pd
from pathlib import Path
from urllib.request import urlretrieve

# -----------------------------
# URLs oficiales del DANE
# -----------------------------
URL_DEPARTAMENTOS = "https://geoportal.dane.gov.co/descargas/divipola/DIVIPOLA_Departamentos.xlsx"
URL_MUNICIPIOS = "https://geoportal.dane.gov.co/descargas/divipola/DIVIPOLA_Municipios.xlsx"

# -----------------------------
# Carpeta de salida
# -----------------------------
BASE_DIR = Path("dimensiones_colombia_oficial")
BASE_DIR.mkdir(exist_ok=True)

archivo_departamentos_xlsx = BASE_DIR / "DIVIPOLA_Departamentos.xlsx"
archivo_municipios_xlsx = BASE_DIR / "DIVIPOLA_Municipios.xlsx"

archivo_departamentos_csv = BASE_DIR / "dim_departamento_colombia_oficial.csv"
archivo_ciudades_csv = BASE_DIR / "dim_ciudad_colombia_oficial.csv"

# -----------------------------
# Descargar archivos oficiales
# -----------------------------
print("Descargando archivos oficiales del DANE...")
urlretrieve(URL_DEPARTAMENTOS, archivo_departamentos_xlsx)
urlretrieve(URL_MUNICIPIOS, archivo_municipios_xlsx)
print("Descarga completada.")

# -----------------------------
# Leer archivos Excel
# -----------------------------
df_dep = pd.read_excel(archivo_departamentos_xlsx)
df_mun = pd.read_excel(archivo_municipios_xlsx)

# Normalizar nombres de columnas
df_dep.columns = [str(c).strip() for c in df_dep.columns]
df_mun.columns = [str(c).strip() for c in df_mun.columns]

print("\nColumnas detectadas en departamentos:")
print(df_dep.columns.tolist())

print("\nColumnas detectadas en municipios:")
print(df_mun.columns.tolist())

# -----------------------------
# Buscar columnas de forma flexible
# -----------------------------
def buscar_columna(columnas, posibles_nombres):
    for col in columnas:
        col_l = col.lower().strip()
        for nombre in posibles_nombres:
            if nombre in col_l:
                return col
    raise ValueError(f"No encontré una columna compatible con: {posibles_nombres}")

# Departamentos
col_dep_codigo = buscar_columna(df_dep.columns, ["codigo departamento", "código departamento", "codigo"])
col_dep_nombre = buscar_columna(df_dep.columns, ["departamento", "nombre departamento"])

# Municipios
col_mun_codigo = buscar_columna(df_mun.columns, ["codigo municipio", "código municipio"])
col_mun_nombre = buscar_columna(df_mun.columns, ["municipio", "nombre municipio"])
col_mun_dep_codigo = buscar_columna(df_mun.columns, ["codigo departamento", "código departamento"])
col_mun_dep_nombre = buscar_columna(df_mun.columns, ["departamento", "nombre departamento"])

# -----------------------------
# Construir dim_departamento
# -----------------------------
dim_departamento = (
    df_dep[[col_dep_codigo, col_dep_nombre]]
    .dropna()
    .drop_duplicates()
    .rename(columns={
        col_dep_codigo: "codigo_departamento",
        col_dep_nombre: "nombre_departamento"
    })
    .sort_values("nombre_departamento")
    .reset_index(drop=True)
)

dim_departamento["codigo_departamento"] = (
    dim_departamento["codigo_departamento"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.zfill(2)
)

dim_departamento["pais"] = "Colombia"

# -----------------------------
# Construir dim_ciudad
# Nota: aquí 'ciudad' = municipio oficial
# -----------------------------
dim_ciudad = (
    df_mun[[col_mun_codigo, col_mun_nombre, col_mun_dep_codigo, col_mun_dep_nombre]]
    .dropna()
    .drop_duplicates()
    .rename(columns={
        col_mun_codigo: "codigo_ciudad",
        col_mun_nombre: "nombre_ciudad",
        col_mun_dep_codigo: "codigo_departamento",
        col_mun_dep_nombre: "nombre_departamento"
    })
    .sort_values(["nombre_departamento", "nombre_ciudad"])
    .reset_index(drop=True)
)

dim_ciudad["codigo_ciudad"] = (
    dim_ciudad["codigo_ciudad"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.zfill(5)
)

dim_ciudad["codigo_departamento"] = (
    dim_ciudad["codigo_departamento"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.zfill(2)
)

# -----------------------------
# Guardar CSV
# -----------------------------
dim_departamento.to_csv(archivo_departamentos_csv, index=False, encoding="utf-8-sig")
dim_ciudad.to_csv(archivo_ciudades_csv, index=False, encoding="utf-8-sig")

print("\nArchivos generados correctamente:")
print(" -", archivo_departamentos_csv)
print(" -", archivo_ciudades_csv)

print("\nResumen:")
print("Departamentos:", len(dim_departamento))
print("Municipios/Ciudades:", len(dim_ciudad))