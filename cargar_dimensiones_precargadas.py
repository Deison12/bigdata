"""
Script 1: carga de dimensiones precargadas a MySQL.

Objetivo:
- Crear la base de datos y las tablas del esquema estrella si el usuario lo desea.
- Leer los archivos CSV de dimensiones estáticas o precargadas.
- Insertar los datos en MySQL respetando el orden correcto de llaves foráneas.

Ejemplo de uso:
    python cargar_dimensiones_precargadas.py --init-schema
    python cargar_dimensiones_precargadas.py --csv-dir plantillas_csv

Dimensiones que normalmente se cargan aquí:
- Dim_Departamento
- Dim_Ciudad
- Dim_Estado_Civil
- Dim_Profesion
- Dim_Categoria_Tarjeta
- Dim_Categoria_Comercio
- Dim_Trimestre
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pandas as pd
import mysql.connector
from mysql.connector import Error

from config import DB_CONFIG, ENCODING, SCHEMA_FILE


# -----------------------------------------------------------------------------
# CONFIGURACIÓN DE LAS DIMENSIONES PRECARGADAS
# -----------------------------------------------------------------------------
# Aquí definimos el nombre de la tabla, el nombre del archivo CSV esperado y las
# columnas que deben existir en el archivo.
DIMENSIONES_PRECARGADAS = [
    {
        "tabla": "Dim_Departamento",
        "archivo": "dim_departamento.csv",
        "columnas": ["id_departamento", "nombre_departamento", "pais"],
    },
    {
        "tabla": "Dim_Ciudad",
        "archivo": "dim_ciudad.csv",
        "columnas": ["id_ciudad", "nombre_ciudad", "id_departamento"],
    },
    {
        "tabla": "Dim_Estado_Civil",
        "archivo": "dim_estado_civil.csv",
        "columnas": ["id_estado_civil", "descripcion"],
    },
    {
        "tabla": "Dim_Profesion",
        "archivo": "dim_profesion.csv",
        "columnas": ["id_profesion", "nombre", "sector"],
    },
    {
        "tabla": "Dim_Categoria_Tarjeta",
        "archivo": "dim_categoria_tarjeta.csv",
        "columnas": ["id_categoria_tarjeta", "nombre_categoria", "nivel_beneficio"],
    },
    {
        "tabla": "Dim_Categoria_Comercio",
        "archivo": "dim_categoria_comercio.csv",
        "columnas": ["id_categoria_comercio", "nombre_categoria", "tipo_gasto"],
    },
    {
        "tabla": "Dim_Trimestre",
        "archivo": "dim_trimestre.csv",
        "columnas": ["id_trimestre", "numero_trimestre", "descripcion"],
    },
]


# -----------------------------------------------------------------------------
# FUNCIONES DE APOYO
# -----------------------------------------------------------------------------
def conectar_mysql(sin_base: bool = False):
    """Crea una conexión a MySQL.

    Parámetros:
        sin_base: si es True, se conecta solo al servidor. Esto es útil cuando
        queremos crear primero la base de datos.
    """
    config = DB_CONFIG.copy()
    if sin_base:
        config.pop("database", None)
    return mysql.connector.connect(**config)


def ejecutar_schema() -> None:
    """Ejecuta el archivo schema_dw.sql para crear el modelo desde cero."""
    if not SCHEMA_FILE.exists():
        raise FileNotFoundError(f"No se encontró el archivo SQL: {SCHEMA_FILE}")

    print(f"[INFO] Ejecutando esquema SQL: {SCHEMA_FILE}")
    with conectar_mysql(sin_base=True) as conexion:
        with conexion.cursor() as cursor:
            contenido = SCHEMA_FILE.read_text(encoding="utf-8")
            for resultado in cursor.execute(contenido, multi=True):
                _ = resultado
        conexion.commit()
    print("[OK] Esquema creado correctamente.")


def validar_columnas(df: pd.DataFrame, columnas_esperadas: List[str], nombre_archivo: str) -> None:
    """Valida que el CSV tenga las columnas requeridas."""
    faltantes = [col for col in columnas_esperadas if col not in df.columns]
    if faltantes:
        raise ValueError(
            f"El archivo {nombre_archivo} no tiene estas columnas requeridas: {faltantes}"
        )


def construir_sql_upsert(tabla: str, columnas: List[str]) -> str:
    """Construye una sentencia INSERT ... ON DUPLICATE KEY UPDATE.

    Esto permite re-ejecutar el script sin que falle por registros duplicados,
    siempre que exista una llave primaria o una llave única que capture el caso.
    """
    columnas_sql = ", ".join(columnas)
    placeholders = ", ".join(["%s"] * len(columnas))

    # En la parte UPDATE excluimos la primera columna si es un identificador.
    columnas_update = [c for c in columnas if not c.startswith("id_") or c != columnas[0]]
    if not columnas_update:
        columnas_update = columnas[1:]

    update_sql = ", ".join([f"{col}=VALUES({col})" for col in columnas_update])
    return f"INSERT INTO {tabla} ({columnas_sql}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_sql}"


def cargar_csv_a_tabla(ruta_csv: Path, tabla: str, columnas: List[str]) -> int:
    """Carga un CSV a una tabla específica y retorna la cantidad de filas procesadas."""
    print(f"[INFO] Leyendo {ruta_csv.name} -> {tabla}")
    df = pd.read_csv(ruta_csv, encoding=ENCODING)
    validar_columnas(df, columnas, ruta_csv.name)
    df = df[columnas].copy()
    df = df.where(pd.notnull(df), None)

    sql = construir_sql_upsert(tabla, columnas)
    registros = [tuple(fila) for fila in df.itertuples(index=False, name=None)]

    with conectar_mysql() as conexion:
        with conexion.cursor() as cursor:
            cursor.executemany(sql, registros)
        conexion.commit()

    print(f"[OK] {tabla}: {len(registros)} registros cargados/actualizados.")
    return len(registros)


# -----------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# -----------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Carga dimensiones precargadas a MySQL")
    parser.add_argument(
        "--csv-dir",
        default="plantillas_csv",
        help="Carpeta donde están los CSV de dimensiones precargadas.",
    )
    parser.add_argument(
        "--init-schema",
        action="store_true",
        help="Si se incluye, crea la base de datos y las tablas ejecutando schema_dw.sql.",
    )
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    if not csv_dir.exists():
        raise FileNotFoundError(f"No existe la carpeta de CSV: {csv_dir.resolve()}")

    try:
        if args.init_schema:
            ejecutar_schema()

        total = 0
        for dimension in DIMENSIONES_PRECARGADAS:
            ruta_csv = csv_dir / dimension["archivo"]
            if not ruta_csv.exists():
                print(f"[WARN] No se encontró {ruta_csv.name}. Se omite esta dimensión.")
                continue

            total += cargar_csv_a_tabla(
                ruta_csv=ruta_csv,
                tabla=dimension["tabla"],
                columnas=dimension["columnas"],
            )

        print("\n================ RESUMEN ================")
        print(f"Total de registros procesados: {total}")
        print("Proceso finalizado correctamente.")

    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}")
    except (ValueError, Error) as exc:
        print(f"[ERROR] {exc}")


if __name__ == "__main__":
    main()