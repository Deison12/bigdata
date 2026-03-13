# cargar_dimensiones_precargadas.py

import argparse
import csv
import sys
from pathlib import Path

import mysql.connector
from mysql.connector import Error

from config_ajustado import DB_CONFIG


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CSV_DIR = BASE_DIR / "plantillas_csv"
DEFAULT_SCHEMA_FILE = BASE_DIR / "sql" / "schema_dw.sql"

# Orden importante por posibles dependencias
CSV_TABLES = [
    ("dim_departamento.csv", "Dim_Departamento"),
    ("dim_ciudad.csv", "Dim_Ciudad"),
    ("dim_estado_civil.csv", "Dim_Estado_Civil"),
    ("dim_profesion.csv", "Dim_Profesion"),
    ("dim_categoria_tarjeta.csv", "Dim_Categoria_Tarjeta"),
    ("dim_categoria_comercio.csv", "Dim_Categoria_Comercio"),
    ("dim_trimestre.csv", "Dim_Trimestre"),
]


def get_connection(use_database=True):
    cfg = DB_CONFIG.copy()
    if not use_database:
        cfg.pop("database", None)
    return mysql.connector.connect(**cfg)


def ensure_database_exists():
    database_name = DB_CONFIG["database"]

    conn = get_connection(use_database=False)
    cursor = conn.cursor()

    cursor.execute(
        f"CREATE DATABASE IF NOT EXISTS `{database_name}` "
        "DEFAULT CHARACTER SET utf8mb4 "
        "DEFAULT COLLATE utf8mb4_unicode_ci"
    )

    cursor.close()
    conn.close()
    print(f"[OK] Base de datos verificada/creada: {database_name}")


def split_sql_statements(script_text):
    """
    Divide un script SQL en sentencias individuales respetando comillas
    y omitiendo comentarios simples/bloque.
    """
    statements = []
    buffer = []

    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False

    i = 0
    length = len(script_text)

    while i < length:
        ch = script_text[i]
        nxt = script_text[i + 1] if i + 1 < length else ""

        # Fin comentario de línea
        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        # Fin comentario de bloque
        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue

        # Inicio comentarios (solo si no estamos dentro de comillas)
        if not in_single_quote and not in_double_quote:
            if ch == "-" and nxt == "-":
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                i += 2
                continue

        # Manejo de comillas
        if ch == "'" and not in_double_quote:
            prev_char = script_text[i - 1] if i > 0 else ""
            if prev_char != "\\":
                in_single_quote = not in_single_quote

        elif ch == '"' and not in_single_quote:
            prev_char = script_text[i - 1] if i > 0 else ""
            if prev_char != "\\":
                in_double_quote = not in_double_quote

        # Separador real de sentencia
        if ch == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
        else:
            buffer.append(ch)

        i += 1

    remaining = "".join(buffer).strip()
    if remaining:
        statements.append(remaining)

    # Ignorar DELIMITER si existiera
    filtered = []
    for stmt in statements:
        if stmt.strip().upper().startswith("DELIMITER"):
            continue
        filtered.append(stmt)

    return filtered


def ejecutar_schema(schema_file: Path):
    if not schema_file.exists():
        raise FileNotFoundError(f"No existe el archivo de schema: {schema_file}")

    print(f"[INFO] Ejecutando esquema SQL: {schema_file}")

    with open(schema_file, "r", encoding="utf-8") as f:
        script = f.read()

    sentencias = split_sql_statements(script)
    if not sentencias:
        raise ValueError("El archivo schema_dw.sql no contiene sentencias ejecutables.")

    conn = get_connection(use_database=True)
    cursor = conn.cursor()

    try:
        for i, sentencia in enumerate(sentencias, start=1):
            try:
                cursor.execute(sentencia)
            except Error as e:
                preview = sentencia[:200].replace("\n", " ")
                raise RuntimeError(
                    f"Error ejecutando sentencia #{i}: {e}\nSQL: {preview}..."
                ) from e

        conn.commit()
        print(f"[OK] Esquema SQL ejecutado correctamente. Sentencias: {len(sentencias)}")
    finally:
        cursor.close()
        conn.close()


def normalizar_valor(valor):
    if valor is None:
        return None

    valor = str(valor).strip()

    if valor == "":
        return None

    if valor.lower() in {"null", "none", "nan"}:
        return None

    return valor


def limpiar_tabla(cursor, table_name):
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute(f"DELETE FROM `{table_name}`")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")


def cargar_csv_en_tabla(cursor, csv_path: Path, table_name: str, replace_data=False):
    if not csv_path.exists():
        raise FileNotFoundError(f"No existe el archivo CSV: {csv_path}")

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError(f"El CSV {csv_path.name} no tiene encabezados.")

        columnas = [c.strip() for c in reader.fieldnames if c and c.strip()]
        if not columnas:
            raise ValueError(f"El CSV {csv_path.name} no tiene columnas válidas.")

        filas = []
        for row in reader:
            valores = [normalizar_valor(row.get(col)) for col in columnas]

            # Saltar filas totalmente vacías
            if all(v is None for v in valores):
                continue

            filas.append(tuple(valores))

    if not filas:
        print(f"[WARN] {csv_path.name}: no contiene filas útiles para cargar.")
        return 0

    if replace_data:
        limpiar_tabla(cursor, table_name)

    placeholders = ", ".join(["%s"] * len(columnas))
    columnas_sql = ", ".join([f"`{c}`" for c in columnas])

    sql = f"INSERT INTO `{table_name}` ({columnas_sql}) VALUES ({placeholders})"
    cursor.executemany(sql, filas)

    return len(filas)


def cargar_dimensiones_precargadas(csv_dir: Path, replace_data=False):
    if not csv_dir.exists():
        raise FileNotFoundError(f"No existe el directorio de CSV: {csv_dir}")

    conn = get_connection(use_database=True)
    cursor = conn.cursor()

    total_registros = 0

    try:
        for csv_name, table_name in CSV_TABLES:
            csv_path = csv_dir / csv_name
            print(f"[INFO] Cargando {csv_name} -> {table_name}")

            cantidad = cargar_csv_en_tabla(
                cursor=cursor,
                csv_path=csv_path,
                table_name=table_name,
                replace_data=replace_data
            )

            conn.commit()
            total_registros += cantidad
            print(f"[OK] {table_name}: {cantidad} registros cargados")

        print(f"[OK] Carga completa de dimensiones precargadas. Total registros: {total_registros}")
    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Crea esquema DW y carga dimensiones precargadas desde CSV."
    )
    parser.add_argument(
        "--init-schema",
        action="store_true",
        help="Ejecuta el schema_dw.sql antes de cargar los CSV"
    )
    parser.add_argument(
        "--schema-file",
        default=str(DEFAULT_SCHEMA_FILE),
        help="Ruta al archivo schema_dw.sql"
    )
    parser.add_argument(
        "--csv-dir",
        default=str(DEFAULT_CSV_DIR),
        help="Directorio donde están los CSV de dimensiones precargadas"
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Solo crea base/esquema, no carga CSV"
    )
    parser.add_argument(
        "--replace-data",
        action="store_true",
        help="Limpia la tabla antes de volver a cargar cada dimensión"
    )

    args = parser.parse_args()

    schema_file = Path(args.schema_file).resolve()
    csv_dir = Path(args.csv_dir).resolve()

    try:
        ensure_database_exists()

        if args.init_schema:
            ejecutar_schema(schema_file)

        if not args.skip_load:
            cargar_dimensiones_precargadas(
                csv_dir=csv_dir,
                replace_data=args.replace_data
            )

        print("[OK] Proceso finalizado correctamente.")

    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()