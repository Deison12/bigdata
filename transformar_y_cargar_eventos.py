"""
Script 2: transformación del dataset de eventos/transacciones y carga al DW.

Qué hace este script:
1. Lee un CSV crudo con las transacciones del evento o caso de uso.
2. Renombra las columnas a una estructura estándar.
3. Limpia textos, fechas y valores numéricos.
4. Busca los IDs de las dimensiones precargadas en MySQL.
5. Construye las dimensiones dinámicas:
   - Dim_Cliente
   - Dim_Tarjeta
   - Dim_Tiempo
   - Dim_Comercio
   - Dim_Ubicacion
6. Construye la tabla de hechos Fact_Transaccion_Tarjeta.
7. Exporta cada tabla transformada a CSV.
8. Inserta los datos en MySQL.

Importante:
- Este script está pensado para un proyecto académico y por eso es muy
  comentado.
- Si tu CSV real usa nombres de columna diferentes, solo debes ajustar el
  diccionario COLUMNAS_ENTRADA que aparece más abajo.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd
import mysql.connector
from mysql.connector import Error

from config import DB_CONFIG, ENCODING, SALIDAS_DIR


# -----------------------------------------------------------------------------
# MAPEO DE COLUMNAS DEL CSV CRUDO
# -----------------------------------------------------------------------------
# El lado izquierdo es el nombre REAL que viene en tu CSV.
# El lado derecho es el nombre estándar que usará el script internamente.
# Si tu archivo viene con otros encabezados, aquí es donde debes cambiarlos.
COLUMNAS_ENTRADA = {
    "id_transaccion": "id_transaccion",
    "nombre_cliente": "nombre_cliente",
    "genero": "genero",
    "fecha_nacimiento": "fecha_nacimiento",
    "ingresos_mensuales": "ingresos_mensuales",
    "score_crediticio": "score_crediticio",
    "estado_civil": "estado_civil",
    "profesion": "profesion",
    "numero_tarjeta": "numero_tarjeta",
    "tipo_tarjeta": "tipo_tarjeta",
    "fecha_emision": "fecha_emision",
    "fecha_vencimiento": "fecha_vencimiento",
    "limite_credito": "limite_credito",
    "tasa_interes": "tasa_interes",
    "categoria_tarjeta": "categoria_tarjeta",
    "fecha_transaccion": "fecha_transaccion",
    "nombre_comercio": "nombre_comercio",
    "categoria_comercio": "categoria_comercio",
    "departamento": "departamento",
    "ciudad": "ciudad",
    "monto_transaccion": "monto_transaccion",
    "interes_generado": "interes_generado",
    "comision": "comision",
    "descuento_aplicado": "descuento_aplicado",
    "impuesto": "impuesto",
    "monto_total": "monto_total",
    "cuotas": "cuotas",
    "saldo_anterior": "saldo_anterior",
    "saldo_posterior": "saldo_posterior",
    "pago_minimo": "pago_minimo",
    "puntos_generados": "puntos_generados",
    "cashback": "cashback",
    "es_fraude": "es_fraude",
    "estado_transaccion": "estado_transaccion",
    "canal_transaccion": "canal_transaccion",
}


# -----------------------------------------------------------------------------
# FUNCIONES DE APOYO
# -----------------------------------------------------------------------------
def conectar_mysql():
    """Abre una conexión al DW en MySQL."""
    return mysql.connector.connect(**DB_CONFIG)


def estandarizar_texto(valor):
    """Limpia espacios extra y convierte textos vacíos en None."""
    if pd.isna(valor):
        return None
    texto = str(valor).strip()
    return texto if texto else None


def convertir_fecha(serie: pd.Series) -> pd.Series:
    """Convierte una serie a fecha sin hora."""
    return pd.to_datetime(serie, errors="coerce").dt.date


def convertir_numero(serie: pd.Series) -> pd.Series:
    """Convierte una serie a numérica, enviando errores a NaN."""
    return pd.to_numeric(serie, errors="coerce")


def convertir_bit(serie: pd.Series) -> pd.Series:
    """Convierte valores comunes de fraude a 0/1."""
    mapa = {
        "1": 1,
        "0": 0,
        "true": 1,
        "false": 0,
        "si": 1,
        "sí": 1,
        "no": 0,
        "fraude": 1,
        "normal": 0,
    }
    return (
        serie.astype(str)
        .str.strip()
        .str.lower()
        .map(mapa)
        .fillna(pd.to_numeric(serie, errors="coerce"))
        .fillna(0)
        .astype(int)
    )


def enmascarar_tarjeta(numero_tarjeta) -> str | None:
    """Genera el número enmascarado con los últimos 4 dígitos visibles."""
    if pd.isna(numero_tarjeta):
        return None
    digitos = "".join(ch for ch in str(numero_tarjeta) if ch.isdigit())
    if len(digitos) < 4:
        return None
    return f"****-****-****-{digitos[-4:]}"


def validar_columnas(df: pd.DataFrame) -> None:
    """Verifica que el CSV tenga todas las columnas definidas en COLUMNAS_ENTRADA."""
    faltantes = [col for col in COLUMNAS_ENTRADA if col not in df.columns]
    if faltantes:
        raise ValueError(
            "El archivo de eventos no tiene estas columnas requeridas: "
            f"{faltantes}. Revisa el diccionario COLUMNAS_ENTRADA."
        )


def consultar_lookup(query: str) -> Dict:
    """Devuelve un diccionario con los valores de una tabla de referencia.

    La primera columna del SELECT se usa como llave del diccionario y la segunda
    como valor.
    """
    with conectar_mysql() as conexion:
        with conexion.cursor() as cursor:
            cursor.execute(query)
            filas = cursor.fetchall()
    return {fila[0]: fila[1] for fila in filas}


def construir_sql_upsert(tabla: str, columnas: List[str]) -> str:
    """Genera un INSERT ... ON DUPLICATE KEY UPDATE para una tabla dada."""
    columnas_sql = ", ".join(columnas)
    placeholders = ", ".join(["%s"] * len(columnas))
    update_sql = ", ".join([f"{col}=VALUES({col})" for col in columnas[1:]])
    return f"INSERT INTO {tabla} ({columnas_sql}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_sql}"


def insertar_dataframe(tabla: str, df: pd.DataFrame) -> None:
    """Inserta un DataFrame completo a una tabla MySQL."""
    if df.empty:
        print(f"[WARN] {tabla} no tiene registros para insertar.")
        return

    columnas = df.columns.tolist()
    sql = construir_sql_upsert(tabla, columnas)
    registros = [tuple(None if pd.isna(v) else v for v in fila) for fila in df.itertuples(index=False, name=None)]

    with conectar_mysql() as conexion:
        with conexion.cursor() as cursor:
            cursor.executemany(sql, registros)
        conexion.commit()

    print(f"[OK] {tabla}: {len(df)} registros cargados.")


def truncar_tablas_dinamicas() -> None:
    """Limpia las tablas dinámicas antes de volver a cargar el proceso.

    Esto es útil en ejercicios académicos cuando se desea repetir el flujo de
    carga completa desde cero, sin conservar una corrida anterior.
    """
    tablas = [
        "Fact_Transaccion_Tarjeta",
        "Dim_Ubicacion",
        "Dim_Comercio",
        "Dim_Tiempo",
        "Dim_Tarjeta",
        "Dim_Cliente",
    ]
    with conectar_mysql() as conexion:
        with conexion.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            for tabla in tablas:
                cursor.execute(f"TRUNCATE TABLE {tabla}")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conexion.commit()
    print("[OK] Tablas dinámicas limpiadas.")


# -----------------------------------------------------------------------------
# TRANSFORMACIÓN DEL DATASET
# -----------------------------------------------------------------------------
def preparar_dataframe(ruta_csv: Path) -> pd.DataFrame:
    """Lee el archivo crudo y lo normaliza."""
    df = pd.read_csv(ruta_csv, encoding=ENCODING)
    validar_columnas(df)
    df = df.rename(columns=COLUMNAS_ENTRADA).copy()

    # Limpieza de columnas de texto.
    columnas_texto = [
        "nombre_cliente",
        "genero",
        "estado_civil",
        "profesion",
        "tipo_tarjeta",
        "categoria_tarjeta",
        "nombre_comercio",
        "categoria_comercio",
        "departamento",
        "ciudad",
        "estado_transaccion",
        "canal_transaccion",
    ]
    for columna in columnas_texto:
        df[columna] = df[columna].apply(estandarizar_texto)

    # Fechas.
    for columna_fecha in ["fecha_nacimiento", "fecha_emision", "fecha_vencimiento", "fecha_transaccion"]:
        df[columna_fecha] = convertir_fecha(df[columna_fecha])

    # Números.
    columnas_numericas = [
        "id_transaccion",
        "ingresos_mensuales",
        "score_crediticio",
        "limite_credito",
        "tasa_interes",
        "monto_transaccion",
        "interes_generado",
        "comision",
        "descuento_aplicado",
        "impuesto",
        "monto_total",
        "cuotas",
        "saldo_anterior",
        "saldo_posterior",
        "pago_minimo",
        "puntos_generados",
        "cashback",
    ]
    for columna in columnas_numericas:
        df[columna] = convertir_numero(df[columna])

    df["es_fraude"] = convertir_bit(df["es_fraude"])
    df["numero_enmascarado"] = df["numero_tarjeta"].apply(enmascarar_tarjeta)

    # Aseguramos que exista un id de transacción. Si el CSV no lo trae bien,
    # generamos uno secuencial.
    if df["id_transaccion"].isna().all():
        df["id_transaccion"] = range(1, len(df) + 1)
    df["id_transaccion"] = df["id_transaccion"].fillna(method="ffill").fillna(0).astype(int)

    return df


def aplicar_lookups(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte nombres de dimensiones precargadas a sus IDs reales en MySQL."""
    estado_civil_map = consultar_lookup(
        "SELECT LOWER(descripcion), id_estado_civil FROM Dim_Estado_Civil"
    )
    profesion_map = consultar_lookup(
        "SELECT LOWER(nombre), id_profesion FROM Dim_Profesion"
    )
    categoria_tarjeta_map = consultar_lookup(
        "SELECT LOWER(nombre_categoria), id_categoria_tarjeta FROM Dim_Categoria_Tarjeta"
    )
    categoria_comercio_map = consultar_lookup(
        "SELECT LOWER(nombre_categoria), id_categoria_comercio FROM Dim_Categoria_Comercio"
    )
    trimestre_map = consultar_lookup(
        "SELECT numero_trimestre, id_trimestre FROM Dim_Trimestre"
    )

    # Para ciudades necesitamos considerar el departamento, porque puede haber
    # ciudades repetidas en distintos departamentos.
    with conectar_mysql() as conexion:
        with conexion.cursor() as cursor:
            cursor.execute(
                """
                SELECT LOWER(c.nombre_ciudad), LOWER(d.nombre_departamento), c.id_ciudad
                FROM Dim_Ciudad c
                INNER JOIN Dim_Departamento d ON c.id_departamento = d.id_departamento
                """
            )
            ciudad_filas = cursor.fetchall()
    ciudad_map = {(fila[0], fila[1]): fila[2] for fila in ciudad_filas}

    df["id_estado_civil"] = df["estado_civil"].str.lower().map(estado_civil_map)
    df["id_profesion"] = df["profesion"].str.lower().map(profesion_map)
    df["id_categoria_tarjeta"] = df["categoria_tarjeta"].str.lower().map(categoria_tarjeta_map)
    df["id_categoria_comercio"] = df["categoria_comercio"].str.lower().map(categoria_comercio_map)

    df["trimestre_numero"] = pd.to_datetime(df["fecha_transaccion"], errors="coerce").dt.quarter
    df["id_trimestre"] = df["trimestre_numero"].map(trimestre_map)

    df["id_ciudad"] = df.apply(
        lambda fila: ciudad_map.get(
            (
                str(fila["ciudad"]).lower() if pd.notna(fila["ciudad"]) else None,
                str(fila["departamento"]).lower() if pd.notna(fila["departamento"]) else None,
            )
        ),
        axis=1,
    )

    # Validaciones tempranas para evitar que la carga falle después.
    columnas_fk = [
        "id_estado_civil",
        "id_profesion",
        "id_categoria_tarjeta",
        "id_categoria_comercio",
        "id_trimestre",
        "id_ciudad",
    ]
    for columna in columnas_fk:
        faltantes = int(df[columna].isna().sum())
        if faltantes > 0:
            raise ValueError(
                f"La columna {columna} tiene {faltantes} valores sin homologar. "
                "Revisa los CSV precargados o los nombres del dataset crudo."
            )

    df[columnas_fk] = df[columnas_fk].astype(int)
    return df


def construir_dimensiones_y_fact(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """A partir del dataset limpio genera las dimensiones y la tabla de hechos."""

    # -----------------------
    # DIMENSIÓN CLIENTE
    # -----------------------
    dim_cliente = (
        df[
            [
                "nombre_cliente",
                "genero",
                "fecha_nacimiento",
                "ingresos_mensuales",
                "score_crediticio",
                "id_estado_civil",
                "id_profesion",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_cliente.insert(0, "id_cliente", range(1, len(dim_cliente) + 1))

    # Creamos una copia auxiliar para asignar el id_cliente de vuelta al dataset.
    dim_cliente_aux = dim_cliente.copy()
    dim_cliente_aux = dim_cliente_aux.rename(columns={"nombre_cliente": "nombre_cliente"})

    df = df.merge(
        dim_cliente_aux,
        how="left",
        on=[
            "nombre_cliente",
            "genero",
            "fecha_nacimiento",
            "ingresos_mensuales",
            "score_crediticio",
            "id_estado_civil",
            "id_profesion",
        ],
    )

    # La dimensión final debe usar el nombre de la columna del modelo dimensional.
    dim_cliente = dim_cliente.rename(columns={"nombre_cliente": "nombre"})

    # -----------------------
    # DIMENSIÓN TARJETA
    # -----------------------
    dim_tarjeta = (
        df[
            [
                "numero_enmascarado",
                "tipo_tarjeta",
                "fecha_emision",
                "fecha_vencimiento",
                "limite_credito",
                "tasa_interes",
                "id_categoria_tarjeta",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_tarjeta.insert(0, "id_tarjeta", range(1, len(dim_tarjeta) + 1))

    df = df.merge(
        dim_tarjeta,
        how="left",
        on=[
            "numero_enmascarado",
            "tipo_tarjeta",
            "fecha_emision",
            "fecha_vencimiento",
            "limite_credito",
            "tasa_interes",
            "id_categoria_tarjeta",
        ],
    )

    # -----------------------
    # DIMENSIÓN TIEMPO
    # -----------------------
    dim_tiempo = df[["fecha_transaccion", "id_trimestre"]].drop_duplicates().reset_index(drop=True)
    dim_tiempo = dim_tiempo.rename(columns={"fecha_transaccion": "fecha"})
    dim_tiempo["dia"] = pd.to_datetime(dim_tiempo["fecha"]).dt.day
    dim_tiempo["mes"] = pd.to_datetime(dim_tiempo["fecha"]).dt.month
    dim_tiempo["anio"] = pd.to_datetime(dim_tiempo["fecha"]).dt.year
    dim_tiempo.insert(0, "id_tiempo", range(1, len(dim_tiempo) + 1))

    df = df.merge(
        dim_tiempo,
        how="left",
        left_on=["fecha_transaccion", "id_trimestre"],
        right_on=["fecha", "id_trimestre"],
    )

    # -----------------------
    # DIMENSIÓN COMERCIO
    # -----------------------
    dim_comercio = (
        df[["nombre_comercio", "id_categoria_comercio"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_comercio.insert(0, "id_comercio", range(1, len(dim_comercio) + 1))
    dim_comercio = dim_comercio.rename(columns={"nombre_comercio": "nombre_comercio"})

    df = df.merge(
        dim_comercio,
        how="left",
        on=["nombre_comercio", "id_categoria_comercio"],
    )

    # -----------------------
    # DIMENSIÓN UBICACIÓN
    # -----------------------
    dim_ubicacion = df[["id_ciudad"]].drop_duplicates().reset_index(drop=True)
    dim_ubicacion.insert(0, "id_ubicacion", range(1, len(dim_ubicacion) + 1))

    df = df.merge(dim_ubicacion, how="left", on=["id_ciudad"])

    # -----------------------
    # TABLA DE HECHOS
    # -----------------------
    fact = df[
        [
            "id_transaccion",
            "id_cliente",
            "id_tarjeta",
            "id_tiempo",
            "id_comercio",
            "id_ubicacion",
            "monto_transaccion",
            "interes_generado",
            "comision",
            "descuento_aplicado",
            "impuesto",
            "monto_total",
            "cuotas",
            "saldo_anterior",
            "saldo_posterior",
            "pago_minimo",
            "puntos_generados",
            "cashback",
            "es_fraude",
            "estado_transaccion",
            "canal_transaccion",
        ]
    ].copy()

    # Limpieza final de columnas y tipos.
    dim_cliente = dim_cliente[
        [
            "id_cliente",
            "nombre",
            "genero",
            "fecha_nacimiento",
            "ingresos_mensuales",
            "score_crediticio",
            "id_estado_civil",
            "id_profesion",
        ]
    ]
    dim_tarjeta = dim_tarjeta[
        [
            "id_tarjeta",
            "numero_enmascarado",
            "tipo_tarjeta",
            "fecha_emision",
            "fecha_vencimiento",
            "limite_credito",
            "tasa_interes",
            "id_categoria_tarjeta",
        ]
    ]
    dim_tiempo = dim_tiempo[["id_tiempo", "fecha", "dia", "mes", "anio", "id_trimestre"]]
    dim_comercio = dim_comercio[["id_comercio", "nombre_comercio", "id_categoria_comercio"]]
    dim_ubicacion = dim_ubicacion[["id_ubicacion", "id_ciudad"]]

    return {
        "Dim_Cliente": dim_cliente,
        "Dim_Tarjeta": dim_tarjeta,
        "Dim_Tiempo": dim_tiempo,
        "Dim_Comercio": dim_comercio,
        "Dim_Ubicacion": dim_ubicacion,
        "Fact_Transaccion_Tarjeta": fact,
    }


def exportar_csv(tablas: Dict[str, pd.DataFrame], output_dir: Path) -> None:
    """Exporta cada DataFrame transformado a un CSV independiente."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for nombre_tabla, df in tablas.items():
        ruta = output_dir / f"{nombre_tabla}.csv"
        df.to_csv(ruta, index=False, encoding=ENCODING)
        print(f"[OK] CSV generado: {ruta}")


# -----------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# -----------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Transforma un CSV de eventos y lo carga al DW")
    parser.add_argument("--archivo", required=True, help="Ruta del CSV crudo de eventos/transacciones")
    parser.add_argument(
        "--salida",
        default=str(SALIDAS_DIR),
        help="Carpeta donde se guardarán los CSV transformados.",
    )
    parser.add_argument(
        "--no-cargar-db",
        action="store_true",
        help="Si se incluye, solo genera los CSV y no inserta datos en MySQL.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Si se incluye, limpia primero las tablas dinámicas antes de insertar.",
    )
    args = parser.parse_args()

    ruta_csv = Path(args.archivo)
    salida = Path(args.salida)

    if not ruta_csv.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {ruta_csv.resolve()}")

    try:
        print("[INFO] Leyendo y preparando dataset crudo...")
        df = preparar_dataframe(ruta_csv)

        print("[INFO] Buscando IDs de dimensiones precargadas...")
        df = aplicar_lookups(df)

        print("[INFO] Construyendo dimensiones dinámicas y tabla de hechos...")
        tablas = construir_dimensiones_y_fact(df)

        print("[INFO] Exportando resultados a CSV...")
        exportar_csv(tablas, salida)

        if not args.no_cargar_db:
            if args.truncate:
                truncar_tablas_dinamicas()

            print("[INFO] Cargando tablas transformadas en MySQL...")
            orden_carga = [
                "Dim_Cliente",
                "Dim_Tarjeta",
                "Dim_Tiempo",
                "Dim_Comercio",
                "Dim_Ubicacion",
                "Fact_Transaccion_Tarjeta",
            ]
            for tabla in orden_carga:
                insertar_dataframe(tabla, tablas[tabla])

        print("\n================ RESUMEN ================")
        for nombre, tabla_df in tablas.items():
            print(f"{nombre}: {len(tabla_df)} registros")
        print("Proceso completado correctamente.")

    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}")
    except (ValueError, Error) as exc:
        print(f"[ERROR] {exc}")


if __name__ == "__main__":
    main()