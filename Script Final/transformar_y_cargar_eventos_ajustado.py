from __future__ import annotations

"""
Script de transformación del dataset de eventos/transacciones y carga al DW.

Versión ajustada para trabajar con el dataset sucio del proyecto.

Cambios principales:
- Ya no espera `id_transaccion` en la fuente.
- Ya no espera `comision` en la fuente.
- Calcula `monto_total` en el ETL.
- Calcula `pago_minimo` en el ETL.
- Limpia textos, números y fechas con suciedad controlada.
- Homologa catálogos antes de buscar llaves foráneas.
- Separa registros rechazados en lugar de detener todo el proceso por nulos
  o datos imposibles de homologar.

Mejoras de esta versión:
- Propaga errores reales con código de salida != 0.
- Muestra trazas útiles para depuración.
- Detecta mejor errores de llaves foráneas antes de insertar la fact.
- Usa INSERT normal para todas las tablas si se ejecuta con --truncate.
- Usa UPSERT solo para dimensiones cuando NO se ejecuta con --truncate.
- Corrige duplicados de Dim_Comercio normalizando la llave de negocio antes de deduplicar.
"""

import argparse
import math
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple

try:
    import mysql.connector
    from mysql.connector import Error
except ModuleNotFoundError:
    mysql = None

    class Error(Exception):
        pass

import pandas as pd

try:
    from config_ajustado import (
        ARCHIVO_EVENTOS_POR_DEFECTO,
        BATCH_SIZE,
        CANONICAL_MAPS,
        DATE_COLUMNS,
        DATE_FORMATS,
        DB_CONFIG,
        ENCODING,
        EXPECTED_EVENT_COLUMNS,
        NULL_LITERALS,
        NUMERIC_COLUMNS,
        PAGO_MINIMO_FIJO,
        PAGO_MINIMO_PCT_MIN,
        READ_CSV_DTYPE,
        SALIDAS_DIR,
        STRICT_LOOKUPS,
        TEXT_COLUMNS,
    )
except ImportError:
    from config import DB_CONFIG, ENCODING, SALIDAS_DIR  # type: ignore

    ARCHIVO_EVENTOS_POR_DEFECTO = Path("dataset_eventos_50000_v2_sucio.csv")
    BATCH_SIZE = 1000
    EXPECTED_EVENT_COLUMNS = []
    NULL_LITERALS = {"", " ", "nan", "null", "none", "n/a"}
    DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y"]
    TEXT_COLUMNS = []
    NUMERIC_COLUMNS = []
    DATE_COLUMNS = []
    CANONICAL_MAPS = {}
    PAGO_MINIMO_FIJO = 30000
    PAGO_MINIMO_PCT_MIN = 0.04
    READ_CSV_DTYPE = str
    STRICT_LOOKUPS = True


# -----------------------------------------------------------------------------
# CONEXIÓN Y UTILIDADES GENERALES
# -----------------------------------------------------------------------------
def conectar_mysql():
    """Abre una conexión al DW en MySQL."""
    if mysql is None:
        raise ModuleNotFoundError(
            "No se encontró la librería mysql-connector-python. "
            "Instálala con: pip install mysql-connector-python"
        )
    return mysql.connector.connect(**DB_CONFIG)


def quitar_tildes(texto: str) -> str:
    """Elimina tildes para facilitar la homologación de catálogos."""
    return "".join(
        ch for ch in unicodedata.normalize("NFD", texto)
        if unicodedata.category(ch) != "Mn"
    )


def normalizar_clave(valor: object) -> str | None:
    """Genera una clave comparable para mapas y búsquedas."""
    if valor is None or pd.isna(valor):
        return None
    texto = str(valor).strip()
    if texto.lower() in NULL_LITERALS:
        return None
    texto = re.sub(r"\s+", " ", texto)
    texto = quitar_tildes(texto).lower()
    return texto


def smart_title(texto: str) -> str:
    """Capitaliza de forma razonable nombres y comercios."""
    limpio = re.sub(r"\s+", " ", texto.strip())
    if not limpio:
        return limpio

    if limpio.isupper() or limpio.islower():
        limpio = limpio.title()

    reemplazos = {
        "Mcdonald'S": "McDonald's",
        "D1": "D1",
        "Pos": "POS",
        "Ktronix": "Ktronix",
        "App Móvil": "App móvil",
    }
    return reemplazos.get(limpio, limpio)


def valor_es_nulo(valor: object) -> bool:
    if valor is None or pd.isna(valor):
        return True
    return str(valor).strip().lower() in NULL_LITERALS


def convertir_serie_a_int_nullable(serie: pd.Series) -> pd.Series:
    """
    Convierte una serie a enteros preservando nulos.
    Devuelve dtype object con ints de Python o None.
    """
    return serie.apply(lambda x: int(x) if pd.notna(x) else None)


# -----------------------------------------------------------------------------
# LIMPIEZA DE TEXTO, FECHA Y NÚMEROS
# -----------------------------------------------------------------------------
def limpiar_texto(valor: object, columna: str | None = None) -> str | None:
    """Limpia espacios, homologa catálogos y normaliza texto."""
    if valor_es_nulo(valor):
        return None

    texto = re.sub(r"\s+", " ", str(valor).strip())
    clave = normalizar_clave(texto)
    if clave is None:
        return None

    if columna and columna in CANONICAL_MAPS:
        if clave in CANONICAL_MAPS[columna]:
            return CANONICAL_MAPS[columna][clave]

    if columna in {"nombre_cliente", "nombre_comercio"}:
        return smart_title(texto)

    return texto


def parsear_fecha(valor: object):
    """Intenta convertir fechas con varios formatos configurados."""
    if valor_es_nulo(valor):
        return None

    texto = re.sub(r"\s+", " ", str(valor).strip())
    for fmt in DATE_FORMATS:
        try:
            return pd.to_datetime(texto, format=fmt, errors="raise").date()
        except Exception:
            pass

    try:
        fecha = pd.to_datetime(texto, errors="coerce")
        if pd.isna(fecha):
            return None
        return fecha.date()
    except Exception:
        return None


_num_pattern = re.compile(r"[^0-9,.-]")


def parsear_numero(valor: object) -> float | None:
    """Limpia valores monetarios y numéricos con formatos mixtos."""
    if valor_es_nulo(valor):
        return None

    texto = str(valor).strip()
    texto = _num_pattern.sub("", texto)
    if texto in {"", "-", ".", ","}:
        return None

    if "," in texto and "." in texto:
        if texto.rfind(",") > texto.rfind("."):
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", "")
    elif "," in texto:
        partes = texto.split(",")
        if len(partes) == 2 and len(partes[1]) in (1, 2):
            texto = texto.replace(",", ".")
        else:
            texto = texto.replace(",", "")
    elif texto.count(".") > 1:
        texto = texto.replace(".", "")

    try:
        return float(texto)
    except ValueError:
        return None


def convertir_bit(serie: pd.Series) -> pd.Series:
    """Convierte distintas variantes de sí/no a 0/1."""
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

    def _convertir(valor: object) -> int:
        clave = normalizar_clave(valor)
        if clave is None:
            return 0
        if clave in mapa:
            return mapa[clave]
        numero = parsear_numero(valor)
        if numero is None:
            return 0
        return int(numero)

    return serie.apply(_convertir).astype(int)


# def enmascarar_tarjeta(numero_tarjeta: object) -> str | None:
#     """Genera un número enmascarado con los últimos 4 dígitos visibles."""
#     if valor_es_nulo(numero_tarjeta):
#         return None
#     digitos = "".join(ch for ch in str(numero_tarjeta) if ch.isdigit())
#     if len(digitos) < 4:
#         return None
#     return f"****-****-****-{digitos[-4:]}"
def enmascarar_tarjeta(numero_tarjeta: object) -> str | None:
    """Devuelve el número de tarjeta completo, sin enmascarar."""
    if valor_es_nulo(numero_tarjeta):
        return None

    digitos = "".join(ch for ch in str(numero_tarjeta) if ch.isdigit())
    if not digitos:
        return None

    return digitos


def validar_columnas(df: pd.DataFrame) -> None:
    """Valida que el CSV tenga las columnas requeridas."""
    faltantes = [col for col in EXPECTED_EVENT_COLUMNS if col not in df.columns]
    if faltantes:
        raise ValueError(
            "El archivo de eventos no tiene estas columnas requeridas: "
            f"{faltantes}."
        )


def separar_rechazados(
    df: pd.DataFrame,
    condicion: pd.Series,
    motivo: str,
    acumulado: List[pd.DataFrame],
) -> pd.DataFrame:
    """Separa filas inválidas y las acumula con una razón de rechazo."""
    if condicion.any():
        rechazados = df[condicion].copy()
        rechazados["motivo_rechazo"] = motivo
        acumulado.append(rechazados)
    return df[~condicion].copy()


# -----------------------------------------------------------------------------
# PREPARACIÓN DEL DATASET CRUDO
# -----------------------------------------------------------------------------
def preparar_dataframe(ruta_csv: Path) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    """Lee, limpia y aplica reglas básicas al dataset sucio."""
    df = pd.read_csv(ruta_csv, encoding=ENCODING, dtype=READ_CSV_DTYPE)
    validar_columnas(df)
    df = df.copy()
    df.insert(0, "fila_origen", range(2, len(df) + 2))

    metricas = {"registros_entrada": len(df)}
    rechazados: List[pd.DataFrame] = []

    for columna in TEXT_COLUMNS:
        if columna in df.columns:
            df[columna] = df[columna].apply(lambda v: limpiar_texto(v, columna))

    for columna in DATE_COLUMNS:
        if columna in df.columns:
            df[columna] = df[columna].apply(parsear_fecha)

    for columna in NUMERIC_COLUMNS:
        if columna in df.columns:
            df[columna] = df[columna].apply(parsear_numero)

    df["es_fraude"] = convertir_bit(df["es_fraude"])
    df["numero_enmascarado"] = df["numero_tarjeta"].apply(enmascarar_tarjeta)

    df["interes_generado"] = df["interes_generado"].fillna(0.0)
    df["descuento_aplicado"] = df["descuento_aplicado"].fillna(0.0)
    df["cashback"] = df["cashback"].fillna(0.0)
    df["cuotas"] = df["cuotas"].fillna(1).round().astype(int)
    df["saldo_anterior"] = df["saldo_anterior"].fillna(0.0)

    impuesto_calculado = (
        df["monto_transaccion"].fillna(0)
        - df["descuento_aplicado"].fillna(0)
        + df["interes_generado"].fillna(0)
    ) * 0.19
    df["impuesto"] = df["impuesto"].fillna(impuesto_calculado.round(2))

    saldo_posterior_calculado = (
        df["saldo_anterior"].fillna(0)
        + df["monto_transaccion"].fillna(0)
        + df["interes_generado"].fillna(0)
        - df["descuento_aplicado"].fillna(0)
        + df["impuesto"].fillna(0)
        - df["cashback"].fillna(0)
    )
    df["saldo_posterior"] = df["saldo_posterior"].fillna(saldo_posterior_calculado.round(2))

    puntos_calculados = df["monto_transaccion"].fillna(0).apply(lambda x: int(math.floor(x / 1000)))
    df["puntos_generados"] = df["puntos_generados"].fillna(puntos_calculados)

    df["monto_total"] = (
        df["monto_transaccion"].fillna(0)
        + df["interes_generado"].fillna(0)
        - df["descuento_aplicado"].fillna(0)
        + df["impuesto"].fillna(0)
        - df["cashback"].fillna(0)
    ).round(2)

    df["pago_minimo"] = df["saldo_posterior"].fillna(0).apply(
        lambda x: round(max(PAGO_MINIMO_FIJO, x * PAGO_MINIMO_PCT_MIN), 2)
    )

    for columna in ["score_crediticio", "cuotas", "puntos_generados"]:
        if columna in df.columns:
            df[columna] = df[columna].apply(lambda x: int(x) if pd.notna(x) else None)

    columnas_esenciales = [
        "nombre_cliente",
        "numero_enmascarado",
        "fecha_transaccion",
        "nombre_comercio",
        "categoria_comercio",
        "departamento",
        "ciudad",
        "monto_transaccion",
    ]
    condicion_faltantes = df[columnas_esenciales].isna().any(axis=1)
    df = separar_rechazados(
        df,
        condicion_faltantes,
        "Faltan campos esenciales luego de la limpieza",
        rechazados,
    )

    metricas["registros_validos_post_limpieza"] = len(df)
    metricas["registros_rechazados_limpieza"] = sum(len(x) for x in rechazados)

    df_rechazados = pd.concat(rechazados, ignore_index=True) if rechazados else pd.DataFrame()
    return df, df_rechazados, metricas


# -----------------------------------------------------------------------------
# LOOKUPS A DIMENSIONES PRECARGADAS
# -----------------------------------------------------------------------------
def consultar_lookup(query: str) -> Dict:
    """Devuelve un diccionario con los valores de una tabla de referencia."""
    conexion = conectar_mysql()
    try:
        cursor = conexion.cursor()
        try:
            cursor.execute(query)
            filas = cursor.fetchall()
        finally:
            cursor.close()
    finally:
        conexion.close()
    return {fila[0]: fila[1] for fila in filas}


def aplicar_lookups(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    """Homologa y busca IDs de dimensiones precargadas."""
    metricas = {}
    rechazados: List[pd.DataFrame] = []

    estado_civil_map = {
        normalizar_clave(k): v
        for k, v in consultar_lookup("SELECT descripcion, id_estado_civil FROM Dim_Estado_Civil").items()
    }
    profesion_map = {
        normalizar_clave(k): v
        for k, v in consultar_lookup("SELECT nombre, id_profesion FROM Dim_Profesion").items()
    }
    categoria_tarjeta_map = {
        normalizar_clave(k): v
        for k, v in consultar_lookup("SELECT nombre_categoria, id_categoria_tarjeta FROM Dim_Categoria_Tarjeta").items()
    }
    categoria_comercio_map = {
        normalizar_clave(k): v
        for k, v in consultar_lookup("SELECT nombre_categoria, id_categoria_comercio FROM Dim_Categoria_Comercio").items()
    }
    trimestre_map = consultar_lookup("SELECT numero_trimestre, id_trimestre FROM Dim_Trimestre")

    conexion = conectar_mysql()
    try:
        cursor = conexion.cursor()
        try:
            cursor.execute(
                """
                SELECT c.nombre_ciudad, d.nombre_departamento, c.id_ciudad
                FROM Dim_Ciudad c
                INNER JOIN Dim_Departamento d ON c.id_departamento = d.id_departamento
                """
            )
            ciudad_filas = cursor.fetchall()
        finally:
            cursor.close()
    finally:
        conexion.close()

    ciudad_map = {
        (normalizar_clave(fila[0]), normalizar_clave(fila[1])): fila[2]
        for fila in ciudad_filas
    }

    df = df.copy()
    df["id_estado_civil"] = df["estado_civil"].apply(
        lambda x: estado_civil_map.get(normalizar_clave(x)) if pd.notna(x) else None
    )
    df["id_profesion"] = df["profesion"].apply(
        lambda x: profesion_map.get(normalizar_clave(x)) if pd.notna(x) else None
    )
    df["id_categoria_tarjeta"] = df["categoria_tarjeta"].apply(
        lambda x: categoria_tarjeta_map.get(normalizar_clave(x)) if pd.notna(x) else None
    )
    df["id_categoria_comercio"] = df["categoria_comercio"].apply(
        lambda x: categoria_comercio_map.get(normalizar_clave(x)) if pd.notna(x) else None
    )
    df["trimestre_numero"] = pd.to_datetime(df["fecha_transaccion"], errors="coerce").dt.quarter
    df["id_trimestre"] = df["trimestre_numero"].map(trimestre_map)
    df["id_ciudad"] = df.apply(
        lambda fila: ciudad_map.get(
            (normalizar_clave(fila["ciudad"]), normalizar_clave(fila["departamento"]))
        ),
        axis=1,
    )

    columnas_requeridas = ["id_categoria_comercio", "id_trimestre", "id_ciudad"]
    for columna in columnas_requeridas:
        condicion = df[columna].isna()
        if condicion.any():
            motivo = f"No se pudo homologar la llave requerida: {columna}"
            df = separar_rechazados(df, condicion, motivo, rechazados)

    metricas["registros_validos_post_lookups"] = len(df)
    metricas["registros_rechazados_lookup"] = sum(len(x) for x in rechazados)

    df_rechazados = pd.concat(rechazados, ignore_index=True) if rechazados else pd.DataFrame()

    if STRICT_LOOKUPS and df.empty:
        raise ValueError(
            "Después de homologar dimensiones no quedaron registros válidos. "
            "Revisa las dimensiones precargadas y las reglas canónicas."
        )

    return df, df_rechazados, metricas


# -----------------------------------------------------------------------------
# CONSTRUCCIÓN DE DIMENSIONES Y TABLA DE HECHOS
# -----------------------------------------------------------------------------
def construir_dimensiones_y_fact(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """A partir del dataset limpio genera las dimensiones dinámicas y la fact."""
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
    df = df.merge(
        dim_cliente,
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
    dim_cliente = dim_cliente.rename(columns={"nombre_cliente": "nombre"})

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
    tmp_comercio = df[["nombre_comercio", "id_categoria_comercio"]].copy()

    # Llave normalizada para deduplicar como MySQL realmente compara
    tmp_comercio["comercio_key"] = tmp_comercio["nombre_comercio"].apply(
        lambda x: normalizar_clave(x) if pd.notna(x) else None
    )

    tmp_comercio = tmp_comercio.dropna(subset=["comercio_key", "id_categoria_comercio"])

    dim_comercio = (
        tmp_comercio
        .drop_duplicates(subset=["comercio_key", "id_categoria_comercio"])
        .reset_index(drop=True)
    )

    dim_comercio["nombre_comercio"] = dim_comercio["nombre_comercio"].apply(
        lambda x: smart_title(str(x)) if pd.notna(x) else x
    )

    dim_comercio.insert(0, "id_comercio", range(1, len(dim_comercio) + 1))

    df["comercio_key"] = df["nombre_comercio"].apply(
        lambda x: normalizar_clave(x) if pd.notna(x) else None
    )

    df = df.merge(
        dim_comercio[["id_comercio", "comercio_key", "id_categoria_comercio"]],
        how="left",
        on=["comercio_key", "id_categoria_comercio"],
    )

    dim_comercio = dim_comercio[["id_comercio", "nombre_comercio", "id_categoria_comercio"]]

    dim_ubicacion = df[["id_ciudad"]].drop_duplicates().reset_index(drop=True)
    dim_ubicacion.insert(0, "id_ubicacion", range(1, len(dim_ubicacion) + 1))
    df = df.merge(dim_ubicacion, how="left", on=["id_ciudad"])

    for col in ["id_cliente", "id_tarjeta", "id_tiempo", "id_comercio", "id_ubicacion"]:
        df[col] = convertir_serie_a_int_nullable(df[col])

    dim_cliente["id_cliente"] = convertir_serie_a_int_nullable(dim_cliente["id_cliente"])
    dim_tarjeta["id_tarjeta"] = convertir_serie_a_int_nullable(dim_tarjeta["id_tarjeta"])
    dim_tiempo["id_tiempo"] = convertir_serie_a_int_nullable(dim_tiempo["id_tiempo"])
    dim_comercio["id_comercio"] = convertir_serie_a_int_nullable(dim_comercio["id_comercio"])
    dim_ubicacion["id_ubicacion"] = convertir_serie_a_int_nullable(dim_ubicacion["id_ubicacion"])

    fact = df[
        [
            "id_cliente",
            "id_tarjeta",
            "id_tiempo",
            "id_comercio",
            "id_ubicacion",
            "monto_transaccion",
            "interes_generado",
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
    dim_ubicacion = dim_ubicacion[["id_ubicacion", "id_ciudad"]]

    columnas_fk = ["id_cliente", "id_tarjeta", "id_tiempo", "id_comercio", "id_ubicacion"]
    if fact[columnas_fk].isna().any().any():
        nulos = fact[columnas_fk].isna().sum().to_dict()
        raise ValueError(f"La tabla de hechos quedó con llaves foráneas nulas: {nulos}")

    return {
        "Dim_Cliente": dim_cliente,
        "Dim_Tarjeta": dim_tarjeta,
        "Dim_Tiempo": dim_tiempo,
        "Dim_Comercio": dim_comercio,
        "Dim_Ubicacion": dim_ubicacion,
        "Fact_Transaccion_Tarjeta": fact,
    }


def validar_referencias_fact(tablas: Dict[str, pd.DataFrame]) -> None:
    """
    Verifica que todos los ids de la fact existan en sus dimensiones
    antes de intentar insertar en MySQL.
    """
    fact = tablas["Fact_Transaccion_Tarjeta"]

    mapa = {
        "id_cliente": set(tablas["Dim_Cliente"]["id_cliente"].dropna().astype(int).tolist()),
        "id_tarjeta": set(tablas["Dim_Tarjeta"]["id_tarjeta"].dropna().astype(int).tolist()),
        "id_tiempo": set(tablas["Dim_Tiempo"]["id_tiempo"].dropna().astype(int).tolist()),
        "id_comercio": set(tablas["Dim_Comercio"]["id_comercio"].dropna().astype(int).tolist()),
        "id_ubicacion": set(tablas["Dim_Ubicacion"]["id_ubicacion"].dropna().astype(int).tolist()),
    }

    errores = {}
    for fk, universo in mapa.items():
        ids_fact = set(fact[fk].dropna().astype(int).tolist())
        faltantes = ids_fact - universo
        if faltantes:
            errores[fk] = sorted(list(faltantes))[:10]

    if errores:
        raise ValueError(
            "La fact contiene llaves foráneas que no existen en las dimensiones construidas: "
            f"{errores}"
        )


# -----------------------------------------------------------------------------
# SALIDA Y CARGA
# -----------------------------------------------------------------------------
def construir_sql_upsert(tabla: str, columnas: List[str]) -> str:
    """Genera un INSERT ... ON DUPLICATE KEY UPDATE para una tabla dada."""
    columnas_sql = ", ".join([f"`{c}`" for c in columnas])
    placeholders = ", ".join(["%s"] * len(columnas))
    update_sql = ", ".join([f"`{col}`=VALUES(`{col}`)" for col in columnas[1:]])
    return (
        f"INSERT INTO `{tabla}` ({columnas_sql}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )


def construir_sql_insert(tabla: str, columnas: List[str]) -> str:
    """Genera un INSERT normal."""
    columnas_sql = ", ".join([f"`{c}`" for c in columnas])
    placeholders = ", ".join(["%s"] * len(columnas))
    return f"INSERT INTO `{tabla}` ({columnas_sql}) VALUES ({placeholders})"


def insertar_dataframe(tabla: str, df: pd.DataFrame, usar_upsert: bool = True) -> None:
    """Inserta un DataFrame completo a una tabla MySQL."""
    if df.empty:
        print(f"[WARN] {tabla} no tiene registros para insertar.")
        return

    columnas = df.columns.tolist()
    sql = construir_sql_upsert(tabla, columnas) if usar_upsert else construir_sql_insert(tabla, columnas)

    registros = [
        tuple(None if pd.isna(v) else v for v in fila)
        for fila in df.itertuples(index=False, name=None)
    ]

    conexion = conectar_mysql()
    try:
        cursor = conexion.cursor()
        try:
            for inicio in range(0, len(registros), BATCH_SIZE):
                lote = registros[inicio:inicio + BATCH_SIZE]
                cursor.executemany(sql, lote)
            conexion.commit()
        except Error:
            conexion.rollback()
            raise
        finally:
            cursor.close()
    finally:
        conexion.close()

    modo = "UPSERT" if usar_upsert else "INSERT"
    print(f"[OK] {tabla}: {len(df)} registros cargados con {modo}.")


def truncar_tablas_dinamicas() -> None:
    """Limpia las tablas dinámicas antes de recargar."""
    tablas = [
        "Fact_Transaccion_Tarjeta",
        "Dim_Ubicacion",
        "Dim_Comercio",
        "Dim_Tiempo",
        "Dim_Tarjeta",
        "Dim_Cliente",
    ]
    conexion = conectar_mysql()
    try:
        cursor = conexion.cursor()
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            for tabla in tablas:
                cursor.execute(f"TRUNCATE TABLE `{tabla}`")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            conexion.commit()
        except Error:
            conexion.rollback()
            raise
        finally:
            cursor.close()
    finally:
        conexion.close()

    print("[OK] Tablas dinámicas limpiadas.")


def exportar_csv(tablas: Dict[str, pd.DataFrame], output_dir: Path) -> None:
    """Exporta cada tabla transformada a un CSV independiente."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for nombre_tabla, df in tablas.items():
        ruta = output_dir / f"{nombre_tabla}.csv"
        try:
            df.to_csv(ruta, index=False, encoding=ENCODING)
            print(f"[OK] CSV generado: {ruta}")
        except PermissionError as exc:
            raise PermissionError(
                f"No se pudo escribir el archivo '{ruta}'. "
                f"Probablemente está abierto en Excel. Ciérralo y vuelve a ejecutar."
            ) from exc


def exportar_rechazados(df_rechazados: pd.DataFrame, output_dir: Path) -> None:
    """Guarda el detalle de registros rechazados."""
    if df_rechazados.empty:
        return

    ruta = output_dir / "Registros_Rechazados.csv"
    try:
        df_rechazados.to_csv(ruta, index=False, encoding=ENCODING)
        print(f"[OK] CSV de rechazados generado: {ruta}")
    except PermissionError as exc:
        raise PermissionError(
            f"No se pudo escribir el archivo '{ruta}'. "
            f"Probablemente está abierto en Excel. Ciérralo y vuelve a ejecutar."
        ) from exc


def generar_reporte_calidad(metricas: Dict[str, int], output_dir: Path) -> None:
    """Genera un breve reporte markdown del proceso de calidad de datos."""
    ruta = output_dir / "Reporte_Calidad_ETL.md"
    lineas = [
        "# Reporte de calidad del ETL\n",
        "\n",
        "## Resumen\n",
    ]
    for clave, valor in metricas.items():
        lineas.append(f"- **{clave}**: {valor}\n")
    lineas.extend(
        [
            "\n## Reglas aplicadas\n",
            "- Limpieza de espacios y homologación de catálogos.\n",
            "- Conversión de fechas en varios formatos.\n",
            "- Conversión de montos con símbolos y separadores mixtos.\n",
            "- Cálculo de `monto_total`.\n",
            "- Cálculo de `pago_minimo`.\n",
            "- Separación de registros rechazados con su motivo.\n",
        ]
    )

    try:
        ruta.write_text("".join(lineas), encoding="utf-8")
        print(f"[OK] Reporte de calidad generado: {ruta}")
    except PermissionError as exc:
        raise PermissionError(
            f"No se pudo escribir el archivo '{ruta}'. "
            f"Probablemente está abierto en otro programa. Ciérralo y vuelve a ejecutar."
        ) from exc


# -----------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# -----------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Transforma un CSV de eventos sucio y lo carga al DW")
    parser.add_argument(
        "--archivo",
        default=str(ARCHIVO_EVENTOS_POR_DEFECTO),
        help="Ruta del CSV crudo de eventos/transacciones.",
    )
    parser.add_argument(
        "--salida",
        default=str(SALIDAS_DIR),
        help="Carpeta donde se guardarán los CSV transformados.",
    )
    parser.add_argument(
        "--no-cargar-db",
        action="store_true",
        help="Si se incluye, genera CSV y reportes pero no inserta datos en MySQL.",
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
        print("[INFO] Leyendo y limpiando dataset crudo...")
        df, rechazados_limpieza, metricas_limpieza = preparar_dataframe(ruta_csv)

        print("[INFO] Buscando IDs de dimensiones precargadas...")
        df, rechazados_lookup, metricas_lookup = aplicar_lookups(df)

        rechazados = (
            pd.concat(
                [x for x in [rechazados_limpieza, rechazados_lookup] if not x.empty],
                ignore_index=True,
            )
            if (not rechazados_limpieza.empty or not rechazados_lookup.empty)
            else pd.DataFrame()
        )

        metricas = {
            **metricas_limpieza,
            **metricas_lookup,
            "registros_rechazados_total": len(rechazados),
            "registros_finales_para_carga": len(df),
        }

        print("[INFO] Construyendo dimensiones dinámicas y tabla de hechos...")
        tablas = construir_dimensiones_y_fact(df)

        print("[DEBUG] Conteos preparados para salida/carga:")
        for nombre_tabla, df_tabla in tablas.items():
            print(f"  - {nombre_tabla}: {len(df_tabla)} registros")

        print("[DEBUG] Fact_Transaccion_Tarjeta a insertar:", len(tablas["Fact_Transaccion_Tarjeta"]))

        print("[INFO] Validando llaves de la fact contra dimensiones construidas...")
        validar_referencias_fact(tablas)
        print("[OK] Validación de llaves de la fact superada.")

        print("[INFO] Exportando resultados a CSV...")
        exportar_csv(tablas, salida)
        exportar_rechazados(rechazados, salida)
        generar_reporte_calidad(metricas, salida)

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
                if args.truncate:
                    insertar_dataframe(tabla, tablas[tabla], usar_upsert=False)
                else:
                    if tabla == "Fact_Transaccion_Tarjeta":
                        insertar_dataframe(tabla, tablas[tabla], usar_upsert=False)
                    else:
                        insertar_dataframe(tabla, tablas[tabla], usar_upsert=True)

        print("\n================ RESUMEN ================")
        for nombre, tabla_df in tablas.items():
            print(f"{nombre}: {len(tabla_df)} registros")
        print(f"Registros rechazados: {len(rechazados)}")
        print("Proceso completado correctamente.")

    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)
    except PermissionError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)
    except (ValueError, Error) as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR NO CONTROLADO] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()