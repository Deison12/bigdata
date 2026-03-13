"""
Archivo de configuración central del proyecto ETL de fraude con tarjetas.

Objetivo:
- Reunir en un solo lugar los parámetros de conexión a MySQL.
- Definir las rutas usadas por los scripts.
- Centralizar reglas base de lectura y limpieza del dataset sucio.
- Evitar que el profesor o el estudiante tenga que modificar varios archivos.

Importante:
- Este proyecto usa un dataset crudo "sucio", por eso varias reglas de limpieza
  y normalización se dejan definidas aquí para reutilizarlas en el ETL.
- El archivo de eventos NO trae:
    * id_transaccion
    * comision
    * monto_total
    * pago_minimo
  porque esos valores se manejan en la base de datos o se calculan dentro del ETL.
"""

from pathlib import Path

# =============================================================================
# CONFIGURACIÓN DE MYSQL
# =============================================================================
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",  # Contraseña vacía según lo indicado
    "database": "fraude_tarjetas_dw",
}

# =============================================================================
# RUTAS DEL PROYECTO
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent

# Carpeta donde se almacenan los CSV de dimensiones precargadas.
DIMENSIONES_DIR = BASE_DIR / "plantillas_csv"

# Carpeta de salida de las tablas transformadas.
SALIDAS_DIR = BASE_DIR / "salidas"

# Carpeta de archivos SQL.
SQL_DIR = BASE_DIR / "sql"

# Carpeta opcional para logs y reportes.
LOGS_DIR = BASE_DIR / "logs"

# Dataset de eventos por defecto.
ARCHIVO_EVENTOS_POR_DEFECTO = BASE_DIR / "dataset_eventos_50000_v2_sucio.csv"

# Crear carpetas si no existen.
SALIDAS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# =============================================================================
# PARÁMETROS GENERALES DE ARCHIVOS
# =============================================================================
ENCODING = "utf-8-sig"

# Se fuerza la lectura como texto para no perder la "suciedad" del dataset:
# símbolos $, comas, espacios, fechas con formatos mixtos, etc.
READ_CSV_DTYPE = str

# Cantidad de registros por lote para cargas grandes.
BATCH_SIZE = 1000

# Archivo SQL opcional para crear el esquema desde cero.
SCHEMA_FILE = SQL_DIR / "schema_dw.sql"

# =============================================================================
# COLUMNAS ESPERADAS DEL DATASET CRUDO DE EVENTOS
# =============================================================================
# Nota:
# - No se incluye id_transaccion, porque la base lo debe generar automáticamente.
# - No se incluye comision.
# - No se incluye monto_total ni pago_minimo, porque deben calcularse en el ETL.
EXPECTED_EVENT_COLUMNS = [
    "nombre_cliente",
    "genero",
    "fecha_nacimiento",
    "ingresos_mensuales",
    "score_crediticio",
    "estado_civil",
    "profesion",
    "numero_tarjeta",
    "tipo_tarjeta",
    "fecha_emision",
    "fecha_vencimiento",
    "limite_credito",
    "tasa_interes",
    "categoria_tarjeta",
    "fecha_transaccion",
    "nombre_comercio",
    "categoria_comercio",
    "departamento",
    "ciudad",
    "monto_transaccion",
    "interes_generado",
    "descuento_aplicado",
    "impuesto",
    "cuotas",
    "saldo_anterior",
    "saldo_posterior",
    "puntos_generados",
    "cashback",
    "es_fraude",
    "estado_transaccion",
    "canal_transaccion",
]

# =============================================================================
# REGLAS BASE DE LIMPIEZA
# =============================================================================
NULL_LITERALS = {
    "",
    " ",
    "nan",
    "NaN",
    "null",
    "NULL",
    "none",
    "None",
    "n/a",
    "N/A",
    "na",
    "NA",
}

DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m-%d-%Y",
]

TEXT_COLUMNS = [
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

NUMERIC_COLUMNS = [
    "ingresos_mensuales",
    "score_crediticio",
    "limite_credito",
    "tasa_interes",
    "monto_transaccion",
    "interes_generado",
    "descuento_aplicado",
    "impuesto",
    "cuotas",
    "saldo_anterior",
    "saldo_posterior",
    "puntos_generados",
    "cashback",
]

DATE_COLUMNS = [
    "fecha_nacimiento",
    "fecha_emision",
    "fecha_vencimiento",
    "fecha_transaccion",
]

# =============================================================================
# REGLAS DE NEGOCIO PARA CÁLCULOS EN EL ETL
# =============================================================================
# monto_total = monto_transaccion + interes_generado - descuento_aplicado + impuesto - cashback
PAGO_MINIMO_FIJO = 30000
PAGO_MINIMO_PCT_MIN = 0.04
PAGO_MINIMO_PCT_MAX = 0.12

# =============================================================================
# HOMOLOGACIÓN DE CATÁLOGOS
# =============================================================================
CANONICAL_MAPS = {
    "genero": {
        "masculino": "Masculino",
        "femenino": "Femenino",
    },
    "tipo_tarjeta": {
        "credito": "Crédito",
        "crédito": "Crédito",
    },
    "categoria_tarjeta": {
        "clasica": "Clásica",
        "clásica": "Clásica",
        "gold": "Gold",
        "platinum": "Platinum",
    },
    "categoria_comercio": {
        "supermercado": "Supermercado",
        "tecnologia": "Tecnología",
        "tecnología": "Tecnología",
        "restaurante": "Restaurante",
        "transporte": "Transporte",
    },
    "estado_transaccion": {
        "aprobada": "Aprobada",
        "en revision": "En revisión",
        "en revisión": "En revisión",
        "rechazada": "Rechazada",
    },
    "canal_transaccion": {
        "app movil": "App móvil",
        "app móvil": "App móvil",
        "web": "Web",
        "pos": "POS",
    },
    "estado_civil": {
        "soltero": "Soltero",
        "casado": "Casado",
        "divorciado": "Divorciado",
        "viudo": "Viudo",
    },
    "profesion": {
        "ingeniero": "Ingeniero",
        "ing.": "Ingeniero",
        "abogado": "Abogado",
        "abog.": "Abogado",
        "comerciante": "Comerciante",
        "comerc.": "Comerciante",
        "docente": "Docente",
        "profesor": "Docente",
    },
    "departamento": {
        "antioquia": "Antioquia",
        "cundinamarca": "Cundinamarca",
        "valle del cauca": "Valle del Cauca",
        "valle": "Valle del Cauca",
    },
    "ciudad": {
        "medellin": "Medellín",
        "medellín": "Medellín",
        "bogota": "Bogotá",
        "bogotá": "Bogotá",
        "cali": "Cali",
    },
}

# =============================================================================
# OPCIONES DEL ETL
# =============================================================================
STRICT_LOOKUPS = True
