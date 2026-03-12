"""
Archivo de configuración general del proyecto.

Aquí se centralizan los parámetros de conexión y las rutas más importantes.
La idea es que el estudiante o el profesor solo tenga que cambiar este archivo
si desea apuntar a otra base de datos o mover las carpetas de trabajo.
"""

from pathlib import Path

# =========================
# CONFIGURACIÓN DE MYSQL
# =========================
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",  # Contraseña vacía según lo indicado
    "database": "fraude_tarjetas_dw",
}

# =========================
# RUTAS DEL PROYECTO
# =========================
BASE_DIR = Path(__file__).resolve().parent
PLANTILLAS_DIR = BASE_DIR / "plantillas_csv"
SALIDAS_DIR = BASE_DIR / "salidas"
SQL_DIR = BASE_DIR / "sql"

# Se crea la carpeta de salidas si no existe.
SALIDAS_DIR.mkdir(exist_ok=True)

# =========================
# PARÁMETROS GENERALES
# =========================
ENCODING = "utf-8-sig"
BATCH_SIZE = 1000

# Archivo SQL opcional para crear el esquema desde cero.
SCHEMA_FILE = SQL_DIR / "schema_dw.sql"