# `cargar_dimensiones_precargadas.py`

## Propósito
Este módulo de Python es el encargado de arrancar el proceso de estructuración del Data Warehouse desde cero, si es necesario, y de poblar los catálogos o dimensiones que rara vez cambian (como Profesiones, Estado Civil, Categorías de Tarjetas, Departamentos).

Para esto:
1. Asegura que la base de datos `fraude_tarjetas_dw` exista.
2. Ejecuta sentencias DDL completas (leyendo de `schema_dw.sql`) para crear las tablas si el flag `--init-schema` es pasado por CLI.
3. Lee masivamente los archivos `.csv` en la carpeta predeterminada (frecuentemente `plantillas_csv/`) y vuelca esos registros transaccionalmente en MySQL.

## Dependencias
- `mysql.connector` (Conexión al servidor MySQL local).
- `csv` y `argparse`, `pathlib`.
- Importa configuraciones base desde `config_ajustado.py`.

## Funciones Principales

### `get_connection(use_database=True)`
Devuelve la instancia de la conexión a MySQL instanciando los diccionarios de configuración.

### `ensure_database_exists()`
Se conecta utilizando el nivel de servidor MySQL (`use_database=False`) e intenta el comando de `CREATE DATABASE IF NOT EXISTS`, forzando el formato `utf8mb4_unicode_ci` apropiado para nombres y locaciones con tildes.

### `ejecutar_schema(schema_file: Path)`
Parsea de texto plano un documento SQL de definición de Data Warehouse. Entiende cuándo ignorar un comentario y divide el archivo usando `;`. Luego itera el listado de sentencias y las lanza usando el cursor activo. 

### `normalizar_valor(valor)`
Una función de saneamiento pequeña que examina strings (que originalmente vienen de `DictReader`) y los convierte nativamente en `None` de Python si identifica valores basura como `"nan", "null"` o un espaciado vacío. Esto previene que MySQL guarde valores literales `"NULL"` como si fuesen textos reales.

### `limpiar_tabla(cursor, table_name)`
Antes de hacer re-cargas, tumba silenciosamente la seguridad de Foreign Keys globalmente con `SET FOREIGN_KEY_CHECKS = 0` y efectua un vaciado veloz de datos mediante `DELETE` para evitar solapes o IDs duplicados si se seleccionó la opción de reemplazo (`--replace-data`).

### `cargar_csv_en_tabla(cursor, csv_path: Path, table_name: str, replace_data=False)`
La función en la que recae el esfuerzo más pesado: Lee CSV por CSV, obtiene las cabeceras, valida filas ignorando nulas limpias, construye genéricamente un `INSERT INTO` masivo (generando `num_columnas` comodines `%s`) y lanza el cargue de lote devolviendo las métricas.

## Uso (Cli Argument Parser)
```bash
# Ejecutar y solo cargar CSV (Asume que el DB y las tablas ya existen).
python cargar_dimensiones_precargadas.py 

# Inicializar y recrear el modelo desde schema_dw.sql borrando lo viejo y cargando los CSV desde inicio.
python cargar_dimensiones_precargadas.py --init-schema --replace-data
```
