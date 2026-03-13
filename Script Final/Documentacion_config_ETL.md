# Documentación de `config.py`

## 1. Propósito del archivo

`config.py` es el archivo central de configuración del proyecto ETL de fraude con tarjetas.

Su función es concentrar:
- la conexión a MySQL,
- las rutas del proyecto,
- la estructura esperada del dataset crudo,
- las reglas base de limpieza,
- y algunos parámetros de negocio que el ETL necesita para calcular campos.

La idea es que el profesor o el estudiante pueda entender y ajustar el proyecto desde un solo punto.

---

## 2. Qué problema corrige esta nueva versión

La versión anterior del proyecto estaba pensada para un dataset más limpio y todavía asumía columnas como:
- `id_transaccion`
- `comision`
- `monto_total`
- `pago_minimo`

Pero en la nueva definición del proyecto eso cambió:
- `id_transaccion` debe ser generado por la base de datos.
- `comision` ya no se manejará.
- `monto_total` debe calcularse en el ETL.
- `pago_minimo` debe calcularse en el ETL.

Además, ahora el dataset del evento es intencionalmente sucio:
- trae nulos,
- formatos de fecha mezclados,
- números con símbolos y separadores,
- textos con espacios,
- mayúsculas y minúsculas mezcladas,
- y variantes de escritura como `Bogota`, `bogotá`, `BOGOTÁ`.

Por eso `config.py` ahora quedó preparado para ese escenario.

---

## 3. Componentes principales del archivo

### 3.1. `DB_CONFIG`
Define los parámetros de conexión a MySQL.

En este proyecto:
- host: `127.0.0.1`
- puerto: `3306`
- usuario: `root`
- contraseña: vacía
- base de datos: `fraude_tarjetas_dw`

### 3.2. Rutas del proyecto
Se definieron rutas para:
- `DIMENSIONES_DIR`
- `SALIDAS_DIR`
- `SQL_DIR`
- `LOGS_DIR`
- `ARCHIVO_EVENTOS_POR_DEFECTO`

### 3.3. `EXPECTED_EVENT_COLUMNS`
Contiene la lista oficial de columnas que el dataset crudo sí debe traer.

No aparecen:
- `id_transaccion`
- `comision`
- `monto_total`
- `pago_minimo`

porque no deben venir desde la fuente.

### 3.4. Reglas de limpieza
Se definieron:
- `NULL_LITERALS`
- `DATE_FORMATS`
- `TEXT_COLUMNS`
- `NUMERIC_COLUMNS`
- `DATE_COLUMNS`

### 3.5. Reglas de cálculo
Se dejaron parametrizadas las reglas para:
- `monto_total`
- `pago_minimo`

### 3.6. `CANONICAL_MAPS`
Define cómo homologar valores sucios a una forma estándar.

Ejemplos:
- `bogota` -> `Bogotá`
- `clasica` -> `Clásica`
- `ing.` -> `Ingeniero`
- `profesor` -> `Docente`
- `app movil` -> `App móvil`

---

## 4. Por qué `READ_CSV_DTYPE = str`

Se configuró que el CSV se lea inicialmente como texto para no perder la suciedad del dataset:
- `$ 5,489,655.00`
- `118950,71`
- `03-10-2025`

Así el ETL puede demostrar la limpieza y normalización paso a paso.

---

## 5. Uso recomendado

El script `transformar_y_cargar_eventos.py` debe importar desde aquí:
- conexión
- rutas
- listas de columnas
- formatos
- mapas canónicos
- reglas de cálculo

Así cualquier cambio futuro se hace en un solo archivo.
