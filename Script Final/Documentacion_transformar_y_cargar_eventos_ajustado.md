# `transformar_y_cargar_eventos_ajustado.py`

## Propósito
Este es el archivo principal ("core") del proyecto de Extracción, Transformación y Carga (ETL). Su objetivo vital es tomar el dataset inicial, en bruto y "sucio", pasarlo por rigurosas reglas de limpieza de texto, fechas y de negocio, y posteriormente inyectarlo directamente en un Data Warehouse (MySQL) siguiendo una arquitectura dimensional estructurada tipo estrella/copo de nieve.

## Dependencias
- `pandas` para todo el marco de DataFrames, transformaciones, uniones (merge) cruzadas de datos numéricos y de fecha en un set de más de 50.000 registros.
- `mysql.connector` para establecer túneles `.connect()` e inyecciones masivas parametrizadas con `executemany` que resguardan la limpieza y previenen sentencias ilegibles (SQL Injection). 
- `argparse` y `sys`/`pathlib`/`re`/`unicodedata`/`math` para procesamiento lógico desde la terminal con parseo de flags y manipulación de texto.
- Invoca las constantes parametrizadas del núcleo `config_ajustado.py`.

## Fases del Proceso ETL

### 1. Extracción (y validación del esquema base)
Revisa `validar_columnas()` apenas carga el DataFrame original a través de `preparar_dataframe()`, deteniéndose en caso de que falte alguna del conjunto predefinido `EXPECTED_EVENT_COLUMNS`.

### 2. Transformaciones (Limpieza y Cálculos)
1. **Sanitización de Texto:** `limpiar_texto()` remueve mayúsculas mixtas, extrae tildes (`quitar_tildes()`), y normaliza nombres propios como McDonald's (`smart_title()`).
2. **Homologación de Catálogos:** Utiliza configuraciones predefinidas (Ej. Mapea *Soltero* con mayúscula si viene como *soltero* minúscula).
3. **Parseo de Fechas (`parsear_fecha`):** Acepta un arreglo variado de formatos (ISO 8601, DD/MM/YYYY o MM-DD-YYYY) mediante un bloque try/catch.
4. **Parseo Monetario (`parsear_numero`):** Implementa expresiones regulares para destripar la suciedad monetaria (`[^0-9,.-]`) identificando cuando el punto es milesimo o centesimal gracias a la función `rfind()`.
5. **Generadoras:** Evalúa fórmulas condicionadas para dar valor nulo o calcular (Ej: `monto_total` = monto + interes - pct_descuento + etc.). 

### 3. Carga y Lookups de Modelado
Tras limpiar, se comunica al entorno MSQL para consultar tablas predefinidas (`aplicar_lookups()`). Reemplaza nombres de ciudad o profesiones por su `ID_Numerico` correspondiente a través de variables diccionario (`ciudad_map`, `profesion_map`).

Construye una sub-separación por conjuntos extraídos del Dataframe Original (`construir_dimensiones_y_fact()`) entregando piezas exclusivas de clientes y tarjetas:
- `Dim_Cliente`
- `Dim_Tarjeta`
- `Dim_Tiempo`
- `Dim_Comercio` (Corrigiendo posibles duplicados)
- `Fact_Transaccion_Tarjeta`

Por último, `validar_referencias_fact()` cruza IDs usando `Sets` lógicos para interceptar un Foranea Faltante, abortando el macro-proceso previniendo contaminación de integridad referencial. Si todo sale bien, llama iterativamente a `insertar_dataframe()` inyectando SQL mediante UPSERT.

## Resultados / Salidas Variadas Generadas
- 7 Tablas pobladas de lado de la BD MySQL DW o la modalidad "Sólo ETL".
- Un archivo general consolidado (`Reporte_Calidad_ETL.md`) indicando cómo resultó el análisis a nivel datos.
- Archivo adjunto detallando motivos por los que un subconjunto no pudo importarse (`Registros_Rechazados.csv`).
- Todos los DataFrames particionados en CSV legibles como backup por directorio `salidas_demo/`.
