# `generar_reporte_kpis_negocio.py`

## Propósito
Este script genera un único y extenso **Reporte Ejecutivo en Microsoft Word (.docx)** para resolver de forma directa las **9 preguntas clave del modelo de negocio** propuestas por el Data Warehouse, relacionadas a métricas de prevención de fraude (KPIs). El resultado consta de gráficas, texto descriptivo y tablas tabuladas limpiamente.

Se sirve de Pandas para manipular las bases de datos transaccionales crudas en CSV, pre-calcular agrupaciones analíticas, sacar desviaciones estándar y encontrar el "Top X" de los valores. 

## Dependencias
- `pandas` (Análisis numérico y agrupación).
- `python-docx` (Librería generadora de documentos Microsoft Word MS-XML interactuables desde código puro).
- `matplotlib.pyplot` y `seaborn` (Generación de los gráficos vectorizados insertados como imágenes).
- `io` y `os` (Para inyección de streams temporales de gráficos de memoria al .docx).

## Comportamiento y Procesamiento Subyacente
1. Lee obligatoriamente el archivo `dataset_eventos_50000.csv` con encoding `utf-8-sig`.
2. Calcula automáticamente el nivel de la Antigüedad, restando de a días transcurridos la `fecha_transaccion` contra la `fecha_emision` y segmentándola lógicamente por años en buckets o rangos de análisis (ej: `['<1 año', '1-3 años', ...]`).
3. Dispone de dos funciones unificadoras visuales: `format_currency` y `format_percent`.
4. El núcleo `crear_reporte_kpis()` orquesta en bloque cada una de las interrogantes creando `Headings (H1)` en el docx:
    - **Pregunta 1:** Mide las variaciones de fraudes por Trimestre (`pct_change()`) globalmente.
    - **Pregunta 2:** Secciona según Categoría de Tarjeta evaluando métricas como "Número de Fraudes" vs "Monto Acumulado Fraudulento".
    - **Pregunta 3:** Cruza `profesion` con `estado_civil` y produce una tasa combinada `((seg_fraude / seg_total) * 100)`.
    - **Pregunta 4:** Realiza una advertencia textual, ya que el archivo carece de `DateTime` completo al nivel de hora para validar franjas horarias explícitamente.
    - **Pregunta 5:** Retorna directamente la Desviación y la media `.std()` de la columna monetaria.
    - **Pregunta 6 & 7:** Rastrean Comercio y Mes (Evolución Cronológica mediante un Seaborn Lineplot `sns.lineplot`).
    - **Pregunta 8:** Compara Antigüedad contra Fraude utilizando la columna calculada al inicio.
    - **Pregunta 9:** Cruza el Top 3 de montos pero graficándolo mes con mes y utilizando un `hue='ciudad'` parametrizado.

## Resultados / Salida
Un archivo con un peso moderado (`Reporte_KPIs_Negocio.docx`), almacenado en el sub-directorio `reportes_generados/`, lleno de interpretaciones orientadas a ejecutivos con tablas construidas dinámicamente.
