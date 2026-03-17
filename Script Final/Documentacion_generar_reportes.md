# `generar_reportes.py`

## Propósito
Este módulo en Python es una herramienta generalista enfocada en producir documentos de reporte que exploran estadísticas de gran envergadura (como Fraudes, Demográficos, Operativos y Financieros). Funciona bajo la premisa de extraer gráficas e interpretaciones básicas del dataset extraído por la ETL.

## Dependencias
- `pandas` y `os`/`io` para manipulación general de directorios y estructuras de datos crudas.
- `matplotlib.pyplot` y `seaborn` para generación de los gráficos.
- `python-docx` (`from docx import Document`) para dar formato y estructura en Word.

## Comportamiento
Crea si no existe un directorio llamado `OUTPUT_DIR` (`reportes_generados`).

### Funciones Principales

1. `generar_reporte_fraudes()`: Evalúa incidencias sobre las diferentes pasarelas / canales (ej: POS, Web, App Móvil) y categorizaciones de comercio (ej: Transporte, Tecnología). Genera gráficas asociadas a esas agrupaciones insertando la imagen pre-renderizada devuelta por `add_plot_to_doc()`. 

2. `generar_reporte_demografico()`: Mide métricas sociodemográficas poblacionales de las tablas, validando cantidades a nivel general de los "clientes" filtrándolos y correlacionándolos por Género. Retorna tablas MS-Word en estilo `Table Grid` y salva `Reporte_Demografico_Ejecutivo.docx`.

3. `generar_reporte_operativo()`: Analiza el conteo puro de transacciones y operaciones basadas en las columnas territoriales: `departamento` y `ciudad`. Responde a las inquietudes sobre ¿en dónde se hace más volumen transaccional?, agregando items descriptivos o viñetas usando `p.add_run(f"{ciudad}: ").bold = True`. Guarda `Reporte_Operativo_Ejecutivo.docx`.

4. `generar_reporte_financiero()`: Computa promedios totales en términos absolutos del `monto_total`, las `comisiones` generadas en general al banco y los `impuestos` cruzándolos por nivel de `categoria_tarjeta`.

## Resultados / Salida
Cuatro archivos .docx enfocados que simplifican el flujo comercial dentro de `/reportes_generados/`.
