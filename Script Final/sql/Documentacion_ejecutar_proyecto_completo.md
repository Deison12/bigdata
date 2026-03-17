# `sql/ejecutar_proyecto_completo.py`

## Propósito
Éste es un archivo Orquestador central (Pipeline Execution Script). Reúne e invoca los distintos scripts y archivos generados para proveer una forma única, limpia y centralizada de ejecutar la configuración, limpieza y carga de los datos. Simplifica la experiencia del usuario o el analista sin obligarlo a recordar el uso puntual o flags complejos de los módulos por separado.

## Dependencias
- `pathlib` para manejo robusto de rutas agnósticas y relativas (usando el base dir como `parent.parent`).
- `argparse` para aceptar interacciones CLI nativas con modos excluyentes por terminal (`--cargar-db` y `--solo-etl`).
- `subprocess` para creación de una terminal simulada nativa de Python invocando comandos a través del framework principal o Virtual Environment actual referenciado globalmente por `sys.executable`.

## Funciones Principales

### `validar_archivos()`
Examina obligatoriamente si los tres puntos ancla del proyecto están presentes:
1. Las dimensiones (`cargar_dimensiones_precargadas.py`).
2. El Dataset Sucio CSV Original (`dataset_eventos_50000_v2_sucio.csv`).
3. El Ejecutable de Extracción principal (`transformar_y_cargar_eventos_ajustado.py`).
Si alguno de la lista anterior falla en su búsqueda mediante `.exists()`, el macro-proceso general alerta sobre lo que falta, evita disparar errores parciales y aborta la sub-rutina de inmediato haciendo uso de `sys.exit(1)`.

### `ejecutar_comando(comando: list[str], titulo: str)`
Corre nativamente listados de subcomandos (agregando formateo enriquecido y logging al paso terminal ejecutado) usando `.run(comando, text=True)`. Si el script invocado arroja excepciones con código de muerte diferente de "0" (`resultado.returncode != 0`), finaliza el proceso con error preservando la traza hacia la terminal superior.

### `main()`
1. Resuelve las políticas generales de las banderas elegidas (Modos: Sólo exportación de ETL o bien carga final MySQL).
2. Genera o asegura la creación de la carpeta temporal destino (`salidas_demo/`).
3. Construye invocaciones anilladas al subproceso usando el intérprete `sys.executable` con el flag `--init-schema`.
4. Muestra un log descriptivo confirmando en pantalla cuáles archivos o listado de dependencias son esperables que se extraigan en el Output principal tras el paso exitoso por el Pipeline.
