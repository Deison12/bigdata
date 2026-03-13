from pathlib import Path
import argparse
import subprocess
import sys


# Este archivo está dentro de /sql
# Por eso la carpeta base real del proyecto es el padre de /sql
BASE_DIR = Path(__file__).resolve().parent.parent

ARCHIVO_DIM_SCRIPT = BASE_DIR / "cargar_dimensiones_precargadas.py"
ARCHIVO_ETL = BASE_DIR / "transformar_y_cargar_eventos_ajustado.py"
ARCHIVO_DATASET = BASE_DIR / "dataset_eventos_50000_v2_sucio.csv"
CARPETA_DIM = BASE_DIR / "plantillas_csv"
SALIDA_ETL = BASE_DIR / "salidas_demo"


def ejecutar_comando(comando: list[str], titulo: str) -> None:
    print("\n" + "=" * 70)
    print(titulo)
    print("=" * 70)
    print("Comando:", " ".join(map(str, comando)))

    resultado = subprocess.run(comando, text=True)

    if resultado.returncode != 0:
        print(f"\n[ERROR] Falló el paso: {titulo}")
        sys.exit(resultado.returncode)


def validar_archivos() -> None:
    faltantes = []

    for ruta in [ARCHIVO_DIM_SCRIPT, ARCHIVO_ETL, ARCHIVO_DATASET]:
        if not ruta.exists():
            faltantes.append(str(ruta))

    if not CARPETA_DIM.exists():
        faltantes.append(str(CARPETA_DIM))

    if faltantes:
        print("[ERROR] Faltan estos archivos o carpetas:")
        for item in faltantes:
            print(" -", item)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ejecutor completo del proyecto ETL para fraude en tarjetas"
    )

    grupo_modo = parser.add_mutually_exclusive_group()
    grupo_modo.add_argument(
        "--cargar-db",
        action="store_true",
        help="Ejecuta el ETL y además carga el resultado final en MySQL."
    )
    grupo_modo.add_argument(
        "--solo-etl",
        action="store_true",
        help="Ejecuta schema + dimensiones + ETL, pero no inserta la salida final en MySQL."
    )

    args = parser.parse_args()

    validar_archivos()

    print("\n[INFO] Iniciando ejecución completa del proyecto...")

    # Crear carpeta de salida si no existe
    SALIDA_ETL.mkdir(parents=True, exist_ok=True)

    # Paso 1 y 2: crear base/tablas y cargar dimensiones precargadas
    ejecutar_comando(
        [
            sys.executable,
            str(ARCHIVO_DIM_SCRIPT),
            "--init-schema",
            "--csv-dir",
            str(CARPETA_DIM)
        ],
        "PASO 1 y 2: Crear base/tablas y cargar dimensiones precargadas"
    )

    # Paso 3: ejecutar ETL
    comando_etl = [
        sys.executable,
        str(ARCHIVO_ETL),
        "--archivo",
        str(ARCHIVO_DATASET),
        "--salida",
        str(SALIDA_ETL)
    ]

    # Modo de ejecución:
    # --cargar-db   => inserta en MySQL
    # --solo-etl    => no inserta en MySQL
    # sin parámetros => por defecto solo transforma/exporta (modo demo)
    if args.cargar_db:
        comando_etl.append("--truncate")
    else:
        comando_etl.append("--no-cargar-db")

    ejecutar_comando(
        comando_etl,
        "PASO 3: Ejecutar ETL del dataset sucio"
    )

    print("\n" + "=" * 70)
    print("[OK] PROCESO COMPLETADO")
    print("=" * 70)
    print("Revisa la carpeta:", SALIDA_ETL)
    print("Archivos esperados:")
    print(" - Dim_Cliente.csv")
    print(" - Dim_Tarjeta.csv")
    print(" - Dim_Tiempo.csv")
    print(" - Dim_Comercio.csv")
    print(" - Dim_Ubicacion.csv")
    print(" - Fact_Transaccion_Tarjeta.csv")
    print(" - Registros_Rechazados.csv")
    print(" - Reporte_Calidad_ETL.md")

    if args.cargar_db:
        print("\nTambién se insertaron los datos en MySQL.")
    else:
        print("\nSe ejecutó en modo demostración: transforma y exporta, pero no inserta la salida final en MySQL.")


if __name__ == "__main__":
    main()