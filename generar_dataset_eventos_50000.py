# generar_dataset_eventos_50000.py
# ------------------------------------------------------------
# Genera un dataset crudo de eventos/transacciones con 50.000
# registros, compatible con transformar_y_cargar_eventos.py
# ------------------------------------------------------------

import csv
import random
from datetime import date, timedelta

random.seed(42)

# ------------------------------------------------------------
# CONFIGURACIÓN GENERAL
# ------------------------------------------------------------
TOTAL_REGISTROS = 50000
ARCHIVO_SALIDA = "dataset_eventos_50000.csv"

# ------------------------------------------------------------
# CATÁLOGOS CONSISTENTES CON LAS DIMENSIONES PRECARGADAS
# ------------------------------------------------------------
ESTADOS_CIVILES = ["Soltero", "Casado", "Divorciado", "Viudo"]
PROFESIONES = ["Ingeniero", "Abogado", "Comerciante", "Docente"]
CATEGORIAS_TARJETA = ["Clásica", "Gold", "Platinum"]

# Solo usamos ciudades/departamentos que ya vienen en el proyecto
UBICACIONES = [
    ("Antioquia", "Medellín"),
    ("Cundinamarca", "Bogotá"),
    ("Valle del Cauca", "Cali"),
]

COMERCIOS = {
    "Supermercado": ["Éxito", "Jumbo", "D1", "Ara", "Carulla"],
    "Tecnología": ["Alkosto", "Ktronix", "Falabella Tech", "Tecno Plaza", "Digital Center"],
    "Restaurante": ["Crepes & Waffles", "El Corral", "Frisby", "McDonald's", "La Brasa Roja"],
    "Transporte": ["Cabify", "Uber", "Metro", "Taxis Libres", "JetSMART"],
}

CANALES = ["App móvil", "Web", "POS"]

NOMBRES_M = [
    "Carlos", "Juan", "Luis", "Andrés", "Mateo", "Sebastián", "Santiago", "Daniel",
    "David", "Julián", "Felipe", "Camilo", "Nicolás", "Miguel", "Alejandro", "Esteban"
]

NOMBRES_F = [
    "Laura", "María", "Ana", "Paula", "Valentina", "Sofía", "Camila", "Daniela",
    "Juliana", "Natalia", "Andrea", "Carolina", "Luisa", "Sara", "Isabela", "Catalina"
]

APELLIDOS = [
    "Gómez", "Pérez", "Rodríguez", "López", "Martínez", "Ramírez", "Hernández",
    "Torres", "Sánchez", "Moreno", "Díaz", "Vargas", "Castro", "Ortiz",
    "Suárez", "Rojas", "Jiménez", "Restrepo", "Cárdenas", "Giraldo"
]

# ------------------------------------------------------------
# ESTRUCTURA DEL DATASET
# ------------------------------------------------------------
COLUMNAS = [
    "id_transaccion",
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
    "comision",
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

# ------------------------------------------------------------
# FUNCIONES AUXILIARES
# ------------------------------------------------------------
def fecha_aleatoria(inicio: date, fin: date) -> date:
    dias = (fin - inicio).days
    return inicio + timedelta(days=random.randint(0, dias))


def generar_nombre(genero: str) -> str:
    nombre = random.choice(NOMBRES_M if genero == "Masculino" else NOMBRES_F)
    apellido1 = random.choice(APELLIDOS)
    apellido2 = random.choice(APELLIDOS)
    return f"{nombre} {apellido1} {apellido2}"


def generar_numero_tarjeta() -> str:
    primer_digito = random.choice(["4", "5"])
    resto = "".join(str(random.randint(0, 9)) for _ in range(15))
    return primer_digito + resto


def construir_catalogo_comercios():
    catalogo = []
    for categoria, nombres in COMERCIOS.items():
        for nombre in nombres:
            depto, ciudad = random.choice(UBICACIONES)
            catalogo.append({
                "nombre_comercio": nombre,
                "categoria_comercio": categoria,
                "departamento": depto,
                "ciudad": ciudad
            })
    return catalogo


# ------------------------------------------------------------
# CREAR CLIENTES Y TARJETAS BASE
# ------------------------------------------------------------
def generar_clientes_y_tarjetas(total_clientes=9000):
    clientes = []
    tarjetas = []

    fecha_nac_inicio = date(1958, 1, 1)
    fecha_nac_fin = date(2004, 12, 31)

    for i in range(total_clientes):
        genero = random.choice(["Masculino", "Femenino"])
        nombre_cliente = generar_nombre(genero)

        fecha_nacimiento = fecha_aleatoria(fecha_nac_inicio, fecha_nac_fin)
        estado_civil = random.choices(
            ESTADOS_CIVILES,
            weights=[0.50, 0.35, 0.10, 0.05]
        )[0]

        profesion = random.choices(
            PROFESIONES,
            weights=[0.40, 0.15, 0.25, 0.20]
        )[0]

        departamento, ciudad = random.choices(
            UBICACIONES,
            weights=[0.35, 0.45, 0.20]
        )[0]

        ingresos = {
            "Ingeniero": random.randint(3500000, 12000000),
            "Abogado": random.randint(3000000, 10000000),
            "Comerciante": random.randint(1800000, 9000000),
            "Docente": random.randint(2200000, 7000000),
        }[profesion]

        score = max(450, min(900, int(random.gauss(680 + (ingresos / 1000000) * 8, 70))))

        cliente = {
            "nombre_cliente": nombre_cliente,
            "genero": genero,
            "fecha_nacimiento": fecha_nacimiento.isoformat(),
            "ingresos_mensuales": ingresos,
            "score_crediticio": score,
            "estado_civil": estado_civil,
            "profesion": profesion,
            "departamento_cliente": departamento,
            "ciudad_cliente": ciudad,
        }

        clientes.append(cliente)

        # Cada cliente tendrá 1 o 2 tarjetas
        cantidad_tarjetas = 1 if random.random() < 0.72 else 2

        for _ in range(cantidad_tarjetas):
            if score >= 760 and ingresos >= 6000000:
                categoria_tarjeta = random.choices(CATEGORIAS_TARJETA, weights=[0.05, 0.35, 0.60])[0]
            elif score >= 680 and ingresos >= 3500000:
                categoria_tarjeta = random.choices(CATEGORIAS_TARJETA, weights=[0.20, 0.65, 0.15])[0]
            else:
                categoria_tarjeta = random.choices(CATEGORIAS_TARJETA, weights=[0.75, 0.22, 0.03])[0]

            limite_credito = {
                "Clásica": random.randint(1000000, 7000000),
                "Gold": random.randint(5000000, 18000000),
                "Platinum": random.randint(12000000, 40000000),
            }[categoria_tarjeta]

            fecha_emision = date(random.randint(2021, 2025), random.randint(1, 12), random.randint(1, 28))
            fecha_vencimiento = date(fecha_emision.year + 5, fecha_emision.month, min(fecha_emision.day, 28))

            tarjeta = {
                "indice_cliente": i,
                "numero_tarjeta": generar_numero_tarjeta(),
                "tipo_tarjeta": "Crédito",
                "fecha_emision": fecha_emision.isoformat(),
                "fecha_vencimiento": fecha_vencimiento.isoformat(),
                "limite_credito": limite_credito,
                "tasa_interes": round(random.uniform(1.20, 2.80), 2),
                "categoria_tarjeta": categoria_tarjeta,
            }

            tarjetas.append(tarjeta)

    return clientes, tarjetas


# ------------------------------------------------------------
# GENERAR EVENTOS
# ------------------------------------------------------------
def generar_dataset():
    clientes, tarjetas = generar_clientes_y_tarjetas()
    catalogo_comercios = construir_catalogo_comercios()

    fecha_evento_inicio = date(2025, 1, 1)
    fecha_evento_fin = date(2025, 12, 31)

    total_fraude = 0

    with open(ARCHIVO_SALIDA, "w", newline="", encoding="utf-8-sig") as archivo:
        writer = csv.DictWriter(archivo, fieldnames=COLUMNAS)
        writer.writeheader()

        for id_tx in range(1000001, 1000001 + TOTAL_REGISTROS):
            tarjeta = random.choice(tarjetas)
            cliente = clientes[tarjeta["indice_cliente"]]
            comercio = random.choice(catalogo_comercios)

            fecha_transaccion = fecha_aleatoria(fecha_evento_inicio, fecha_evento_fin)
            canal = random.choices(CANALES, weights=[0.35, 0.20, 0.45])[0]

            monto_base = {
                "Supermercado": random.uniform(25000, 600000),
                "Tecnología": random.uniform(150000, 4500000),
                "Restaurante": random.uniform(18000, 250000),
                "Transporte": random.uniform(8000, 180000),
            }[comercio["categoria_comercio"]]

            multiplicador_tarjeta = {
                "Clásica": 0.90,
                "Gold": 1.10,
                "Platinum": 1.35,
            }[tarjeta["categoria_tarjeta"]]

            monto_transaccion = round(monto_base * multiplicador_tarjeta, 2)

            if comercio["categoria_comercio"] == "Tecnología":
                cuotas = random.choices([1, 3, 6, 12, 18, 24], weights=[0.30, 0.15, 0.18, 0.20, 0.10, 0.07])[0]
            elif comercio["categoria_comercio"] == "Supermercado":
                cuotas = random.choices([1, 2, 3], weights=[0.85, 0.10, 0.05])[0]
            elif comercio["categoria_comercio"] == "Restaurante":
                cuotas = random.choices([1, 2, 3], weights=[0.90, 0.08, 0.02])[0]
            else:
                cuotas = random.choices([1, 2, 3, 6], weights=[0.75, 0.10, 0.10, 0.05])[0]

            saldo_anterior = round(random.uniform(0, tarjeta["limite_credito"] * 0.85), 2)
            interes_generado = round((saldo_anterior * (tarjeta["tasa_interes"] / 100)) if cuotas > 1 else 0, 2)
            comision = round(monto_transaccion * random.uniform(0.005, 0.025), 2)
            descuento_aplicado = round(monto_transaccion * random.choice([0, 0, 0, 0.03, 0.05, 0.08]), 2)
            impuesto = round((monto_transaccion - descuento_aplicado + comision + interes_generado) * 0.19, 2)
            cashback = round(monto_transaccion * random.choice([0, 0, 0.005, 0.01, 0.015]), 2)
            puntos_generados = int(monto_transaccion // 1000)

            # Lógica simple de fraude
            prob_fraude = 0.015

            if comercio["categoria_comercio"] == "Tecnología":
                prob_fraude += 0.020
            if canal in ["Web", "App móvil"]:
                prob_fraude += 0.010
            if monto_transaccion > 2000000:
                prob_fraude += 0.020
            if cliente["score_crediticio"] < 580:
                prob_fraude += 0.010
            if comercio["ciudad"] != cliente["ciudad_cliente"]:
                prob_fraude += 0.005

            es_fraude = 1 if random.random() < prob_fraude else 0

            if es_fraude == 1:
                total_fraude += 1
                estado_transaccion = random.choices(
                    ["En revisión", "Rechazada", "Aprobada"],
                    weights=[0.55, 0.25, 0.20]
                )[0]
                descuento_aplicado = round(descuento_aplicado * 0.40, 2)
                cashback = round(cashback * 0.30, 2)
            else:
                estado_transaccion = random.choices(
                    ["Aprobada", "En revisión", "Rechazada"],
                    weights=[0.93, 0.05, 0.02]
                )[0]

            monto_total = round(
                monto_transaccion + interes_generado + comision - descuento_aplicado + impuesto - cashback,
                2
            )

            saldo_posterior = round(
                min(tarjeta["limite_credito"], saldo_anterior + monto_total),
                2
            )

            pago_minimo = round(max(30000, saldo_posterior * random.uniform(0.04, 0.12)), 2)

            fila = {
                
                "nombre_cliente": cliente["nombre_cliente"],
                "genero": cliente["genero"],
                "fecha_nacimiento": cliente["fecha_nacimiento"],
                "ingresos_mensuales": cliente["ingresos_mensuales"],
 
                "estado_civil": cliente["estado_civil"],
                "profesion": cliente["profesion"],
                "numero_tarjeta": tarjeta["numero_tarjeta"],
                cvv
                "tipo_tarjeta": tarjeta["tipo_tarjeta"],
                "fecha_emision": tarjeta["fecha_emision"],
                "fecha_vencimiento": tarjeta["fecha_vencimiento"],
                "limite_credito": tarjeta["limite_credito"],
                "tasa_interes": tarjeta["tasa_interes"], de la transcioon o de la tarjeta tipo es
                "categoria_tarjeta": tarjeta["categoria_tarjeta"],
                "fecha_transaccion": fecha_transaccion.isoformat(),
                "nombre_comercio": comercio["nombre_comercio"],
                "categoria_comercio": comercio["categoria_comercio"],
                "departamento": comercio["departamento"],
                "ciudad": comercio["ciudad"],
                "monto_transaccion": f"{monto_transaccion:.2f}",
                "interes_generado": f"{interes_generado:.2f}",
                "comision": f"{comision:.2f}",
                "descuento_aplicado": f"{descuento_aplicado:.2f}",
                "impuesto": f"{impuesto:.2f}",
                "monto_total": f"{monto_total:.2f}",
                "cuotas": cuotas,
                "saldo_anterior": f"{saldo_anterior:.2f}",
                "saldo_posterior": f"{saldo_posterior:.2f}",
                "pago_minimo": f"{pago_minimo:.2f}",
                "puntos_generados": puntos_generados,
                "cashback": f"{cashback:.2f}",
                "es_fraude": es_fraude,
                "estado_transaccion": estado_transaccion,
                "canal_transaccion": canal,
            }

            writer.writerow(fila)

    tasa_fraude = (total_fraude / TOTAL_REGISTROS) * 100

    print("Archivo generado correctamente:", ARCHIVO_SALIDA)
    print("Total de registros:", TOTAL_REGISTROS)
    print("Total fraudes simulados:", total_fraude)
    print(f"Tasa de fraude aproximada: {tasa_fraude:.2f}%")

# ------------------------------------------------------------
# EJECUCIÓN
# ------------------------------------------------------------
if __name__ == "__main__":
    generar_dataset()