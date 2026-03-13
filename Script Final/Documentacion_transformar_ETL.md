# Documentación de `transformar_y_cargar_eventos_ajustado.py`

## 1. Objetivo del script

Este script toma el dataset crudo del evento y lo convierte en un conjunto de tablas listas para el modelo dimensional del proyecto.

Su función no es solo cargar datos. También debe demostrar la fase **T** del ETL:
- limpiar,
- normalizar,
- homologar,
- calcular campos derivados,
- y separar registros inválidos.

---

## 2. Qué problemas corrige respecto a la versión anterior

La versión anterior todavía asumía que el dataset traía columnas que ya no deben venir desde la fuente:
- `id_transaccion`
- `comision`
- `monto_total`
- `pago_minimo`

Además, la limpieza era muy básica:
- `pd.to_datetime()` directo,
- `pd.to_numeric()` directo,
- y pocas reglas para homologar texto.

Con el dataset sucio eso no era suficiente.

Esta versión corrige eso.

---

## 3. Cambios principales

### 3.1. Ya no espera columnas eliminadas de la fuente
El script ahora trabaja con el dataset nuevo, donde:
- `id_transaccion` no viene en el CSV,
- `comision` no se usa,
- `monto_total` se calcula en el ETL,
- `pago_minimo` se calcula en el ETL.

### 3.2. Limpieza de texto
Se limpian espacios y variantes de escritura.

Ejemplos:
- ` bogotá ` -> `Bogotá`
- `clasica` -> `Clásica`
- `app movil` -> `App móvil`
- `profesor` -> `Docente`

### 3.3. Limpieza de fechas
Acepta varios formatos:
- `YYYY-MM-DD`
- `DD/MM/YYYY`
- `MM-DD-YYYY`

### 3.4. Limpieza de números
Convierte correctamente valores como:
- `$ 5,489,655.00`
- `118950,71`
- ` 8,909,720.40 `

### 3.5. Reglas de negocio en el ETL
Se calculan:

#### `monto_total`
```text
monto_total = monto_transaccion + interes_generado - descuento_aplicado + impuesto - cashback
```

#### `pago_minimo`
```text
pago_minimo = max(valor_fijo_minimo, saldo_posterior * porcentaje_minimo)
```

### 3.6. Registros rechazados
En lugar de dañar todo el proceso por filas malas, el script separa los registros inválidos en un archivo aparte con el motivo del rechazo.

Esto es importante porque el dataset fue diseñado con nulos y suciedad intencional.

---

## 4. Flujo general del script

1. Lee el CSV como texto.
2. Valida columnas esperadas.
3. Limpia texto, fechas y números.
4. Enmascara la tarjeta.
5. Calcula campos derivados.
6. Rechaza registros que quedan imposibles de usar.
7. Busca IDs de dimensiones precargadas.
8. Vuelve a separar registros que no lograron homologarse.
9. Construye dimensiones dinámicas.
10. Construye la tabla de hechos.
11. Exporta CSV transformados.
12. Exporta archivo de rechazados.
13. Genera reporte de calidad.
14. Opcionalmente inserta en MySQL.

---

## 5. Archivos que genera

En la carpeta de salida deja:
- `Dim_Cliente.csv`
- `Dim_Tarjeta.csv`
- `Dim_Tiempo.csv`
- `Dim_Comercio.csv`
- `Dim_Ubicacion.csv`
- `Fact_Transaccion_Tarjeta.csv`
- `Registros_Rechazados.csv`
- `Reporte_Calidad_ETL.md`

---

## 6. Consideración importante sobre la base de datos

Este transformador ya está preparado para el diseño nuevo del proyecto.

Eso significa que el esquema SQL también debería quedar coherente con estas decisiones:
- `id_transaccion` debe ser `AUTO_INCREMENT` en la tabla de hechos,
- `comision` debe salir del esquema si ya no se usará,
- `monto_total` y `pago_minimo` sí deben existir en la fact porque ahora se calculan en el ETL.

---

## 7. Ejemplo de ejecución

Solo transformar y exportar CSV:

```bash
python transformar_y_cargar_eventos_ajustado.py --archivo dataset_eventos_50000_v2_sucio.csv --no-cargar-db
```

Transformar y cargar en MySQL:

```bash
python transformar_y_cargar_eventos_ajustado.py --archivo dataset_eventos_50000_v2_sucio.csv --truncate
```

---

## 8. Resultado esperado

El profesor podrá ver claramente que aquí sí existe una transformación real:
- el dataset fuente viene sucio,
- el ETL lo limpia,
- calcula nuevos campos,
- rechaza registros inválidos,
- y produce tablas listas para el DW.
