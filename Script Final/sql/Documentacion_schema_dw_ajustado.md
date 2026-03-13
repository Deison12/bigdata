# Documentación de `schema_dw_ajustado.sql`

## 1. Objetivo

Este archivo define el esquema dimensional del proyecto de fraude con tarjetas, alineado con la versión nueva del ETL y con el dataset sucio del evento.

---

## 2. Qué cambios se hicieron frente al schema anterior

El schema original todavía tenía dos problemas importantes:
- `id_transaccion` venía como llave primaria manual.
- `comision` todavía existía en la tabla de hechos.

Pero el nuevo diseño del proyecto cambió:
- `id_transaccion` ya no viene en el dataset de entrada.
- `id_transaccion` debe generarlo MySQL automáticamente.
- `comision` ya no hace parte del modelo.
- `monto_total` y `pago_minimo` sí se conservan, porque ahora se calculan dentro del ETL.

---

## 3. Ajustes aplicados

### 3.1. `Fact_Transaccion_Tarjeta`
Se ajustó la tabla de hechos para que:
- `id_transaccion` sea `BIGINT AUTO_INCREMENT PRIMARY KEY`
- se elimine `comision`
- se mantengan:
  - `monto_total`
  - `pago_minimo`

### 3.2. Dimensiones
Las dimensiones se mantienen porque siguen siendo compatibles con el modelo:
- `Dim_Estado_Civil`
- `Dim_Profesion`
- `Dim_Categoria_Tarjeta`
- `Dim_Categoria_Comercio`
- `Dim_Trimestre`
- `Dim_Departamento`
- `Dim_Ciudad`
- `Dim_Cliente`
- `Dim_Tarjeta`
- `Dim_Tiempo`
- `Dim_Comercio`
- `Dim_Ubicacion`

---

## 4. Por qué este cambio era necesario

Si se usaba el schema anterior junto con el nuevo ETL:
- la tabla fact esperaba una columna `comision` que ya no existe
- y esperaba un `id_transaccion` manual que tampoco viene del dataset

Eso iba a causar errores de inserción o inconsistencias entre el ETL y MySQL.

---

## 5. Orden correcto de ejecución

1. Crear la base y tablas con `schema_dw_ajustado.sql`
2. Cargar dimensiones precargadas
3. Ejecutar el ETL ajustado
4. Verificar resultados en CSV o cargar a MySQL

---

## 6. Recomendación práctica

Reemplazar el schema viejo por este nuevo para que:
- el modelo físico
- el ETL
- y el dataset sucio

queden hablando el mismo idioma.
