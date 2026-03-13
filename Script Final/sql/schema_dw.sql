SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

CREATE DATABASE IF NOT EXISTS fraude_tarjetas_dw CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE fraude_tarjetas_dw;

DROP TABLE IF EXISTS Fact_Transaccion_Tarjeta;
DROP TABLE IF EXISTS Dim_Ubicacion;
DROP TABLE IF EXISTS Dim_Ciudad;
DROP TABLE IF EXISTS Dim_Departamento;
DROP TABLE IF EXISTS Dim_Comercio;
DROP TABLE IF EXISTS Dim_Categoria_Comercio;
DROP TABLE IF EXISTS Dim_Tiempo;
DROP TABLE IF EXISTS Dim_Trimestre;
DROP TABLE IF EXISTS Dim_Tarjeta;
DROP TABLE IF EXISTS Dim_Categoria_Tarjeta;
DROP TABLE IF EXISTS Dim_Cliente;
DROP TABLE IF EXISTS Dim_Estado_Civil;
DROP TABLE IF EXISTS Dim_Profesion;

-- =========================================================
-- DIMENSIONES PRECARGADAS
-- =========================================================
CREATE TABLE Dim_Estado_Civil (
    id_estado_civil INT PRIMARY KEY,
    descripcion VARCHAR(50) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE Dim_Profesion (
    id_profesion INT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE,
    sector VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE Dim_Categoria_Tarjeta (
    id_categoria_tarjeta INT PRIMARY KEY,
    nombre_categoria VARCHAR(100) NOT NULL UNIQUE,
    nivel_beneficio VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE Dim_Trimestre (
    id_trimestre INT PRIMARY KEY,
    numero_trimestre INT NOT NULL UNIQUE,
    descripcion VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE Dim_Categoria_Comercio (
    id_categoria_comercio INT PRIMARY KEY,
    nombre_categoria VARCHAR(100) NOT NULL UNIQUE,
    tipo_gasto VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE Dim_Departamento (
    id_departamento INT PRIMARY KEY,
    nombre_departamento VARCHAR(100) NOT NULL,
    pais VARCHAR(100) NOT NULL,
    UNIQUE KEY uk_departamento (nombre_departamento, pais)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE Dim_Ciudad (
    id_ciudad INT PRIMARY KEY,
    nombre_ciudad VARCHAR(100) NOT NULL,
    id_departamento INT NOT NULL,
    CONSTRAINT fk_ciudad_departamento
        FOREIGN KEY (id_departamento) REFERENCES Dim_Departamento(id_departamento),
    UNIQUE KEY uk_ciudad (nombre_ciudad, id_departamento)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- DIMENSIONES DINÁMICAS
-- =========================================================
CREATE TABLE Dim_Cliente (
    id_cliente INT PRIMARY KEY,
    nombre VARCHAR(150) NOT NULL,
    genero VARCHAR(20),
    fecha_nacimiento DATE,
    ingresos_mensuales DECIMAL(14,2),
    score_crediticio INT,
    id_estado_civil INT,
    id_profesion INT,
    CONSTRAINT fk_cliente_estado_civil
        FOREIGN KEY (id_estado_civil) REFERENCES Dim_Estado_Civil(id_estado_civil),
    CONSTRAINT fk_cliente_profesion
        FOREIGN KEY (id_profesion) REFERENCES Dim_Profesion(id_profesion)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE Dim_Tarjeta (
    id_tarjeta INT PRIMARY KEY,
    numero_enmascarado VARCHAR(30) NOT NULL,
    tipo_tarjeta VARCHAR(50),
    fecha_emision DATE,
    fecha_vencimiento DATE,
    limite_credito DECIMAL(14,2),
    tasa_interes DECIMAL(8,4),
    id_categoria_tarjeta INT,
    CONSTRAINT fk_tarjeta_categoria
        FOREIGN KEY (id_categoria_tarjeta) REFERENCES Dim_Categoria_Tarjeta(id_categoria_tarjeta)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE Dim_Tiempo (
    id_tiempo INT PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
    dia INT NOT NULL,
    mes INT NOT NULL,
    anio INT NOT NULL,
    id_trimestre INT NOT NULL,
    CONSTRAINT fk_tiempo_trimestre
        FOREIGN KEY (id_trimestre) REFERENCES Dim_Trimestre(id_trimestre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE Dim_Comercio (
    id_comercio INT PRIMARY KEY,
    nombre_comercio VARCHAR(150) NOT NULL,
    id_categoria_comercio INT NOT NULL,
    CONSTRAINT fk_comercio_categoria
        FOREIGN KEY (id_categoria_comercio) REFERENCES Dim_Categoria_Comercio(id_categoria_comercio),
    UNIQUE KEY uk_comercio (nombre_comercio, id_categoria_comercio)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE Dim_Ubicacion (
    id_ubicacion INT PRIMARY KEY,
    id_ciudad INT NOT NULL,
    CONSTRAINT fk_ubicacion_ciudad
        FOREIGN KEY (id_ciudad) REFERENCES Dim_Ciudad(id_ciudad),
    UNIQUE KEY uk_ubicacion (id_ciudad)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- TABLA DE HECHOS AJUSTADA AL NUEVO ETL
-- =========================================================
CREATE TABLE Fact_Transaccion_Tarjeta (
    id_transaccion BIGINT AUTO_INCREMENT PRIMARY KEY,
    id_cliente INT NOT NULL,
    id_tarjeta INT NOT NULL,
    id_tiempo INT NOT NULL,
    id_comercio INT NOT NULL,
    id_ubicacion INT NOT NULL,
    monto_transaccion DECIMAL(14,2),
    interes_generado DECIMAL(14,2),
    descuento_aplicado DECIMAL(14,2),
    impuesto DECIMAL(14,2),
    monto_total DECIMAL(14,2),
    cuotas INT,
    saldo_anterior DECIMAL(14,2),
    saldo_posterior DECIMAL(14,2),
    pago_minimo DECIMAL(14,2),
    puntos_generados INT,
    cashback DECIMAL(14,2),
    es_fraude BIT,
    estado_transaccion VARCHAR(50),
    canal_transaccion VARCHAR(50),
    CONSTRAINT fk_fact_cliente FOREIGN KEY (id_cliente) REFERENCES Dim_Cliente(id_cliente),
    CONSTRAINT fk_fact_tarjeta FOREIGN KEY (id_tarjeta) REFERENCES Dim_Tarjeta(id_tarjeta),
    CONSTRAINT fk_fact_tiempo FOREIGN KEY (id_tiempo) REFERENCES Dim_Tiempo(id_tiempo),
    CONSTRAINT fk_fact_comercio FOREIGN KEY (id_comercio) REFERENCES Dim_Comercio(id_comercio),
    CONSTRAINT fk_fact_ubicacion FOREIGN KEY (id_ubicacion) REFERENCES Dim_Ubicacion(id_ubicacion)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
