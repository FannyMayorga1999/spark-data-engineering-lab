# Spark Data Engineering Lab

Laboratorio de aprendizaje de Apache Spark (PySpark) para fundamentos de ingeniería de datos.

## Prerrequisitos

- Python 3.12+
- Java JDK 17 (configurado vía `JAVA_HOME`)
- Hadoop WinUtils (configurado vía `HADOOP_HOME`, necesario en Windows)

## Setup

```bash
# 1. Clonar el repositorio y entrar al directorio
cd spark-data-engineering-lab

# 2. Crear y activar el entorno virtual
python -m venv venv
source venv/Scripts/activate  # En Windows/MinGW
# ó: venv\Scripts\activate.bat  # En Windows CMD

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar variables de entorno (Windows/MinGW)
source setenv.sh
```

## Scripts

| Script | Descripción |
|--------|-------------|
| `scripts/01_hola_spark.py` | Introducción a Spark: creación de DataFrames, filtros, Spark SQL |
| `scripts/02_analisis_ventas.py` | Pipeline de ventas: datos sintéticos, agrupaciones, exportación a Parquet |

## Uso

```bash
python scripts/01_hola_spark.py
python scripts/02_analisis_ventas.py
```

Los archivos Parquet de salida se escriben en `data/output/`.

## Estructura del proyecto

- `scripts/` — Scripts Python con PySpark
- `notebooks/` — (para futuros notebooks Jupyter)
- `data/input/` — (para datasets de entrada)
- `data/output/` — Salida Parquet de los scripts
- `setenv.sh` — Configuración de variables de entorno para Windows
