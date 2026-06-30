# Spark Data Engineering Lab

Laboratorio de aprendizaje de Apache Spark (PySpark) para fundamentos de ingeniería de datos.

## Stack tecnológico

| Componente | Versión |
|------------|---------|
| Python | 3.12.10 |
| PySpark | 4.1.2 |
| Apache Spark | 4.1.2 (bundled con PySpark) |
| Apache Hadoop | 3.4.2 (bundled con PySpark) |
| Java (JDK) | Eclipse Adoptium Temurin-17.0.19+10 |
| Hadoop WinUtils | 3.3.6 (necesario en Windows) |
| Sistema Operativo | Windows 10 Pro (Insider Preview 26200) |
| Gestor de paquetes | pip 25.0.1 |

## Prerrequisitos

- **Python 3.12+** — Descargar de [python.org](https://www.python.org/downloads/)
- **Java JDK 17** — Eclipse Adoptium Temurin ([enlace](https://adoptium.net/temurin/releases/))
- **Hadoop WinUtils** — `winutils.exe` y `hadoop.dll` para Hadoop 3.x ([cdarlint/winutils](https://github.com/cdarlint/winutils))
- **Git Bash** (recomendado) o terminal compatible con MinGW

### Variables de entorno requeridas

| Variable | Ruta ejemplo |
|----------|-------------|
| `JAVA_HOME` | `C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot` |
| `HADOOP_HOME` | `C:\hadoop` |
| `PATH` | Debe incluir `%JAVA_HOME%/bin` y `%HADOOP_HOME%/bin` |

## Proyecto

```
spark-data-engineering-lab/
├── scripts/
│   ├── 01_hola_spark.py          # Intro a Spark: DataFrames, filtros, Spark SQL
│   └── 02_analisis_ventas.py     # Pipeline ETL: datos sintéticos, agregaciones, Parquet
├── notebooks/                     # (reservado para Jupyter)
├── data/
│   ├── input/                     # (para datasets de entrada)
│   └── output/                    # Salida Parquet generada por los scripts
├── venv/                          # Entorno virtual Python con PySpark
├── requirements.txt               # Dependencias Python
├── setenv.sh                      # Configuración de entorno (Windows/MinGW)
├── .gitignore
└── README.md
```

## Setup

```bash
# 1. Clonar
cd spark-data-engineering-lab

# 2. Crear y activar entorno virtual
python -m venv venv
source venv/Scripts/activate        # Git Bash / MinGW
# ó: venv\Scripts\activate          # Windows CMD
# ó: venv/Scripts/Activate.ps1      # PowerShell

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar entorno (Java, Hadoop, PATH, venv)
source setenv.sh
```

## Scripts

### 01_hola_spark.py
- Crea un SparkSession
- Genera un DataFrame con nombres y edades
- Filtra registros (`Edad > 30`)
- Ejecuta consultas SQL con tablas temporales
- Cuenta filas

### 02_analisis_ventas.py
- Genera 1000 registros sintéticos de ventas (8 productos, 5 regiones, 2024)
- Define esquema tipado con `StructType`
- Calcula columnas derivadas: `total_venta`, `semana`, `mes`
- Ejecuta consultas SQL de agregación:
  - Ventas totales por producto
  - Ventas por región (DataFrame API)
  - Top 5 productos por mes (Q1 2024)
- Exporta resultados a Parquet (Snappy comprimido)

## Uso

```bash
# Activar entorno primero
source setenv.sh

# Ejecutar scripts
python scripts/01_hola_spark.py
python scripts/02_analisis_ventas.py

source setenv.sh && python scripts/03_carga_csv_guiado.py


# Los archivos Parquet se escriben en:
ls data/output/ventas.parquet/
```

## Dependencias

### Python (requirements.txt)
- `pyspark>=4.0.0`
- `py4j>=0.10.9.7`

### Sistema
- Java JDK 17 (JVM para ejecutar Spark)
- Hadoop WinUtils (`winutils.exe`, `hadoop.dll`) para operaciones nativas en Windows

## Notas para Windows

- El `setenv.sh` usa rutas estilo MinGW (`/c/...`). Ejecutar con `source` en Git Bash.
- Si usas CMD o PowerShell, configura las variables manualmente o usa `setenv.bat` (no incluido).
- `hadoop.dll` debe estar en el `PATH` del sistema para que la JVM lo encuentre.
- El warning `Unable to load native-hadoop library` es normal si no se configura `PATH`.
