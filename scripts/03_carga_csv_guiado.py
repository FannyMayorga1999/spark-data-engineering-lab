import os
import sys
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("CargaCSV_Guiado") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO GUIADO: CARGA DE CSV Y EXPLORACION BASICA")
print("=" * 70)

# ============================================================
# PASO 1: Crear datos sinteticos y guardarlos como CSV
# ============================================================
# NOTA: Esto simula tener un archivo CSV real.
# En la realidad, usarias spark.read.csv("data/input/tu_archivo.csv")

print("\n[PASO 1] Creando datos de prueba y guardando CSV...\n")

data = [
    ("Alice",   "Ingenieria", 34, 50000.0, "2024-01-15"),
    ("Bob",     "Ventas",     45, 60000.0, "2023-11-20"),
    ("Carlos",  "Ingenieria", 29, 45000.0, "2024-03-10"),
    ("Diana",   "RH",         38, 55000.0, "2023-08-05"),
    ("Eva",     "Ventas",     27, None,     "2024-06-01"),
    ("Frank",   "Ingenieria", None, 52000.0,"2024-02-14"),
    ("Gloria",  "RH",         42, 48000.0, None),
    ("Hector",  "Ventas",     31, 58000.0, "2023-12-01"),
]

# Guardamos como CSV simple (sin cabecera)
with open("data/input/empleados.csv", "w") as f:
    f.write("nombre,departamento,edad,salario,fecha_ingreso\n")
    for row in data:
        valores = [str(v) if v is not None else "" for v in row]
        f.write(",".join(valores) + "\n")

print("  Archivo creado: data/input/empleados.csv")
print("  Contenido:")
with open("data/input/empleados.csv") as f:
    for linea in f:
        print(f"    {linea.strip()}")

# ============================================================
# PASO 2: Cargar CSV
# ============================================================
print("\n" + "=" * 70)
print("[PASO 2] Cargando CSV con spark.read.csv()")
print("=" * 70)

# --------------- METODO 1: SIN INFERSCHEMA ---------------
print("\n--- Metodo 1: Sin inferSchema (todo string) ---")
df1 = spark.read.csv("data/input/empleados.csv", header=True)
print("\nSchema detectado:")
df1.printSchema()
print("\nDatos:")
df1.show()

print("\n  >>> OBSERVA: edad, salario y fecha son STRING")
print("  >>> Spark no sabe que son numeros/fechas porque no le dijimos")

# --------------- METODO 2: CON INFERSCHEMA ---------------
print("\n--- Metodo 2: Con inferSchema=True ---")
df2 = spark.read.csv("data/input/empleados.csv", header=True, inferSchema=True)
print("\nSchema detectado:")
df2.printSchema()
print("\nDatos:")
df2.show()

print("\n  >>> AHORA SI: edad es INTEGER, salario es DOUBLE")
print("  >>> Spark infirio los tipos revisando los datos")
print("  >>> NOTA: fecha_ingreso sigue siendo STRING (inferSchema no detecta fechas)")

# --------------- METODO 3: CON SCHEMA EXPLICITO ---------------
print("\n--- Metodo 3: Schema explicito (recomendado) ---")
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

schema = StructType([
    StructField("nombre",        StringType(),  True),
    StructField("departamento",  StringType(),  True),
    StructField("edad",          IntegerType(), True),
    StructField("salario",       DoubleType(),  True),
    StructField("fecha_ingreso", DateType(),    True),
])

df3 = spark.read.csv("data/input/empleados.csv", header=True, schema=schema)

print("\nSchema definido por nosotros:")
df3.printSchema()
print("\nDatos:")
df3.show()
print("\n  >>> AHORA fecha_ingreso es DATE (tipo correcto)")
print("  >>> NOTA: Los valores nulos (None) se ven como 'null'")
print("  >>> El True en StructField significa 'nullable' (puede ser nulo)")

# ============================================================
# PASO 3: Exploracion basica del DataFrame
# ============================================================
print("\n" + "=" * 70)
print("[PASO 3] Explorando el DataFrame")
print("=" * 70)

print(f"\n  Numero de filas:       {df3.count()}")
print(f"  Numero de columnas:    {len(df3.columns)}")
print(f"  Nombres de columnas:   {df3.columns}")
print(f"\n  Primeras 3 filas:")
df3.show(3, truncate=False)

print(f"\n  Resumen estadistico (solo columnas numericas):")
df3.describe().show()

# ============================================================
# PASO 4: Contar valores nulos
# ============================================================
print("\n" + "=" * 70)
print("[PASO 4] Contando valores nulos por columna")
print("=" * 70)

from pyspark.sql.functions import col, sum as _sum, when, count

print("\n  Metodo 1: Usando SQL nativo")
df3.createOrReplaceTempView("empleados")
spark.sql("""
    SELECT
        COUNT(*) AS total_filas,
        SUM(CASE WHEN nombre IS NULL THEN 1 ELSE 0 END) AS nulos_nombre,
        SUM(CASE WHEN departamento IS NULL THEN 1 ELSE 0 END) AS nulos_depto,
        SUM(CASE WHEN edad IS NULL THEN 1 ELSE 0 END) AS nulos_edad,
        SUM(CASE WHEN salario IS NULL THEN 1 ELSE 0 END) AS nulos_salario,
        SUM(CASE WHEN fecha_ingreso IS NULL THEN 1 ELSE 0 END) AS nulos_fecha
    FROM empleados
""").show()

print("\n  Metodo 2: Usando DataFrame API")
df3.select(
    count("*").alias("total_filas"),
    _sum(when(col("nombre").isNull(), 1).otherwise(0)).alias("nulos_nombre"),
    _sum(when(col("departamento").isNull(), 1).otherwise(0)).alias("nulos_depto"),
    _sum(when(col("edad").isNull(), 1).otherwise(0)).alias("nulos_edad"),
    _sum(when(col("salario").isNull(), 1).otherwise(0)).alias("nulos_salario"),
    _sum(when(col("fecha_ingreso").isNull(), 1).otherwise(0)).alias("nulos_fecha"),
).show()

print("\n  >>> El salario de Eva es nulo, la edad de Frank es nula,")
print("  >>> y la fecha de Gloria es nula.")

# ============================================================
# PASO 5: Tu turno! (completa el codigo)
# ============================================================
print("\n" + "=" * 70)
print("[PASO 5 - EJERCICIO] Completa el codigo debajo")
print("=" * 70)


print("\n--- Ejercicio 1: Filtra empleados con salario > 50000 ---")
df_filtrado = df3.filter(df3.salario > 50000)
df_filtrado.show()

print("\n--- Ejercicio 2: Selecciona solo nombre y departamento ---")
df_seleccion = df3.select("nombre", "departamento")
df_seleccion.show()

print("\n--- Ejercicio 3: Crea columna 'salario_anual' (salario * 12) ---")
df_con_columna = df3.withColumn("salario_anual", df3.salario * 12)
df_con_columna.show()

print("\n--- Ejercicio 4: Agrupa por departamento y calcula salario promedio ---")
df_agrupado = df3.groupBy("departamento").agg({"salario": "avg"})
df_agrupado.show()

print("\n--- Ejercicio 5: Ordena por edad descendente ---")
df_ordenado = df3.orderBy(df3.edad.desc())
df_ordenado.show()

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO GUIADO")
print("=" * 70)

spark.stop()
