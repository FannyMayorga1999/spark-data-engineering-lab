import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lower, upper

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("FiltrosCondicionales") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 06: FILTROS Y CONDICIONALES")
print("=" * 70)

print("\n[PASO 1] Creando DataFrame de ejemplo...\n")

schema = StructType([
    StructField("id",           IntegerType(), True),
    StructField("nombre",       StringType(),  True),
    StructField("departamento", StringType(),  True),
    StructField("salario",      DoubleType(),  True),
    StructField("edad",         IntegerType(), True),
    StructField("ciudad",       StringType(),  True),
])

data = [
    (1,  "Alice",   "Ingenieria", 50000.0, 34, "Madrid"),
    (2,  "Bob",     "Ventas",     60000.0, 45, "Barcelona"),
    (3,  "Carlos",  "Ingenieria", 45000.0, 29, "Madrid"),
    (4,  "Diana",   "RH",         55000.0, 38, "Valencia"),
    (5,  "Eva",     "Ventas",     62000.0, 27, "Barcelona"),
    (6,  "Frank",   "Ingenieria", 52000.0, 42, "Sevilla"),
    (7,  "Gloria",  "RH",         48000.0, 31, "Madrid"),
    (8,  "Hector",  "Ventas",     58000.0, 36, "Barcelona"),
    (9,  "Irene",   "Marketing",  51000.0, 33, "Valencia"),
    (10, "Juan",    "Marketing",  47000.0, 28, "Sevilla"),
    (11, "Karina",  "Ingenieria", 53000.0, 39, "Madrid"),
    (12, "Luis",    "RH",         49000.0, 41, "Barcelona"),
]

df = spark.createDataFrame(data, schema)

print("DataFrame original:")
df.show(12, truncate=False)

print("\n" + "=" * 70)
print('[PASO 2] filter() y where() — filtrar filas')
print("=" * 70)

print("\nfilter() con expresion de columna:")
df.filter(col("edad") > 35).show(truncate=False)

print("\nwhere() con expresion SQL:")
df.where("salario >= 55000").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 3] Condiciones compuestas: & (and), | (or), ~ (not)")
print("=" * 70)

print("\n& (and): Ingenieria Y salario > 50000:")
df.filter((col("departamento") == "Ingenieria") & (col("salario") > 50000)).show(truncate=False)

print("\n| (or): RH O Marketing:")
df.filter((col("departamento") == "RH") | (col("departamento") == "Marketing")).show(truncate=False)

print("\n~ (not): NO son de Madrid:")
df.filter(~(col("ciudad") == "Madrid")).show(truncate=False)

print("\nCombinacion: (Ingenieria o Ventas) Y salario >= 50000 Y no Madrid:")
df.filter(
    ((col("departamento") == "Ingenieria") | (col("departamento") == "Ventas")) &
    (col("salario") >= 50000) &
    (~(col("ciudad") == "Madrid"))
).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 4] between() — filtrar por rango")
print("=" * 70)

print("\nEdad entre 30 y 40 inclusive:")
df.filter(col("edad").between(30, 40)).show(truncate=False)

print("\nSalario entre 48000 y 53000:")
df.filter(col("salario").between(48000, 53000)).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 5] isin() — filtrar por lista de valores")
print("=" * 70)

print("\nDepartamento en ('Ingenieria', 'Marketing'):")
df.filter(col("departamento").isin("Ingenieria", "Marketing")).show(truncate=False)

print("\nCiudad en ('Madrid', 'Valencia'):")
df.filter(col("ciudad").isin("Madrid", "Valencia")).show(truncate=False)

print("\nNO esta en ('Ingenieria', 'Ventas'):")
df.filter(~col("departamento").isin("Ingenieria", "Ventas")).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 6] startswith(), contains() — filtrar por patron")
print("=" * 70)

print("\nNombre que empieza con 'A' (startswith):")
df.filter(col("nombre").startswith("A")).show(truncate=False)

print("\nNombre que contiene 'e' (contains):")
df.filter(col("nombre").contains("e")).show(truncate=False)

print("\nCiudad que contiene 'el' (contains):")
df.filter(col("ciudad").contains("el")).show(truncate=False)

print("\nNombre que termina con 'a' (endswith):")
df.filter(col("nombre").endswith("a")).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 7] Combinando multiple filtros en pipeline")
print("=" * 70)

print("\nPipeline: Ingenieria, edad >= 30, Madrid:")
df_filtrado = df \
    .filter(col("departamento") == "Ingenieria") \
    .filter(col("edad") >= 30) \
    .filter(col("ciudad") == "Madrid")
df_filtrado.show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 8] Distinct y count con filtros")
print("=" * 70)

print("\nDepartamentos distintos:")
df.select("departamento").distinct().show(truncate=False)

print("\nCantidad de personas por departamento:")
df.groupBy("departamento").count().show(truncate=False)

print("\nCuantos ganan mas de 50000 en cada ciudad:")
df.filter(col("salario") > 50000) \
    .groupBy("ciudad") \
    .count() \
    .show(truncate=False)

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 06")
print("=" * 70)

spark.stop()
