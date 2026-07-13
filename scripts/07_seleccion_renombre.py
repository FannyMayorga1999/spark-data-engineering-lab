import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("SeleccionRenombra") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 07: SELECCION Y RENOMBRE DE COLUMNAS")
print("=" * 70)

print("\n[PASO 1] Creando DataFrame de ejemplo...\n")

schema = StructType([
    StructField("id",            IntegerType(), True),
    StructField("nombre",        StringType(),  True),
    StructField("departamento",  StringType(),  True),
    StructField("salario",       DoubleType(),  True),
    StructField("edad",          IntegerType(), True),
    StructField("ciudad",        StringType(),  True),
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
print("[PASO 2] select() — elegir columnas especificas")
print("=" * 70)

print("\nSeleccionar solo nombre y salario:")
df.select("nombre", "salario").show(truncate=False)

print("\nSeleccionar con string de columna:")
df.select("nombre", "departamento", "ciudad").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 3] select() con col() — renombrar al seleccionar")
print("=" * 70)

print("\nRenombrar 'nombre' a 'empleado' y 'salario' a 'sueldo':")
df.select(
    col("nombre").alias("empleado"),
    col("departamento").alias("depto"),
    col("salario").alias("sueldo")
).show(truncate=False)

print("\nSeleccionar y calcular con alias:")
df.select(
    col("nombre"),
    col("salario"),
    (col("salario") * 1.1).alias("salario_con_aumento")
).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 4] withColumnRenamed() — renombrar columnas")
print("=" * 70)

print("\nRenombrar 'departamento' a 'depto' y 'ciudad' a 'lugar':")
df_renombrado = df.withColumnRenamed("departamento", "depto") \
                   .withColumnRenamed("ciudad", "lugar")
df_renombrado.show(12, truncate=False)

print("\nSchema despues de renombrar:")
df_renombrado.printSchema()

print("\n" + "=" * 70)
print("[PASO 5] drop() — eliminar columnas")
print("=" * 70)

print("\nEliminar columna 'edad' y 'ciudad':")
df_sin_edad = df.drop("edad", "ciudad")
df_sin_edad.show(truncate=False)

print("\nEliminar columna 'id':")
df.drop("id").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 6] Seleccionar columnas con alias + renombrar + drop")
print("=" * 70)

print("\nPipeline completo:")
df_resultado = df \
    .select(
        col("id").alias("employee_id"),
        col("nombre").alias("first_name"),
        col("departamento").alias("department"),
        col("salario").alias("salary"),
        col("ciudad").alias("city")
    ) \
    .drop("employee_id")
df_resultado.show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 7] Seleccionar columnas unicas y contar")
print("=" * 70)

print("\nColumnas del DataFrame original:")
print(df.columns)

print("\nColumnas del DataFrame resultado:")
print(df_resultado.columns)

print("\nCantidad de columnas:", len(df.columns))

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 07")
print("=" * 70)

spark.stop()
