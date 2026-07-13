import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, round, concat, upper, lower, when, length

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("ColumnasDerivadas") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 08: COLUMNAS DERIVADAS")
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
print("[PASO 2] withColumn() — crear columna con operacion aritmetica")
print("=" * 70)

print("\nCrear columna 'salario_anual' = salario * 12:")
df.withColumn("salario_anual", col("salario") * 12).show(truncate=False)

print("\nCrear columna 'doble_salario':")
df.withColumn("doble_salario", col("salario") * 2).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 3] withColumn() con lit() — valor constante")
print("=" * 70)

print("\nAgregar columna 'pais' con valor constante 'Espana':")
df.withColumn("pais", lit("Espana")).show(truncate=False)

print("\nAgregar columna 'activo' con valor constante True:")
df.withColumn("activo", lit(True)).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 4] withColumn() con funciones de string")
print("=" * 70)

print("\nNombre en mayusculas y departamento en minusculas:")
df.withColumn("nombre_upper", upper(col("nombre"))) \
  .withColumn("depto_lower", lower(col("departamento"))) \
  .show(truncate=False)

print("\nLongitud del nombre:")
df.withColumn("nombre_len", length(col("nombre"))).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 5] withColumn() con concat() — combinar columnas")
print("=" * 70)

print("\nCrear 'nombre_completo' concatenando nombre + departamento:")
df.withColumn("info", concat(col("nombre"), lit(" - "), col("departamento"))).show(truncate=False)

print("\nCrear 'etiqueta' con formato personalizado:")
df.withColumn("etiqueta", concat(col("nombre"), lit(" ("), col("ciudad"), lit(")"))).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 6] withColumn() con when() — clasificacion condicional")
print("=" * 70)

print("\nClasificar salario: Alto / Medio / Bajo:")
df.withColumn("clasificacion",
    when(col("salario") >= 58000, "Alto")
    .when(col("salario") >= 50000, "Medio")
    .otherwise("Bajo")
).show(truncate=False)

print("\nClasificar edad: Senior / Junior:")
df.withColumn("nivel",
    when(col("edad") >= 35, "Senior")
    .otherwise("Junior")
).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 7] withColumn() — reemplazar columna existente")
print("=" * 70)

print("\nCrear columna 'salario' redondeado a 0 decimales:")
df.withColumn("salario", round(col("salario"), 0)).show(truncate=False)

print("\nCrear columna 'ciudad' en mayusculas (reemplaza original):")
df.withColumn("ciudad", upper(col("ciudad"))).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 8] Pipeline completo de transformaciones")
print("=" * 70)

print("\nPipeline: crear multiples columnas derivadas:")
df_pipeline = df \
    .withColumn("salario_anual", col("salario") * 12) \
    .withColumn("salario_mensual", round(col("salario") / 12, 2)) \
    .withColumn("info_completa",
        concat(col("nombre"), lit(" | "), col("departamento"), lit(" | "), col("ciudad"))
    ) \
    .withColumn("rango_edad",
        when(col("edad") < 30, "20-29")
        .when(col("edad") < 40, "30-39")
        .otherwise("40+")
    ) \
    .withColumn("pais", lit("Espana"))
df_pipeline.show(12, truncate=False)

print("\nSchema final:")
df_pipeline.printSchema()

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 08")
print("=" * 70)

spark.stop()
