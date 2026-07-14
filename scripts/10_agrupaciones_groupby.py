import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, sum, avg, min, max, count, stddev, round

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("AgrupacionesGroupBy") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 10: AGREGACIONES CON groupBy")
print("=" * 70)

print("\n[PASO 1] Creando DataFrame de ventas...\n")

schema = StructType([
    StructField("region",     StringType(),  True),
    StructField("producto",   StringType(),  True),
    StructField("categoria",  StringType(),  True),
    StructField("fecha",      StringType(),  True),
    StructField("cantidad",   IntegerType(), True),
    StructField("precio",     DoubleType(),  True),
    StructField("total",      DoubleType(),  True),
])

data = [
    ("Norte",  "Laptop",    "Electronica",  "2024-01-15", 2, 1200.0, 2400.0),
    ("Norte",  "Mouse",     "Electronica",  "2024-01-15", 5, 25.0,   125.0),
    ("Sur",    "Laptop",    "Electronica",  "2024-01-16", 1, 1200.0, 1200.0),
    ("Sur",    "Teclado",   "Electronica",  "2024-01-16", 3, 45.0,   135.0),
    ("Este",   "Laptop",    "Electronica",  "2024-01-17", 3, 1200.0, 3600.0),
    ("Este",   "Mouse",     "Electronica",  "2024-01-17", 10, 25.0,  250.0),
    ("Oeste",  "Teclado",   "Electronica",  "2024-01-18", 4, 45.0,   180.0),
    ("Oeste",  "Monitor",   "Electronica",  "2024-01-18", 2, 350.0,  700.0),
    ("Norte",  "Silla",     "Muebles",      "2024-01-19", 3, 150.0,  450.0),
    ("Sur",    "Escritorio","Muebles",      "2024-01-19", 1, 300.0,  300.0),
    ("Este",   "Silla",     "Muebles",      "2024-01-20", 5, 150.0,  750.0),
    ("Oeste",  "Escritorio","Muebles",      "2024-01-20", 2, 300.0,  600.0),
    ("Norte",  "Laptop",    "Electronica",  "2024-02-01", 1, 1200.0, 1200.0),
    ("Sur",    "Mouse",     "Electronica",  "2024-02-01", 8, 25.0,   200.0),
    ("Este",   "Teclado",   "Electronica",  "2024-02-02", 6, 45.0,   270.0),
    ("Oeste",  "Laptop",    "Electronica",  "2024-02-02", 2, 1200.0, 2400.0),
    ("Norte",  "Monitor",   "Electronica",  "2024-02-03", 1, 350.0,  350.0),
    ("Sur",    "Silla",     "Muebles",      "2024-02-03", 4, 150.0,  600.0),
    ("Este",   "Escritorio","Muebles",      "2024-02-04", 2, 300.0,  600.0),
    ("Oeste",  "Silla",     "Muebles",      "2024-02-04", 3, 150.0,  450.0),
]

df = spark.createDataFrame(data, schema)

print("DataFrame de ventas:")
df.show(20, truncate=False)

print("\n" + "=" * 70)
print("[PASO 2] groupBy() + count() — contar registros")
print("=" * 70)

print("\nVentas por region:")
df.groupBy("region").count().show(truncate=False)

print("\nVentas por producto:")
df.groupBy("producto").count().orderBy(col("count").desc()).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 3] groupBy() + agg() — multiples agregaciones")
print("=" * 70)

print("\nTotal de ventas por region:")
df.groupBy("region").agg(
    sum("total").alias("suma_total"),
    avg("total").alias("promedio"),
    min("total").alias("minimo"),
    max("total").alias("maximo"),
    count("total").alias("cantidad")
).show(truncate=False)

print("\nEstadisticas por producto:")
df.groupBy("producto").agg(
    sum("cantidad").alias("total_unidades"),
    sum("total").alias("suma_ventas"),
    round(avg("total"), 2).alias("venta_promedio"),
    count("*").alias("num_ventas")
).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 4] groupBy() multiples columnas")
print("=" * 70)

print("\nVentas por region y producto:")
df.groupBy("region", "producto").agg(
    sum("total").alias("suma_total"),
    count("*").alias("num_ventas")
).orderBy("region", "producto").show(truncate=False)

print("\nVentas por region y categoria:")
df.groupBy("region", "categoria").agg(
    sum("total").alias("suma_total"),
    round(avg("total"), 2).alias("promedio")
).orderBy("region").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 5] orderBy() descendente")
print("=" * 70)

print("\nRegiones con mayor venta total:")
df.groupBy("region") \
  .agg(sum("total").alias("suma_total")) \
  .orderBy(col("suma_total").desc()) \
  .show(truncate=False)

print("\nTop 3 productos por cantidad vendida:")
df.groupBy("producto") \
  .agg(sum("cantidad").alias("total_unidades")) \
  .orderBy(col("total_unidades").desc()) \
  .limit(3) \
  .show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 6] stddev() — desviacion estandar")
print("=" * 70)

print("\nDesviacion estandar de ventas por region:")
df.groupBy("region").agg(
    round(avg("total"), 2).alias("promedio"),
    round(stddev("total"), 2).alias("desviacion"),
    count("*").alias("num_ventas")
).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 7] Filtrar grupos con HAVING (en SQL)")
print("=" * 70)

df.createOrReplaceTempView("ventas")

print("\nRegiones con venta total > 3000 (HAVING):")
spark.sql("""
    SELECT region, SUM(total) AS suma_total
    FROM ventas
    GROUP BY region
    HAVING SUM(total) > 3000
""").show(truncate=False)

print("\nProductos vendidos mas de 10 unidades en total:")
spark.sql("""
    SELECT producto, SUM(cantidad) AS total_unidades
    FROM ventas
    GROUP BY producto
    HAVING SUM(cantidad) > 10
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 8] PIVOT — convertir filas en columnas")
print("=" * 70)

print("\nVentas por region y producto (pivot):")
df.groupBy("region").pivot("producto").sum("total").show(truncate=False)

print("\nVentas por categoria y region (pivot):")
df.groupBy("categoria").pivot("region").sum("total").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 9] ROLLUP — subtotales jerarquicos")
print("=" * 70)

print("\nRollup por region y producto (incluye subtotales):")
df.rollup("region", "producto").agg(
    sum("total").alias("suma_total")
).orderBy("region", "producto").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 10] CUBE — todas las combinaciones")
print("=" * 70)

print("\nCube por region y producto (todas las combinaciones):")
df.cube("region", "producto").agg(
    sum("total").alias("suma_total")
).orderBy("region", "producto").show(truncate=False)

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 10")
print("=" * 70)

spark.stop()
