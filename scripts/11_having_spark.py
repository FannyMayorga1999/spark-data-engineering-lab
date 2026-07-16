import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, sum, avg, count, round

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("HavingEnSpark") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 11: HAVING EN SPARK")
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
df.createOrReplaceTempView("ventas")

print("DataFrame de ventas:")
df.show(20, truncate=False)

print("\n" + "=" * 70)
print("[PASO 2] HAVING en SQL — filtrar grupos")
print("=" * 70)

print("\n--- Regiones con venta total > 3000 ---")
spark.sql("""
    SELECT region, SUM(total) AS suma_total
    FROM ventas
    GROUP BY region
    HAVING SUM(total) > 3000
""").show(truncate=False)

print("\n--- Productos vendidos mas de 20 unidades en total ---")
spark.sql("""
    SELECT producto, SUM(cantidad) AS total_unidades
    FROM ventas
    GROUP BY producto
    HAVING SUM(cantidad) > 20
""").show(truncate=False)

print("\n--- Categorias con promedio de venta > 500 ---")
spark.sql("""
    SELECT categoria, ROUND(AVG(total), 2) AS promedio_venta, COUNT(*) AS num_ventas
    FROM ventas
    GROUP BY categoria
    HAVING AVG(total) > 500
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 3] HAVING con multiples condiciones")
print("=" * 70)

print("\n--- Regiones con venta total > 2000 Y mas de 3 transacciones ---")
spark.sql("""
    SELECT region, SUM(total) AS suma_total, COUNT(*) AS num_ventas
    FROM ventas
    GROUP BY region
    HAVING SUM(total) > 2000 AND COUNT(*) > 3
""").show(truncate=False)

print("\n--- Region + Producto con cantidad total > 5 Y total > 1000 ---")
spark.sql("""
    SELECT region, producto, SUM(cantidad) AS total_unidades, SUM(total) AS suma_total
    FROM ventas
    GROUP BY region, producto
    HAVING SUM(cantidad) > 5 AND SUM(total) > 1000
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 4] HAVING con subconsulta")
print("=" * 70)

print("\n--- Regiones cuya venta promedio supera el promedio general ---")
spark.sql("""
    SELECT region, ROUND(AVG(total), 2) AS promedio_region
    FROM ventas
    GROUP BY region
    HAVING AVG(total) > (SELECT AVG(total) FROM ventas)
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 5] HAVING en DataFrame API — alternativa a filter post-agrupacion")
print("=" * 70)

print("\n--- Usando filter despues de groupBy+agg ---")
df.groupBy("region").agg(
    sum("total").alias("suma_total")
).filter(col("suma_total") > 3000).show(truncate=False)

print("\n--- Usando filter con multiples condiciones ---")
df.groupBy("region", "categoria").agg(
    sum("total").alias("suma_total"),
    count("*").alias("num_ventas")
).filter(
    (col("suma_total") > 1500) & (col("num_ventas") >= 2)
).show(truncate=False)

print("\n--- Comparando: HAVING SQL vs filter en API ---")
print("Ambos producen el mismo resultado:")

print("\nSQL:")
spark.sql("""
    SELECT region, SUM(total) AS suma_total
    FROM ventas
    GROUP BY region
    HAVING SUM(total) > 3000
""").show(truncate=False)

print("DataFrame API (filter post-agg):")
df.groupBy("region").agg(
    sum("total").alias("suma_total")
).filter(col("suma_total") > 3000).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 6] HAVING con ordenamiento")
print("=" * 70)

print("\n--- Regiones con venta > 2000, ordenadas de mayor a menor ---")
spark.sql("""
    SELECT region, SUM(total) AS suma_total
    FROM ventas
    GROUP BY region
    HAVING SUM(total) > 2000
    ORDER BY suma_total DESC
""").show(truncate=False)

print("\n--- Productos con mas de 5 ventas, ordenados por promedio ---")
spark.sql("""
    SELECT producto, COUNT(*) AS num_ventas, ROUND(AVG(total), 2) AS promedio
    FROM ventas
    GROUP BY producto
    HAVING COUNT(*) > 1
    ORDER BY promedio DESC
""").show(truncate=False)

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 11")
print("=" * 70)

spark.stop()
