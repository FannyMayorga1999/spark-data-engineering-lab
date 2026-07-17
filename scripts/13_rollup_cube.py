import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, sum, round, count, lit

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("RollupCube") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 13: ROLLUP Y CUBE")
print("=" * 70)

print("\n[PASO 1] Creando DataFrame de ventas...\n")

schema = StructType([
    StructField("region",     StringType(),  True),
    StructField("producto",   StringType(),  True),
    StructField("categoria",  StringType(),  True),
    StructField("cantidad",   IntegerType(), True),
    StructField("total",      DoubleType(),  True),
])

data = [
    ("Norte",  "Laptop",    "Electronica", 2, 2400.0),
    ("Norte",  "Mouse",     "Electronica", 5, 125.0),
    ("Sur",    "Laptop",    "Electronica", 1, 1200.0),
    ("Sur",    "Teclado",   "Electronica", 3, 135.0),
    ("Este",   "Laptop",    "Electronica", 3, 3600.0),
    ("Este",   "Mouse",     "Electronica", 10, 250.0),
    ("Oeste",  "Teclado",   "Electronica", 4, 180.0),
    ("Oeste",  "Monitor",   "Electronica", 2, 700.0),
    ("Norte",  "Silla",     "Muebles",     3, 450.0),
    ("Sur",    "Escritorio","Muebles",     1, 300.0),
    ("Este",   "Silla",     "Muebles",     5, 750.0),
    ("Oeste",  "Escritorio","Muebles",     2, 600.0),
]

df = spark.createDataFrame(data, schema)

print("DataFrame de ventas:")
df.show(20, truncate=False)

print("\n" + "=" * 70)
print("[PASO 2] ROLLUP — subtotales jerarquicos")
print("=" * 70)

print("\n--- Rollup por region -> producto (totales progresivos) ---")
rollup_df = df.rollup("region", "producto").agg(
    round(sum("total"), 2).alias("total_venta"),
    sum("cantidad").alias("total_cantidad")
).orderBy("region", "producto")

rollup_df.show(truncate=False)

print("Explicacion: null en 'producto' = subtotal de la region")
print("             null en 'region'    = gran total")

print("\n" + "=" * 70)
print("[PASO 3] ROLLUP con 3 columnas — jerarquia completa")
print("=" * 70)

print("\n--- Rollup por region -> categoria -> producto ---")
rollup3 = df.rollup("region", "categoria", "producto").agg(
    round(sum("total"), 2).alias("total_venta")
).orderBy("region", "categoria", "producto")

rollup3.show(30, truncate=False)

print("Filas con null indican totales parciales o gran total")

print("\n" + "=" * 70)
print("[PASO 4] CUBE — todas las combinaciones posibles")
print("=" * 70)

print("\n--- Cube por region y producto ---")
cube_df = df.cube("region", "producto").agg(
    round(sum("total"), 2).alias("total_venta")
).orderBy("region", "producto")

cube_df.show(truncate=False)

print("Diferencia con rollup: cube genera totales por cada columna")
print("independientemente, no solo en jerarquia.")

print("\n" + "=" * 70)
print("[PASO 5] CUBE con 3 columnas")
print("=" * 70)

print("\n--- Cube por region, categoria, producto ---")
cube3 = df.cube("region", "categoria", "producto").agg(
    round(sum("total"), 2).alias("total_venta"),
    count("*").alias("num_registros")
).orderBy("region", "categoria", "producto")

cube3.show(40, truncate=False)

print("\n" + "=" * 70)
print("[PASO 6] ROLLUP vs CUBE — comparacion directa")
print("=" * 70)

print("\n--- Rollup: region, producto ---")
df.rollup("region", "producto").agg(round(sum("total"), 2).alias("total")).orderBy("region", "producto").show(truncate=False)

print("\n--- Cube: region, producto ---")
df.cube("region", "producto").agg(round(sum("total"), 2).alias("total")).orderBy("region", "producto").show(truncate=False)

print("Rollup: nulls aparecen solo en jerarquia (region,null) y (null,null)")
print("Cube:   nulls aparecen en todas las combinaciones: (null,producto), (region,null), (null,null)")

print("\n" + "=" * 70)
print("[PASO 7] Filtrar solo subtotales con grouping()")
print("=" * 70)

from pyspark.sql.functions import grouping, grouping_id

print("\n--- Rollup con columna 'grouping(region)' ---")
df.rollup("region", "producto").agg(
    round(sum("total"), 2).alias("total"),
    grouping("region").alias("grp_region"),
    grouping("producto").alias("grp_producto")
).orderBy("region", "producto").show(truncate=False)

print("grp=1 significa que la columna fue agregada (es subtotal)")

print("\n--- Solo filas de subtotal por region (no gran total) ---")
df.rollup("region", "producto").agg(
    round(sum("total"), 2).alias("total")
).where(grouping("region") == 0).orderBy("region", "producto").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 8] GROUPING SETS — personalizar totales")
print("=" * 70)

print("\n--- Grouping sets manual: (region,producto), (region), (producto), total ---")
df.groupBy("region", "producto").agg(
    round(sum("total"), 2).alias("total")
).rollup("region", "producto").agg(
    round(sum("total"), 2).alias("total")
).createOrReplaceTempView("rollup_view")

spark.sql("""
    SELECT region, producto, round(sum(total), 2) as total
    FROM rollup_view
    GROUP BY region, producto
    GROUPING SETS (
        (region, producto),
        (region),
        (producto),
        ()
    )
    ORDER BY region, producto
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 9] Rollup con SQL")
print("=" * 70)

df.createOrReplaceTempView("ventas")

print("\n--- Rollup via GROUP BY con ROLLUP ---")
spark.sql("""
    SELECT region, producto, round(sum(total), 2) as total_venta
    FROM ventas
    GROUP BY ROLLUP(region, producto)
    ORDER BY region, producto
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 10] Cube con SQL")
print("=" * 70)

print("\n--- Cube via GROUP BY con CUBE ---")
spark.sql("""
    SELECT region, producto, round(sum(total), 2) as total_venta
    FROM ventas
    GROUP BY CUBE(region, producto)
    ORDER BY region, producto
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 11] Comparar numero de filas generadas")
print("=" * 70)

n_regions = df.select("region").distinct().count()
n_productos = df.select("producto").distinct().count()

print(f"Regiones distinct: {n_regions}")
print(f"Productos distinct: {n_productos}")

rollup_rows = df.rollup("region", "producto").count().count()
cube_rows = df.cube("region", "producto").count().count()

print(f"Filas en ROLLUP(region,producto): {rollup_rows}  (esperado: {n_regions + 1} subtotales + {n_regions}*{n_productos} detalle)")
print(f"Filas en CUBE(region,producto):   {cube_rows}  (esperado: {n_regions}*{n_productos} + {n_regions} + {n_productos} + 1)")

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 13")
print("=" * 70)

spark.stop()
