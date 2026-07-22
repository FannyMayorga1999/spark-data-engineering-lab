import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, row_number, rank, dense_rank, round, sum, count
from pyspark.sql.window import Window

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("WindowRanking") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 19: ROW_NUMBER, RANK, DENSE_RANK")
print("=" * 70)

print("\n[PASO 1] Creando DataFrame de ventas...\n")

schema_ventas = StructType([
    StructField("producto",   StringType(),  True),
    StructField("region",     StringType(),  True),
    StructField("mes",        IntegerType(), True),
    StructField("total",      DoubleType(),  True),
])

data_ventas = [
    ("Laptop",   "Norte", 1, 5000.0),
    ("Laptop",   "Sur",   1, 4500.0),
    ("Laptop",   "Este",  1, 5000.0),
    ("Laptop",   "Oeste", 1, 3800.0),
    ("Mouse",    "Norte", 1, 1200.0),
    ("Mouse",    "Sur",   1, 1500.0),
    ("Mouse",    "Este",  1, 1500.0),
    ("Mouse",    "Oeste", 1,  900.0),
    ("Teclado",  "Norte", 1,  800.0),
    ("Teclado",  "Sur",   1, 1100.0),
    ("Teclado",  "Este",  1,  950.0),
    ("Teclado",  "Oeste", 1, 1100.0),
    ("Laptop",   "Norte", 2, 5500.0),
    ("Laptop",   "Sur",   2, 4800.0),
    ("Laptop",   "Este",  2, 5200.0),
    ("Laptop",   "Oeste", 2, 4100.0),
    ("Mouse",    "Norte", 2, 1300.0),
    ("Mouse",    "Sur",   2, 1600.0),
    ("Mouse",    "Este",  2, 1400.0),
    ("Mouse",    "Oeste", 2,  950.0),
    ("Teclado",  "Norte", 2,  850.0),
    ("Teclado",  "Sur",   2, 1150.0),
    ("Teclado",  "Este",  2, 1000.0),
    ("Teclado",  "Oeste", 2, 1200.0),
]

ventas = spark.createDataFrame(data_ventas, schema_ventas)

print("Ventas por producto, region y mes:")
ventas.show(30, truncate=False)

print("\n" + "=" * 70)
print("[PASO 2] Definir ventana (Window)")
print("=" * 70)

window_categoria = Window.partitionBy("producto").orderBy(col("total").desc())

print("Ventana: particionar por producto, ordenar por total descendente.")
print("Esto crea un 'grupo' por producto donde cada fila tiene un numero de posicion.")

print("\n" + "=" * 70)
print("[PASO 3] ROW_NUMBER — numero unico por fila")
print("=" * 70)

df_row_number = ventas.withColumn("row_num", row_number().over(window_categoria))

print("--- Row numbers por producto ---")
df_row_number.orderBy("producto", "row_num").show(30, truncate=False)

print("ROW_NUMBER asigna 1, 2, 3... sin importar empates.")
print("Cada fila tiene un numero UNICO.")

print("\n" + "=" * 70)
print("[PASO 4] RANK — ranking con gaps en empates")
print("=" * 70)

df_rank = ventas.withColumn("r", rank().over(window_categoria))

print("--- Rank por producto ---")
df_rank.orderBy("producto", "r").show(30, truncate=False)

print("RANK asigna el MISMO numero a empates, pero DEJA GAPS.")
print("Ejemplo: si dos filas tienen rank 1, la siguiente tiene rank 3 (no 2).")

print("\n" + "=" * 70)
print("[PASO 5] DENSE_RANK — ranking sin gaps")
print("=" * 70)

df_dense = ventas.withColumn("dr", dense_rank().over(window_categoria))

print("--- Dense Rank por producto ---")
df_dense.orderBy("producto", "dr").show(30, truncate=False)

print("DENSE_RANK asigna el MISMO numero a empates, pero SIN GAPS.")
print("Ejemplo: si dos filas tienen dense_rank 1, la siguiente tiene dense_rank 2.")

print("\n" + "=" * 70)
print("[PASO 6] Comparacion lado a lado")
print("=" * 70)

comparacion = ventas.withColumn("rn", row_number().over(window_categoria)) \
    .withColumn("rk", rank().over(window_categoria)) \
    .withColumn("dr", dense_rank().over(window_categoria))

print("--- Comparacion: row_number vs rank vs dense_rank ---")
comparacion.where(col("producto") == "Laptop").orderBy("rn").show(truncate=False)

print("""
DIFERENCLAVE:
- ROW_NUMBER: siempre unico (1, 2, 3, 4) - no hay empates
- RANK:       empates comparten valor, luego SALTA (1, 1, 3, 4)
- DENSE_RANK: empates comparten valor, SIN salto (1, 1, 2, 3)
""")

print("\n" + "=" * 70)
print("[PASO 7] Top-N: Los 2 productos mas vendidos por region")
print("=" * 70)

window_region = Window.partitionBy("region").orderBy(col("total").desc())

top_n = ventas.withColumn("rank", rank().over(window_region)) \
    .where(col("rank") <= 2) \
    .select("region", "producto", "total", "rank") \
    .orderBy("region", "rank")

print("--- Top 2 por region ---")
top_n.show(20, truncate=False)

print("Pattern: window + rank() + filter rank <= N es la forma clasica de Top-N.")

print("\n" + "=" * 70)
print("[PASO 8] Top-1: Producto con mayor venta por mes")
print("=" * 70)

window_mes = Window.partitionBy("mes").orderBy(col("total").desc())

top1 = ventas.withColumn("rk", rank().over(window_mes)) \
    .where(col("rk") == 1) \
    .select("mes", "producto", "region", "total")

print("--- Producto #1 por mes ---")
top1.show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 9] Filas con empates detectadas")
print("=" * 70)

print("\n--- Detectar si hay multiples regiones con el mismo total ---")
duplicados = comparacion.where(col("rk") != col("rn"))
print("Filas donde rk != rn (hay empates):")
duplicados.show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 10] Window functions en SQL")
print("=" * 70)

ventas.createOrReplaceTempView("v_ventas")

print("\n--- Top 2 productos por region (SQL) ---")
spark.sql("""
    SELECT region, producto, total, rk
    FROM (
        SELECT *,
               RANK() OVER (PARTITION BY region ORDER BY total DESC) as rk
        FROM v_ventas
    )
    WHERE rk <= 2
    ORDER BY region, rk
""").show(20, truncate=False)

print("\n--- Diferencia entre RANK y DENSE_RANK (SQL) ---")
spark.sql("""
    SELECT producto, region, total,
           RANK()       OVER (PARTITION BY producto ORDER BY total DESC) as r,
           DENSE_RANK() OVER (PARTITION BY producto ORDER BY total DESC) as dr,
           ROW_NUMBER() OVER (PARTITION BY producto ORDER BY total DESC) as rn
    FROM v_ventas
    WHERE producto = 'Laptop'
    ORDER BY r
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 11] Ranking con multiples columnas de orden")
print("=" * 70)

window_multi = Window.partitionBy("producto").orderBy(col("total").desc(), col("region").asc())

print("--- Ranking por total DESC, luego region ASC ---")
ventas.withColumn("rk", rank().over(window_multi)) \
    .where(col("producto") == "Teclado") \
    .orderBy("rk").show(truncate=False)

print("Cuando hay empates en total, se usa region como criterio de desempate.")

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 19")
print("=" * 70)

spark.stop()
