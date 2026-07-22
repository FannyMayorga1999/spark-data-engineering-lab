import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lag, lead, round, sum, avg, abs, datediff, to_date
from pyspark.sql.window import Window

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("LagLead") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 20: LAG Y LEAD")
print("=" * 70)

print("\n[PASO 1] Creando DataFrame de ventas mensuales...\n")

schema_ventas = StructType([
    StructField("producto",   StringType(),  True),
    StructField("mes",        IntegerType(), True),
    StructField("total",      DoubleType(),  True),
])

data_ventas = [
    ("Laptop",   1, 5000.0),
    ("Laptop",   2, 5500.0),
    ("Laptop",   3, 4800.0),
    ("Laptop",   4, 6200.0),
    ("Laptop",   5, 5900.0),
    ("Laptop",   6, 7100.0),
    ("Mouse",    1, 1200.0),
    ("Mouse",    2, 1500.0),
    ("Mouse",    3, 1100.0),
    ("Mouse",    4, 1800.0),
    ("Mouse",    5, 1600.0),
    ("Mouse",    6, 2000.0),
    ("Teclado",  1,  800.0),
    ("Teclado",  2,  950.0),
    ("Teclado",  3, 1100.0),
    ("Teclado",  4, 1000.0),
    ("Teclado",  5, 1200.0),
    ("Teclado",  6, 1350.0),
]

ventas = spark.createDataFrame(data_ventas, schema_ventas)

print("Ventas mensuales por producto:")
ventas.orderBy("producto", "mes").show(20, truncate=False)

print("\n" + "=" * 70)
print("[PASO 2] LAG — acceder a la fila ANTERIOR")
print("=" * 70)

window_producto = Window.partitionBy("producto").orderBy("mes")

df_lag = ventas.withColumn("venta_anterior", lag("total", 1).over(window_producto))

print("--- LAG(total, 1): venta del mes anterior ---")
df_lag.orderBy("producto", "mes").show(20, truncate=False)

print("LAG(n) accede a la fila n posiciones ATRAS en la ventana.")
print("El primer mes de cada producto tiene NULL (no hay mes anterior).")

print("\n" + "=" * 70)
print("[PASO 3] LAG con offset personalizado")
print("=" * 70)

df_lag2 = ventas.withColumn("venta_hace_2_meses", lag("total", 2).over(window_producto))

print("--- LAG(total, 2): venta hace 2 meses ---")
df_lag2.orderBy("producto", "mes").show(20, truncate=False)

print("Los primeros 2 meses de cada producto tienen NULL.")

print("\n" + "=" * 70)
print("[PASO 4] LEAD — acceder a la fila SIGUIENTE")
print("=" * 70)

df_lead = ventas.withColumn("venta_siguiente", lead("total", 1).over(window_producto))

print("--- LEAD(total, 1): venta del mes siguiente ---")
df_lead.orderBy("producto", "mes").show(20, truncate=False)

print("LEAD(n) accede a la fila n posiciones ADELANTE.")
print("El ultimo mes de cada producto tiene NULL (no hay mes siguiente).")

print("\n" + "=" * 70)
print("[PASO 5] Diferencia mes a mes (cambio absoluto)")
print("=" * 70)

df_diff = ventas.withColumn("venta_anterior", lag("total", 1).over(window_producto)) \
    .withColumn("diferencia", col("total") - col("venta_anterior")) \
    .withColumn("pct_cambio", round((col("diferencia") / col("venta_anterior")) * 100, 2))

print("--- Diferencia y % de cambio mes a mes ---")
df_diff.orderBy("producto", "mes").show(20, truncate=False)

print("diferencia > 0 = crecimiento, diferencia < 0 = decrecimiento.")

print("\n" + "=" * 70)
print("[PASO 6] Deteccion de anomalias con LAG")
print("=" * 70)

print("\n--- Meses con caida > 10% ---")
df_diff.where(col("pct_cambio") < -10).show(truncate=False)

print("Patron util: detectar meses con caidas significativas.")

print("\n" + "=" * 70)
print("[PASO 7] LAG y LEAD juntos — comparar con ambos lados")
print("=" * 70)

df_both = ventas.withColumn("mes_anterior", lag("total", 1).over(window_producto)) \
    .withColumn("mes_siguiente", lead("total", 1).over(window_producto)) \
    .withColumn("promedio_vecinos", round((col("mes_anterior") + col("mes_siguiente")) / 2, 2))

print("--- Ventana de 3 meses (anterior, actual, siguiente) ---")
df_both.where(col("producto") == "Laptop").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 8] LEAD con valor por defecto (default)")
print("=" * 70)

df_lead_default = ventas.withColumn(
    "venta_sig", lead("total", 1, 0.0).over(window_producto)
)

print("--- LEAD con default=0.0 en vez de NULL ---")
df_lead_default.where(col("producto") == "Mouse").orderBy("mes").show(truncate=False)

print("El tercer parametro de lead() es el valor por defecto para NULLs.")

print("\n" + "=" * 70)
print("[PASO 9] Media movil de 3 periodos con LAG/LEAD")
print("\n" + "=" * 70)

df_media = ventas.withColumn("m1", lag("total", 1).over(window_producto)) \
    .withColumn("m2", lag("total", 2).over(window_producto)) \
    .withColumn("media_3m", round((col("m2") + col("m1") + col("total")) / 3, 2))

print("--- Media movil de 3 meses ---")
df_media.where(col("producto") == "Laptop").orderBy("mes").show(truncate=False)

print("Requiere al menos 3 meses de datos para calcular.")

print("\n" + "=" * 70)
print("[PASO 10] Acumulado con SUM OVER")
print("=" * 70)

window_acum = Window.partitionBy("producto").orderBy("mes").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_acum = ventas.withColumn("acumulado", round(sum("total").over(window_acum), 2))

print("--- Total acumulado por producto ---")
df_acum.where(col("producto") == "Laptop").orderBy("mes").show(truncate=False)

print("rowsBetween(unboundedPreceding, currentRow) = desde el inicio hasta la fila actual.")

print("\n" + "=" * 70)
print("[PASO 11] Diferencia con la primera fila (running diff)")
print("=" * 70)

window_first = Window.partitionBy("producto").orderBy("mes").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_first = ventas.withColumn("primera_venta", lag("total", 0).over(
    Window.partitionBy("producto").rowsBetween(Window.unboundedPreceding, Window.unboundedPreceding)
))

print("\n--- Comparar cada mes con el primer mes ---")
df_first.where(col("producto") == "Laptop").orderBy("mes").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 12] LAG/LEAD en SQL")
print("=" * 70)

ventas.createOrReplaceTempView("v_ventas")

print("\n--- Diferencia mes a mes (SQL) ---")
spark.sql("""
    SELECT producto, mes, total,
           LAG(total, 1)  OVER (PARTITION BY producto ORDER BY mes) as venta_ant,
           total - LAG(total, 1) OVER (PARTITION BY producto ORDER BY mes) as diferencia,
           ROUND((total - LAG(total, 1) OVER (PARTITION BY producto ORDER BY mes))
                 / LAG(total, 1) OVER (PARTITION BY producto ORDER BY mes) * 100, 2) as pct_cambio
    FROM v_ventas
    WHERE producto = 'Laptop'
    ORDER BY mes
""").show(truncate=False)

print("\n--- Lead con default (SQL) ---")
spark.sql("""
    SELECT producto, mes, total,
           LEAD(total, 1, 0.0) OVER (PARTITION BY producto ORDER BY mes) as venta_sig
    FROM v_ventas
    WHERE producto = 'Mouse'
    ORDER BY mes
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 13] Ejemplo practico: deteccion de tendencia")
print("=" * 70)

print("\n--- Clasificar meses como Subida/Baja/Estable ---")
df_trend = df_diff.withColumn("tendencia",
    (col("diferencia") > 0).__and__(col("diferencia").isNotNull()).cast("int") * 1 +
    (col("diferencia") < 0).__and__(col("diferencia").isNotNull()).cast("int") * (-1) +
    (col("diferencia") == 0).__and__(col("diferencia").isNotNull()).cast("int") * 0
)

from pyspark.sql.functions import when
df_trend = df_diff.withColumn("tendencia",
    when(col("diferencia") > 0, "Subida")
    .when(col("diferencia") < 0, "Baja")
    .otherwise("Estable")
)

print("--- Tendencia por mes ---")
df_trend.where(col("producto") == "Laptop").orderBy("mes").show(truncate=False)

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 20")
print("=" * 70)

spark.stop()
