import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, sum, avg, round, count

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("PivotTables") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 12: PIVOT TABLES")
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
print("[PASO 2] PIVOT BASICO — filas a columnas")
print("=" * 70)

print("\n--- Ventas totales por REGION (filas) y PRODUCTO (columnas) ---")
df.groupBy("region").pivot("producto").sum("total").show(truncate=False)

print("\n--- Ventas totales por CATEGORIA (filas) y REGION (columnas) ---")
df.groupBy("categoria").pivot("region").sum("total").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 3] PIVOT con valores especificos")
print("=" * 70)

print("\n--- Pivot solo para Laptop y Mouse (vals especificos) ---")
df.groupBy("region").pivot("producto", ["Laptop", "Mouse"]).sum("total").show(truncate=False)

print("\n--- Pivot solo para Norte y Sur ---")
df.groupBy("producto").pivot("region", ["Norte", "Sur"]).sum("total").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 4] PIVOT con diferente agregacion")
print("=" * 70)

print("\n--- Pivot con COUNT (numero de transacciones) ---")
df.groupBy("region").pivot("producto").count().show(truncate=False)

print("\n--- Pivot con AVG (promedio de venta) ---")
df.groupBy("region").pivot("producto").agg(
    round(avg("total"), 2)
).show(truncate=False)

print("\n--- Pivot con MAX (venta maxima por producto/region) ---")
df.groupBy("region").pivot("producto").max("total").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 5] PIVOT con multiples columnas de agrupacion")
print("=" * 70)

print("\n--- Pivot por REGION+CATEGORIA -> PRODUCTO ---")
df.groupBy("region", "categoria").pivot("producto").sum("total").orderBy("region", "categoria").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 6] PIVOT con fecha (mes) — analisis temporal")
print("=" * 70)

df2 = df.withColumn("mes", col("fecha").substr(1, 7))

print("\n--- Ventas por producto y mes ---")
df2.groupBy("producto").pivot("mes").sum("total").show(truncate=False)

print("\n--- Ventas por region y mes ---")
df2.groupBy("region").pivot("mes").sum("total").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 7] Unpivot (melt) — columnas a filas")
print("=" * 70)

pivoted = df.groupBy("region").pivot("producto").sum("total")
print("Tabla pivoteada:")
pivoted.show(truncate=False)

print("\n--- Unpivot usando stack en SQL ---")
pivoted.createOrReplaceTempView("pivoted")

unpivot_cols = [c for c in pivoted.columns if c != "region"]
stack_expr = ", ".join([f"'{c}', {c}" for c in unpivot_cols])

spark.sql(f"""
    SELECT region, producto, total_venta
    FROM pivoted
    LATERAL VIEW STACK({len(unpivot_cols)}, {stack_expr}) AS producto, total_venta
    WHERE total_venta IS NOT NULL
    ORDER BY region, producto
""").show(truncate=False)

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 12")
print("=" * 70)

spark.stop()
