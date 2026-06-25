import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, date_format, weekofyear
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from datetime import datetime, timedelta
import random

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("AnalisisVentas") \
    .getOrCreate()

random.seed(42)
products = ["Laptop", "Mouse", "Teclado", "Monitor", "Webcam", "Audifonos", "Tablet", "Cargador"]
regions = ["Norte", "Sur", "Centro", "Este", "Oeste"]

rows = []
for i in range(1000):
    date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
    rows.append((
        random.choice(products),
        random.choice(regions),
        round(random.uniform(10, 1500), 2),
        random.randint(1, 5),
        date
    ))

schema = StructType([
    StructField("producto", StringType(), True),
    StructField("region", StringType(), True),
    StructField("precio_unitario", DoubleType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("fecha", DateType(), True),
])

df = spark.createDataFrame(rows, schema)
df = df.withColumn("total_venta", col("precio_unitario") * col("cantidad"))
df = df.withColumn("semana", weekofyear("fecha"))
df = df.withColumn("mes", date_format("fecha", "yyyy-MM"))

print("=" * 60)
print("DATOS DE VENTAS - VISTA PREVIA")
print("=" * 60)
df.show(5, truncate=False)

df.createOrReplaceTempView("ventas")

print("\n" + "=" * 60)
print("VENTAS TOTALES POR PRODUCTO")
print("=" * 60)
spark.sql("""
    SELECT producto,
           COUNT(*) AS num_ventas,
           ROUND(SUM(total_venta), 2) AS ingresos_totales,
           ROUND(AVG(total_venta), 2) AS ticket_promedio
    FROM ventas
    GROUP BY producto
    ORDER BY ingresos_totales DESC
""").show()

print("=" * 60)
print("VENTAS POR REGION")
print("=" * 60)
(df.groupBy("region")
   .agg(
       _sum("total_venta").alias("ingresos"),
       avg("total_venta").alias("promedio"),
       count("*").alias("transacciones")
   )
   .orderBy(col("ingresos").desc())
   .show())

print("=" * 60)
print("TOP 5 PRODUCTOS POR MES (primer trimestre)")
print("=" * 60)
spark.sql("""
    SELECT mes, producto, ROUND(SUM(total_venta), 2) AS ingresos
    FROM ventas
    WHERE mes IN ('2024-01', '2024-02', '2024-03')
    GROUP BY mes, producto
    ORDER BY mes, ingresos DESC
""").show(15)

df.write.mode("overwrite").parquet("data/output/ventas.parquet")
print("\nDatos guardados en data/output/ventas.parquet")

spark.stop()
