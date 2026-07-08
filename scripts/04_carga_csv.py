import os
import sys
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("CargaCSV") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 04: CARGA DE CSV")
print("=" * 70)

print("\n[PASO 1] Generando datos de ventas y guardando CSV...\n")

data = [
    ("Laptop",      "Norte",  1200.00, 3, "2024-01-15"),
    ("Mouse",       "Sur",     25.50, 10, "2024-01-16"),
    ("Teclado",     "Centro",  85.00,  5, "2024-01-17"),
    ("Monitor",     "Norte",   350.00, 2, "2024-01-18"),
    ("Webcam",      "Oeste",    45.99,  7, "2024-01-19"),
    ("Audifonos",   "Este",     60.00,  4, "2024-01-20"),
    ("Tablet",      "Sur",     250.00,  3, "2024-01-21"),
    ("Cargador",    "Centro",   20.00, 15, "2024-01-22"),
    ("Laptop",      "Este",   1300.00,  1, "2024-01-23"),
    ("Mouse",       "Norte",    25.50,  8, "2024-01-24"),
    ("Monitor",     "Sur",     370.00,  2, "2024-01-25"),
    ("Teclado",     "Oeste",    90.00,  6, "2024-01-26"),
    ("Webcam",      "Centro",   45.99,  5, "2024-01-27"),
    ("Tablet",      "Norte",   260.00,  2, "2024-01-28"),
    ("Cargador",    "Sur",      20.00, 12, "2024-01-29"),
    ("Laptop",      "Centro", 1150.00,  2, "2024-01-30"),
    ("Audifonos",   "Norte",    65.00,  3, "2024-01-31"),
    ("Mouse",       "Oeste",    30.00,  6, "2024-02-01"),
    ("Monitor",     "Centro",  340.00,  1, "2024-02-02"),
    ("Teclado",     "Sur",      80.00,  4, "2024-02-03"),
    ("Webcam",      "Este",     50.00,  3, "2024-02-04"),
    ("Tablet",      "Oeste",   240.00,  1, "2024-02-05"),
    ("Cargador",    "Este",     25.00, 10, "2024-02-06"),
    ("Laptop",      "Oeste",   1250.00,  3, "2024-02-07"),
]

with open("data/input/ventas.csv", "w") as f:
    f.write("producto,region,precio,cantidad,fecha\n")
    for row in data:
        valores = [str(v) if v is not None else "" for v in row]
        f.write(",".join(valores) + "\n")

print("  Archivo creado: data/input/ventas.csv")
print(f"  Total registros: {len(data)}")
print("\n  Contenido (primeras 5 filas):")
with open("data/input/ventas.csv") as f:
    for i, linea in enumerate(f):
        if i > 5:
            break
        print(f"    {linea.strip()}")

print("\n" + "=" * 70)
print("[PASO 2] Cargando CSV con inferSchema=True")
print("=" * 70)

df = spark.read.csv("data/input/ventas.csv", header=True, inferSchema=True)

print("\nSchema detectado automaticamente:")
df.printSchema()

print("\nPrimeras 10 filas:")
df.show(10, truncate=False)

print(f"\nNumero total de filas:    {df.count()}")
print(f"Numero total de columnas: {len(df.columns)}")
print(f"Nombres de columnas:      {df.columns}")

print("\n" + "=" * 70)
print("[PASO 3] Comparacion: SIN inferSchema (todo string)")
print("=" * 70)

df_sin = spark.read.csv("data/input/ventas.csv", header=True)
print("\nSchema (sin inferSchema):")
df_sin.printSchema()

print("\n  >>> NOTA: Sin inferSchema, todas las columnas son STRING")
print("  >>> Con inferSchema=True, Spark infirio: producto (string),")
print("  >>> region (string), precio (double), cantidad (int), fecha (string)")

print("\n" + "=" * 70)
print("[PASO 4] Exploracion adicional del DataFrame")
print("=" * 70)

print("\nResumen estadistico (columnas numericas):")
df.describe().show()

print("\nTotal de ventas por producto:")
from pyspark.sql.functions import col, sum as _sum, round as _round
df.groupBy("producto").agg(
    _round(_sum(col("precio") * col("cantidad")), 2).alias("total_ingresos"),
    _sum("cantidad").alias("unidades_vendidas")
).orderBy(col("total_ingresos").desc()).show()

print("\nTotal de ventas por region:")
df.groupBy("region").agg(
    _round(_sum(col("precio") * col("cantidad")), 2).alias("total_ingresos")
).orderBy(col("total_ingresos").desc()).show()

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 04")
print("=" * 70)

spark.stop()
