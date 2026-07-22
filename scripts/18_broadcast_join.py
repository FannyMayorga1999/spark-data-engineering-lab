import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, broadcast, sum, count
from pyspark.sql.functions import round as spark_round

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("BroadcastJoin") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 18: BROADCAST JOIN")
print("=" * 70)

print("\n[PASO 1] Creando DataFrames: tabla grande + tabla pequenia...\n")

schema_productos = StructType([
    StructField("id",       IntegerType(), True),
    StructField("nombre",   StringType(),  True),
    StructField("categoria", StringType(), True),
    StructField("precio",   DoubleType(),  True),
])

data_productos = [
    (1, "Laptop",     "Electronica",  1200.0),
    (2, "Mouse",      "Electronica",    25.0),
    (3, "Teclado",    "Electronica",    45.0),
    (4, "Monitor",    "Electronica",   350.0),
    (5, "Silla",      "Muebles",       150.0),
    (6, "Escritorio", "Muebles",       300.0),
    (7, "Webcam",     "Electronica",    80.0),
    (8, "Audifonos",  "Electronica",    60.0),
]

schema_ventas = StructType([
    StructField("venta_id",    IntegerType(), True),
    StructField("producto_id", IntegerType(), True),
    StructField("cantidad",    IntegerType(), True),
    StructField("total",       DoubleType(),  True),
    StructField("region",      StringType(),  True),
])

import random
random.seed(42)
data_ventas = []
for i in range(1, 501):
    prod_id = random.randint(1, 8)
    cantidad = random.randint(1, 10)
    precio_base = [p[3] for p in data_productos if p[0] == prod_id][0]
    total = round(precio_base * cantidad * random.uniform(0.8, 1.2), 2)
    region = random.choice(["Norte", "Sur", "Este", "Oeste", "Centro"])
    data_ventas.append((i, prod_id, cantidad, total, region))

productos = spark.createDataFrame(data_productos, schema_productos)
ventas = spark.createDataFrame(data_ventas, schema_ventas)

print(f"Tabla pequena - Productos: {productos.count()} filas")
print(f"Tabla grande  - Ventas:    {ventas.count()} filas")

print("\nProductos:")
productos.show(truncate=False)
print("Primeras 10 ventas:")
ventas.show(10, truncate=False)

print("\n" + "=" * 70)
print("[PASO 2] JOIN normal (SortMergeJoin)")
print("=" * 70)

print("\n--- Join sin broadcast ---")
join_normal = ventas.join(productos, ventas.producto_id == productos.id)
join_normal.explain("formatted")

print("Spark usa SortMergeJoin: ambas tablas se ordenan y se unen por partes.")

print("\n" + "=" * 70)
print("[PASO 3] BROADCAST JOIN - forzar con broadcast()")
print("=" * 70)

print("\n--- Join con broadcast en tabla pequena ---")
join_broadcast = ventas.join(broadcast(productos), ventas.producto_id == productos.id)
join_broadcast.explain("formatted")

print("Spark usa BroadcastHashJoin: la tabla pequena se copia a todos los nodos.")
print("Es mas rapido cuando una tabla es pequena (< autoBroadcastJoinThreshold).")

print("\n" + "=" * 70)
print("[PASO 4] Comparar resultados")
print("=" * 70)

print("\n--- Resultado del join normal ---")
resultado_normal = join_normal.select(
    ventas.venta_id,
    productos.nombre.alias("producto"),
    ventas.cantidad,
    ventas.total,
    productos.categoria,
).orderBy(col("venta_id"))
resultado_normal.show(10, truncate=False)

print("\n--- Resultado del broadcast join ---")
resultado_broadcast = join_broadcast.select(
    ventas.venta_id,
    productos.nombre.alias("producto"),
    ventas.cantidad,
    ventas.total,
    productos.categoria,
).orderBy(col("venta_id"))
resultado_broadcast.show(10, truncate=False)

print("Mismos resultados. La diferencia es la ESTRATEGIA de ejecucion, no los datos.")

print("\n" + "=" * 70)
print("[PASO 5] Configurar autoBroadcastJoinThreshold")
print("=" * 70)

print(f"\nValor actual: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")

print("\n--- Desactivar auto broadcast ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
print(f"Nuevo valor: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")

print("\n--- Join sin auto broadcast (deberia usar SortMergeJoin) ---")
join_sin_auto = ventas.join(productos, ventas.producto_id == productos.id)
join_sin_auto.explain("formatted")

print("\n--- Reactivar auto broadcast (threshold = 10MB) ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
print(f"Nuevo valor: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")

print("\n" + "=" * 70)
print("[PASO 6] Broadcast con SQL")
print("=" * 70)

ventas.createOrReplaceTempView("v_ventas")
productos.createOrReplaceTempView("v_productos")

print("\n--- Usar hint BROADCAST en SQL ---")
spark.sql("""
    SELECT /*+ BROADCAST(v_productos) */
           v.venta_id, v.cantidad, v.total, p.nombre, p.categoria
    FROM v_ventas v
    INNER JOIN v_productos p ON v.producto_id = p.id
    ORDER BY v.venta_id
    LIMIT 10
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 7] Broadcast join con agregacion")
print("=" * 70)

print("\n--- Total de ventas por categoria (con broadcast) ---")
ventas.join(broadcast(productos), ventas.producto_id == productos.id) \
    .groupBy(productos.categoria) \
    .agg(
        spark_round(sum("total"), 2).alias("total_ventas"),
        count("venta_id").alias("num_ventas")
    ).orderBy(col("total_ventas").desc()).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 8] Cuando usar Broadcast Join")
print("=" * 70)

print("""
REGLA GENERAL:
- Si una tabla tiene < 10MB (o autoBroadcastJoinThreshold), usar broadcast.
- La tabla pequena se envia a TODOS los nodos del cluster.
- Es ideal para DIMENSIONES (productos, categorias, paises, etc.).

VENTAJAS:
- Evita SortMergeJoin (no necesita ordenar ambas tablas)
- Mucho mas rapido para tablas pequenas
- Evita shuffle (no mueve datos entre nodos)

DESVENTAJAS:
- Si la tabla es grande, consume mucha memoria en cada nodo
- Puede causar OutOfMemoryError si la tabla pequena no es tan pequena

COMO CONFIRMAR:
- .explain("formatted") debe mostrar "BroadcastHashJoin"
- Sin broadcast: "SortMergeJoin"
""")

print("\n" + "=" * 70)
print("[PASO 9] Ejemplo practico: catalogo de productos")
print("=" * 70)

schema_catalogo = StructType([
    StructField("producto_id", IntegerType(), True),
    StructField("proveedor",   StringType(),  True),
    StructField("garantia",    IntegerType(), True),
    StructField("origen",      StringType(),  True),
])

data_catalogo = [
    (1, "Dell",      24, "China"),
    (2, "Logitech",  12, "Japon"),
    (3, "Logitech",  12, "Japon"),
    (4, "Samsung",   36, "Corea"),
    (5, "IKEA",      60, "Suecia"),
    (6, "IKEA",      60, "Suecia"),
    (7, "Logitech",  12, "Japon"),
    (8, "Sony",      24, "Japon"),
]

catalogo = spark.createDataFrame(data_catalogo, schema_catalogo)
print("Catalogo (tabla pequena):")
catalogo.show(truncate=False)

print("\n--- Join ventas + catalogo (broadcast) ---")
ventas.join(broadcast(catalogo), ventas.producto_id == catalogo.producto_id) \
    .select(
        ventas.venta_id,
        catalogo.proveedor,
        catalogo.garantia,
        ventas.total
    ).show(10, truncate=False)

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 18")
print("=" * 70)

spark.stop()
