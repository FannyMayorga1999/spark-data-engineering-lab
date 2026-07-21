import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, round, count, sum

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("SemiAntiJoins") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 16: SEMI Y ANTI JOINS")
print("=" * 70)

print("\n[PASO 1] Creando DataFrames de clientes y pedidos...\n")

schema_clientes = StructType([
    StructField("id",     IntegerType(), True),
    StructField("nombre", StringType(),  True),
    StructField("ciudad", StringType(),  True),
])

data_clientes = [
    (1, "Ana",     "Madrid"),
    (2, "Carlos",  "Barcelona"),
    (3, "Maria",   "Valencia"),
    (4, "Pedro",   "Sevilla"),
    (5, "Laura",   "Bilbao"),
    (6, "Jorge",   "Malaga"),
]

schema_pedidos = StructType([
    StructField("id",          IntegerType(), True),
    StructField("cliente_id",  IntegerType(), True),
    StructField("producto",    StringType(),  True),
    StructField("cantidad",    IntegerType(), True),
    StructField("total",       DoubleType(),  True),
])

data_pedidos = [
    (101, 1,  "Laptop",    1, 1200.0),
    (102, 2,  "Mouse",     3, 75.0),
    (103, 3,  "Teclado",   2, 90.0),
    (104, 1,  "Monitor",   1, 350.0),
    (105, 5,  "Laptop",    1, 1200.0),
    (106, 7,  "Silla",     1, 150.0),
    (107, 2,  "Monitor",   1, 350.0),
    (108, 9,  "Mouse",     5, 125.0),
]

clientes = spark.createDataFrame(data_clientes, schema_clientes)
pedidos = spark.createDataFrame(data_pedidos, schema_pedidos)

print("Clientes:")
clientes.show(truncate=False)
print("Pedidos:")
pedidos.show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 2] LEFT SEMI JOIN — filas de la izquierda que tienen match")
print("=" * 70)

print("\n--- Clientes que SÍ tienen pedidos (left_semi) ---")
semi_join = clientes.join(pedidos, clientes.id == pedidos.cliente_id, "left_semi")
semi_join.show(truncate=False)

print("Filas resultado:", semi_join.count())
print("Solo aparecen columnas de clientes. Es como un filtro WHERE EXISTS.")
print("Solo clientes 1, 2, 3, 5 tienen pedidos.")

print("\n" + "=" * 70)
print("[PASO 3] LEFT SEMI vs INNER JOIN")
print("=" * 70)

print("\n--- Comparar resultados ---")
print("LEFT SEMI JOIN (solo columnas de clientes):")
semi_join.show(truncate=False)

print("INNER JOIN (columnas de clientes + pedidos):")
inner_join = clientes.alias("c").join(
    pedidos.alias("p"),
    col("c.id") == col("p.cliente_id"),
    "inner"
).select(
    col("c.id").alias("cliente_id"),
    col("c.nombre"),
    col("c.ciudad"),
    col("p.id").alias("pedido_id"),
    col("p.producto"),
    col("p.total")
)
inner_join.show(truncate=False)

print("Diferencia clave:")
print("- LEFT SEMI: retorna filas de la izquierda UNA VEZ (sin duplicar)")
print("- INNER JOIN: retorna combinaciones (puede duplicar filas de la izquierda)")
print("Cliente 1 aparece 2 veces en INNER (dos pedidos), pero 1 vez en SEMI.")

print("\n" + "=" * 70)
print("[PASO 4] LEFT SEMI con condicion adicional")
print("=" * 70)

print("\n--- Clientes con pedidos MAYORES a 200 ---")
semi_filtrado = clientes.join(
    pedidos,
    (clientes.id == pedidos.cliente_id) & (pedidos.total > 200),
    "left_semi"
)
semi_filtrado.show(truncate=False)

print("Solo clientes que tienen AL MENOS UN pedido mayor a 200.")

print("\n" + "=" * 70)
print("[PASO 5] LEFT SEMI con multiples condiciones")
print("=" * 70)

print("\n--- Clientes con pedidos de Laptop O Monitor ---")
semi_multi = clientes.join(
    pedidos,
    (clientes.id == pedidos.cliente_id) & 
    (pedidos.producto.isin("Laptop", "Monitor")),
    "left_semi"
)
semi_multi.show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 6] LEFT ANTI JOIN — filas de la izquierda que NO tienen match")
print("=" * 70)

print("\n--- Clientes que NO tienen pedidos (left_anti) ---")
anti_join = clientes.join(pedidos, clientes.id == pedidos.cliente_id, "left_anti")
anti_join.show(truncate=False)

print("Filas resultado:", anti_join.count())
print("Solo aparecen columnas de clientes. Es como un filtro WHERE NOT EXISTS.")
print("Solo clientes 4 (Pedro), 6 (Jorge) NO tienen pedidos.")

print("\n" + "=" * 70)
print("[PASO 7] LEFT ANTI vs NOT IN")
print("=" * 70)

print("\n--- Clientes NO estan en la lista de cliente_id de pedidos ---")
anti_join.show(truncate=False)

print("\n--- Equivalente con subconsulta SQL ---")
clientes.createOrReplaceTempView("clientes")
pedidos.createOrReplaceTempView("pedidos")

spark.sql("""
    SELECT * FROM clientes
    WHERE id NOT IN (SELECT DISTINCT cliente_id FROM pedidos WHERE cliente_id IS NOT NULL)
""").show(truncate=False)

print("LEFT ANTI es mas eficiente que NOT IN en Spark.")

print("\n" + "=" * 70)
print("[PASO 8] LEFT ANTI con condicion")
print("=" * 70)

print("\n--- Clientes que NO tienen pedidos mayores a 100 ---")
anti_filtrado = clientes.join(
    pedidos,
    (clientes.id == pedidos.cliente_id) & (pedidos.total > 100),
    "left_anti"
)
anti_filtrado.show(truncate=False)

print("Clientes 4 y 6 no tienen pedidos, siempre aparecen.")
print("Cliente 3 tiene un pedido de 90 (no > 100), tambien aparece.")

print("\n" + "=" * 70)
print("[PASO 9] LEFT SEMI para validar existencia")
print("=" * 70)

print("\n--- Crear lista de productos unicos ---")
schema_productos = StructType([
    StructField("producto", StringType(), True),
    StructField("precio",   DoubleType(), True),
])

data_productos = [
    ("Laptop",    1200.0),
    ("Mouse",      25.0),
    ("Teclado",    45.0),
    ("Monitor",   350.0),
    ("Silla",     150.0),
    ("Webcam",     80.0),
    ("Audifonos",  60.0),
]

productos = spark.createDataFrame(data_productos, schema_productos)

print("Productos disponibles:")
productos.show(truncate=False)

print("\n--- Productos que HAN SIDO vendidos (left_semi) ---")
productos_vendidos = productos.join(pedidos, productos.producto == pedidos.producto, "left_semi")
productos_vendidos.show(truncate=False)

print("\n--- Productos que NO han sido vendidos (left_anti) ---")
productos_no_vendidos = productos.join(pedidos, productos.producto == pedidos.producto, "left_anti")
productos_no_vendidos.show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 10] Uso practico: clientes inactivos")
print("=" * 70)

print("\n--- Pedidos recientes (simulados) ---")
schema_recientes = StructType([
    StructField("cliente_id",  IntegerType(), True),
    StructField("producto",    StringType(),  True),
    StructField("total",       DoubleType(),  True),
])

data_recientes = [
    (1, "Laptop",    1200.0),
    (3, "Mouse",      25.0),
    (5, "Teclado",    45.0),
]

pedidos_recientes = spark.createDataFrame(data_recientes, schema_recientes)
pedidos_recientes.show(truncate=False)

print("\n--- Clientes INACTIVOS (sin pedidos recientes) ---")
clientes.join(pedidos_recientes, clientes.id == pedidos_recientes.cliente_id, "left_anti") \
    .show(truncate=False)

print("\n--- Clientes ACTIVOS (con pedidos recientes) ---")
clientes.join(pedidos_recientes, clientes.id == pedidos_recientes.cliente_id, "left_semi") \
    .show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 11] LEFT SEMI y LEFT ANTI con SQL")
print("=" * 70)

print("\n--- LEFT SEMI via SQL ---")
spark.sql("""
    SELECT * FROM clientes c
    WHERE EXISTS (SELECT 1 FROM pedidos p WHERE p.cliente_id = c.id)
""").show(truncate=False)

print("\n--- LEFT ANTI via SQL ---")
spark.sql("""
    SELECT * FROM clientes c
    WHERE NOT EXISTS (SELECT 1 FROM pedidos p WHERE p.cliente_id = c.id)
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 12] Rendimiento: SEMI/ANTI vs INNER/LEFT")
print("=" * 70)

print("\n--- Explicar plan fisico de LEFT SEMI ---")
clientes.join(pedidos, clientes.id == pedidos.cliente_id, "left_semi").explain("formatted")

print("\n--- Explicar plan fisico de LEFT ANTI ---")
clientes.join(pedidos, clientes.id == pedidos.cliente_id, "left_anti").explain("formatted")

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 16")
print("=" * 70)

spark.stop()
