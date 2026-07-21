import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, round, count, sum, when

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("LeftRightFullJoins") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 15: LEFT / RIGHT / FULL JOINS")
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
print("[PASO 2] LEFT JOIN — todas las filas de la izquierda")
print("=" * 70)

print("\n--- clientes LEFT JOIN pedidos ---")
left_join = clientes.alias("c").join(
    pedidos.alias("p"),
    col("c.id") == col("p.cliente_id"),
    "left"
).select(
    col("c.id").alias("cliente_id"),
    col("c.nombre"),
    col("c.ciudad"),
    col("p.id").alias("pedido_id"),
    col("p.producto"),
    col("p.cantidad"),
    col("p.total")
)
left_join.show(truncate=False)

print("Filas resultado:", left_join.count())
print("Todos los clientes aparecen. Si no tienen pedido, las columnas de pedidos son NULL.")
print("Cliente 4 (Pedro), 6 (Jorge) no tienen pedidos -> NULL en columnas de pedidos.")

print("\n" + "=" * 70)
print("[PASO 3] Identificar filas sin match en LEFT JOIN")
print("=" * 70)

print("\n--- Clientes SIN pedidos (NULL en pedido_id) ---")
clientes_sin_pedido = left_join.where(col("pedido_id").isNull())
clientes_sin_pedido.show(truncate=False)

print("\n--- Contar clientes con y sin pedidos ---")
clientes_con_pedidos = left_join.where(col("pedido_id").isNotNull()).count()
print(f"Con pedidos: {clientes_con_pedidos}")
print(f"Sin pedidos:  {6 - clientes_con_pedidos}")

print("\n" + "=" * 70)
print("[PASO 4] RIGHT JOIN — todas las filas de la derecha")
print("=" * 70)

print("\n--- clientes RIGHT JOIN pedidos ---")
right_join = clientes.alias("c").join(
    pedidos.alias("p"),
    col("c.id") == col("p.cliente_id"),
    "right"
).select(
    col("c.id").alias("cliente_id"),
    col("c.nombre"),
    col("c.ciudad"),
    col("p.id").alias("pedido_id"),
    col("p.producto"),
    col("p.cantidad"),
    col("p.total")
)
right_join.show(truncate=False)

print("Filas resultado:", right_join.count())
print("Todos los pedidos aparecen. Si el cliente no existe, columnas de clientes son NULL.")
print("Pedido 106 (cliente_id=7) y 108 (cliente_id=9) -> NULL en columnas de clientes.")

print("\n" + "=" * 70)
print("[PASO 5] Identificar pedidos sin cliente valido")
print("=" * 70)

print("\n--- Pedidos con cliente NULL (no existe) ---")
pedidos_sin_cliente = right_join.where(col("nombre").isNull())
pedidos_sin_cliente.show(truncate=False)

print("\n--- Contar pedidos con y sin cliente ---")
pedidos_con_cliente = right_join.where(col("nombre").isNotNull()).count()
print(f"Con cliente: {pedidos_con_cliente}")
print(f"Sin cliente:  {8 - pedidos_con_cliente}")

print("\n" + "=" * 70)
print("[PASO 6] FULL OUTER JOIN — todas las filas de ambos lados")
print("=" * 70)

print("\n--- clientes FULL JOIN pedidos ---")
full_join = clientes.alias("c").join(
    pedidos.alias("p"),
    col("c.id") == col("p.cliente_id"),
    "full"
).select(
    col("c.id").alias("cliente_id"),
    col("c.nombre"),
    col("c.ciudad"),
    col("p.id").alias("pedido_id"),
    col("p.producto"),
    col("p.cantidad"),
    col("p.total")
)
full_join.show(truncate=False)

print("Filas resultado:", full_join.count())
print("Aparecen TODOS los clientes y TODOS los pedidos.")
print("Filas sin match tienen NULL en el lado que no tiene correspondencia.")

print("\n" + "=" * 70)
print("[PASO 7] Contar filas con NULL en FULL JOIN")
print("=" * 70)

print("\n--- Filas con NULL en columna de clientes ---")
full_join.where(col("nombre").isNull()).show(truncate=False)

print("\n--- Filas con NULL en columna de pedidos ---")
full_join.where(col("pedido_id").isNull()).show(truncate=False)

print("\n--- Resumen ---")
n_full = full_join.count()
n_null_cliente = full_join.where(col("nombre").isNull()).count()
n_null_pedido = full_join.where(col("pedido_id").isNull()).count()
print(f"Total filas:        {n_full}")
print(f"Sin cliente:        {n_null_cliente}")
print(f"Sin pedido:         {n_null_pedido}")
print(f"Con ambos:          {n_full - n_null_cliente - n_null_pedido}")

print("\n" + "=" * 70)
print("[PASO 8] Comparacion: LEFT vs RIGHT vs FULL")
print("=" * 70)

print("\n--- LEFT JOIN: filas unicas solo del lado izquierdo ---")
left_only = clientes.alias("c").join(
    pedidos.alias("p"),
    col("c.id") == col("p.cliente_id"),
    "left"
).select(
    col("c.id").alias("cliente_id"),
    col("c.nombre"),
    col("p.id").alias("pedido_id")
).where(col("pedido_id").isNull())
left_only.show(truncate=False)

print("\n--- RIGHT JOIN: filas unicas solo del lado derecho ---")
right_only = clientes.alias("c").join(
    pedidos.alias("p"),
    col("c.id") == col("p.cliente_id"),
    "right"
).select(
    col("c.id").alias("cliente_id"),
    col("c.nombre"),
    col("p.id").alias("pedido_id")
).where(col("nombre").isNull())
right_only.show(truncate=False)

print("\n--- FULL JOIN: todas las filas sin match ---")
full_no_match = clientes.alias("c").join(
    pedidos.alias("p"),
    col("c.id") == col("p.cliente_id"),
    "full"
).select(
    col("c.id").alias("cliente_id"),
    col("c.nombre"),
    col("p.id").alias("pedido_id")
).where(col("nombre").isNull() | col("pedido_id").isNull())
full_no_match.show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 9] LEFT JOIN con condiciones adicionales")
print("=" * 70)

print("\n--- LEFT JOIN + WHERE total > 100 ---")
left_filtrado = clientes.alias("c").join(
    pedidos.alias("p"),
    col("c.id") == col("p.cliente_id"),
    "left"
).select(
    col("c.id").alias("cliente_id"),
    col("c.nombre"),
    col("p.id").alias("pedido_id"),
    col("p.producto"),
    col("p.total")
).where((col("total").isNull()) | (col("total") > 100))
left_filtrado.show(truncate=False)

print("Clientes sin pedidos siempre aparecen (total es NULL, pasa el filtro).")
print("Solo se muestran pedidos con total > 100.")

print("\n" + "=" * 70)
print("[PASO 10] FULL JOIN con agregacion")
print("=" * 70)

print("\n--- Total de pedidos por cliente (incluyendo clientes sin pedidos) ---")
clientes.alias("c").join(
    pedidos.alias("p"),
    col("c.id") == col("p.cliente_id"),
    "full"
).groupBy(
    col("c.id"), col("c.nombre")
).agg(
    count("pedido_id").alias("num_pedidos"),
    round(sum("total"), 2).alias("total_gastado")
).orderBy(col("total_gastado").desc().nulls_last()).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 11] LEFT / RIGHT / FULL con SQL")
print("=" * 70)

clientes.createOrReplaceTempView("clientes")
pedidos.createOrReplaceTempView("pedidos")

print("\n--- LEFT JOIN via SQL ---")
spark.sql("""
    SELECT c.id as cliente_id, c.nombre, c.ciudad,
           p.id as pedido_id, p.producto, p.total
    FROM clientes c
    LEFT JOIN pedidos p ON c.id = p.cliente_id
    ORDER BY c.id
""").show(truncate=False)

print("\n--- RIGHT JOIN via SQL ---")
spark.sql("""
    SELECT c.id as cliente_id, c.nombre, c.ciudad,
           p.id as pedido_id, p.producto, p.total
    FROM clientes c
    RIGHT JOIN pedidos p ON c.id = p.cliente_id
    ORDER BY p.id
""").show(truncate=False)

print("\n--- FULL JOIN via SQL ---")
spark.sql("""
    SELECT c.id as cliente_id, c.nombre, c.ciudad,
           p.id as pedido_id, p.producto, p.total
    FROM clientes c
    FULL OUTER JOIN pedidos p ON c.id = p.cliente_id
    ORDER BY c.id NULLS LAST, p.id NULLS LAST
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 12] Manejo de NULLs resultantes")
print("=" * 70)

print("\n--- FULL JOIN con COALESCE para valores por defecto ---")
spark.sql("""
    SELECT 
        COALESCE(c.id, -1) as cliente_id,
        COALESCE(c.nombre, 'DESCONOCIDO') as nombre,
        COALESCE(c.ciudad, 'N/A') as ciudad,
        COALESCE(p.id, -1) as pedido_id,
        COALESCE(p.producto, 'N/A') as producto,
        COALESCE(p.total, 0.0) as total
    FROM clientes c
    FULL OUTER JOIN pedidos p ON c.id = p.cliente_id
    ORDER BY cliente_id
""").show(truncate=False)

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 15")
print("=" * 70)

spark.stop()
