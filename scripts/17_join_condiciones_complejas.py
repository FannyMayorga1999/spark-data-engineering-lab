import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, lit, round, count, sum, datediff, to_date

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("JoinCondicionesComplejas") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 17: JOIN CON CONDICIONES COMPLEJAS")
print("=" * 70)

print("\n[PASO 1] Creando DataFrames...\n")

schema_clientes = StructType([
    StructField("id",       IntegerType(), True),
    StructField("nombre",   StringType(),  True),
    StructField("ciudad",   StringType(),  True),
    StructField("nivel",    StringType(),  True),
])

data_clientes = [
    (1, "Ana",     "Madrid",    "Premium"),
    (2, "Carlos",  "Barcelona", "Basico"),
    (3, "Maria",   "Valencia",  "Premium"),
    (4, "Pedro",   "Sevilla",   "Basico"),
    (5, "Laura",   "Bilbao",    "Premium"),
    (6, "Jorge",   "Madrid",    "Basico"),
]

schema_pedidos = StructType([
    StructField("id",          IntegerType(), True),
    StructField("cliente_id",  IntegerType(), True),
    StructField("producto",    StringType(),  True),
    StructField("cantidad",    IntegerType(), True),
    StructField("total",       DoubleType(),  True),
    StructField("fecha",       StringType(),  True),
])

data_pedidos = [
    (101, 1,  "Laptop",    1, 1200.0, "2024-01-15"),
    (102, 2,  "Mouse",     3, 75.0,   "2024-01-20"),
    (103, 3,  "Teclado",   2, 90.0,   "2024-02-10"),
    (104, 1,  "Monitor",   1, 350.0,  "2024-02-28"),
    (105, 5,  "Laptop",    1, 1200.0, "2024-03-05"),
    (106, 7,  "Silla",     1, 150.0,  "2024-03-10"),
    (107, 2,  "Monitor",   1, 350.0,  "2024-03-15"),
    (108, 9,  "Mouse",     5, 125.0,  "2024-04-01"),
    (109, 1,  "Audifonos", 2, 120.0,  "2024-04-10"),
    (110, 3,  "Laptop",    1, 1200.0, "2024-04-20"),
    (111, 6,  "Teclado",   1, 45.0,   "2024-05-01"),
    (112, 4,  "Monitor",   2, 700.0,  "2024-05-15"),
]

clientes = spark.createDataFrame(data_clientes, schema_clientes)
pedidos = spark.createDataFrame(data_pedidos, schema_pedidos)

print("Clientes:")
clientes.show(truncate=False)
print("Pedidos:")
pedidos.show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 2] JOIN con condicion AND (igualdad + filtro)")
print("=" * 70)

print("\n--- Clientes de Madrid con pedidos > 100 ---")
join_and = clientes.join(
    pedidos,
    (clientes.id == pedidos.cliente_id) & (pedidos.total > 100)
)
join_and.show(truncate=False)

print("Se unen SOLO cuando se cumplen AMBAS condiciones.")

print("\n" + "=" * 70)
print("[PASO 3] JOIN con condicion OR")
print("=" * 70)

print("\n--- Clientes de Madrid O pedidos > 500 ---")
join_or = clientes.join(
    pedidos,
    (clientes.id == pedidos.cliente_id) &
    ((clientes.ciudad == "Madrid") | (pedidos.total > 500))
)
join_or.show(truncate=False)

print("Se unen cuando se cumple AL MENOS UNA de las condiciones.")

print("\n" + "=" * 70)
print("[PASO 4] JOIN con condicion AND + OR combinadas")
print("=" * 70)

print("\n--- Clientes Premium de Madrid O cualquier cliente con pedido > 1000 ---")
join_complex = clientes.join(
    pedidos,
    (clientes.id == pedidos.cliente_id) &
    ((clientes.nivel == "Premium") & (clientes.ciudad == "Madrid")) |
    (pedidos.total > 1000)
)
join_complex.show(truncate=False)

print("Precedencia: AND se evalua antes que OR.")
print("Usa parentesis para controlar la logica.")

print("\n" + "=" * 70)
print("[PASO 5] JOIN por rango de fechas")
print("=" * 70)

schema_ofertas = StructType([
    StructField("producto",   StringType(),  True),
    StructField("descuento",  DoubleType(),  True),
    StructField("inicio",     StringType(),  True),
    StructField("fin",        StringType(),  True),
])

data_ofertas = [
    ("Laptop",    0.10, "2024-01-01", "2024-02-28"),
    ("Mouse",     0.20, "2024-03-01", "2024-04-30"),
    ("Teclado",   0.15, "2024-02-01", "2024-03-31"),
    ("Monitor",   0.05, "2024-04-01", "2024-06-30"),
]

ofertas = spark.createDataFrame(data_ofertas, schema_ofertas)
ofertas = ofertas.withColumn("inicio", to_date(col("inicio")))
ofertas = ofertas.withColumn("fin", to_date(col("fin")))

pedidos_con_fecha = pedidos.withColumn("fecha", to_date(col("fecha")))

print("Ofertas:")
ofertas.show(truncate=False)

print("\n--- Pedidos que cayeron en periodo de oferta por producto ---")
join_fecha = pedidos_con_fecha.join(
    ofertas,
    (pedidos_con_fecha.producto == ofertas.producto) &
    (pedidos_con_fecha.fecha >= ofertas.inicio) &
    (pedidos_con_fecha.fecha <= ofertas.fin)
).select(
    pedidos_con_fecha.id.alias("pedido_id"),
    pedidos_con_fecha.producto,
    pedidos_con_fecha.total,
    pedidos_con_fecha.fecha,
    ofertas.descuento
)
join_fecha.show(truncate=False)

print("Join por rango: la fecha del pedido debe estar entre inicio y fin de la oferta.")

print("\n" + "=" * 70)
print("[PASO 6] SELF JOIN con multiples condiciones")
print("=" * 70)

print("\n--- Pares de clientes de la misma ciudad con pedidos similares ---")
self_join = clientes.alias("c1").join(
    clientes.alias("c2"),
    (col("c1.ciudad") == col("c2.ciudad")) &
    (col("c1.id") < col("c2.id"))
).select(
    col("c1.nombre").alias("cliente_1"),
    col("c2.nombre").alias("cliente_2"),
    col("c1.ciudad")
)
self_join.show(truncate=False)

print("Self join: una tabla consigo misma. La condicion c1.id < c2.id evita duplicados.")

print("\n" + "=" * 70)
print("[PASO 7] JOIN con NOT (exclusion)")
print("=" * 70)

print("\n--- Clientes que NO compraron Laptop (combinacion SEMI + ANTI) ---")
clientes_con_laptop = clientes.join(
    pedidos,
    (clientes.id == pedidos.cliente_id) & (pedidos.producto == "Laptop"),
    "left_semi"
)
exclusion = clientes.join(clientes_con_laptop, clientes.id == clientes_con_laptop.id, "left_anti")
exclusion.show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 8] JOIN con condiciones en SQL")
print("=" * 70)

clientes.createOrReplaceTempView("v_clientes")
pedidos.createOrReplaceTempView("v_pedidos")
ofertas.createOrReplaceTempView("v_ofertas")
pedidos_con_fecha.createOrReplaceTempView("v_pedidos_f")

print("\n--- Clientes de Madrid con pedidos > 100 (SQL) ---")
spark.sql("""
    SELECT c.nombre, c.ciudad, p.producto, p.total
    FROM v_clientes c
    INNER JOIN v_pedidos p ON c.id = p.cliente_id AND p.total > 100
    WHERE c.ciudad = 'Madrid'
    ORDER BY p.total DESC
""").show(truncate=False)

print("\n--- Join por rango de fechas (SQL) ---")
spark.sql("""
    SELECT p.id as pedido_id, p.producto, p.total, p.fecha, o.descuento
    FROM v_pedidos_f p
    INNER JOIN v_ofertas o ON p.producto = o.producto AND p.fecha BETWEEN o.inicio AND o.fin
    ORDER BY p.fecha
""").show(truncate=False)

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 17")
print("=" * 70)

spark.stop()
