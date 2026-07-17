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
    .appName("InnerJoin") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 14: INNER JOIN")
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
print("[PASO 2] INNER JOIN basico — por columna con mismo nombre")
print("=" * 70)

print("\n--- clientes.join(pedidos, 'id') ---")
join_basico = clientes.join(pedidos, "id")
join_basico.show(truncate=False)

print("Filas resultado:", join_basico.count())
print("Solo aparecen filas donde 'id' coincide en ambas tablas.")
print("Cliente 4 (Pedro) y 6 (Jorge) NO aparecen (sin pedidos).")
print("Pedido 106 (cliente_id=7) y 108 (cliente_id=9) NO aparecen (sin cliente).")

print("\n" + "=" * 70)
print("[PASO 3] INNER JOIN con columna de conexion diferente")
print("=" * 70)

print("\n--- join por clientes.id = pedidos.cliente_id ---")
join_cols = clientes.join(pedidos, clientes.id == pedidos.cliente_id)
join_cols.show(truncate=False)

print("Mismo resultado, pero usando condicion explicita de igualdad.")

print("\n" + "=" * 70)
print("[PASO 4] INNER JOIN con alias — evitar ambiguedad")
print("=" * 70)

print("\n--- Usando aliases c y p ---")
join_alias = clientes.alias("c").join(
    pedidos.alias("p"),
    col("c.id") == col("p.cliente_id")
).select(
    col("c.id").alias("cliente_id"),
    col("c.nombre"),
    col("p.id").alias("pedido_id"),
    col("p.producto"),
    col("p.total")
)

join_alias.show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 5] INNER JOIN con SQL")
print("=" * 70)

clientes.createOrReplaceTempView("clientes")
pedidos.createOrReplaceTempView("pedidos")

print("\n--- Join via SQL ---")
spark.sql("""
    SELECT c.id as cliente_id, c.nombre, c.ciudad,
           p.id as pedido_id, p.producto, p.cantidad, p.total
    FROM clientes c
    INNER JOIN pedidos p ON c.id = p.cliente_id
    ORDER BY c.id, p.id
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 6] Filas que se pierden en el INNER JOIN")
print("=" * 70)

print("\n--- LEFT JOIN para ver todo ---")
left_join = clientes.join(pedidos, clientes.id == pedidos.cliente_id, "left")
left_join.show(truncate=False)

print("\n--- Filas con NULL (no hicieron match) ---")
left_join.where(col("pedido_id").isNull()).show(truncate=False)

print("Estas filas SE PIERDEN en un INNER JOIN.")

print("\n" + "=" * 70)
print("[PASO 7] Contar cuantos registros se pierden")
print("=" * 70)

n_clientes = clientes.count()
n_pedidos = pedidos.count()
n_inner = join_cols.count()

print(f"Clientes: {n_clientes}")
print(f"Pedidos:  {n_pedidos}")
print(f"Inner join resultado: {n_inner}")
print(f"Clientes sin pedido:  {n_clientes - n_inner + pedidos.where(~col('cliente_id').isin([1,2,3,5])).count()}")

print("\n--- Clientes que NO tienen pedidos ---")
clientes.join(pedidos, clientes.id == pedidos.cliente_id, "left_anti").show(truncate=False)

print("\n--- Pedidos sin cliente valido ---")
pedidos.join(clientes, clientes.id == pedidos.cliente_id, "left_anti").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 8] INNER JOIN con condicion compuesta")
print("=" * 70)

print("\n--- Join con condicion: id == cliente_id AND total > 100 ---")
join_filtrado = clientes.join(
    pedidos,
    (clientes.id == pedidos.cliente_id) & (pedidos.total > 100)
)
join_filtrado.show(truncate=False)

print("Solo pedidos con total > 100 aparecen en el resultado.")

print("\n" + "=" * 70)
print("[PASO 9] INNER JOIN con condicion OR")
print("=" * 70)

print("\n--- Join: cliente de Madrid O pedido > 200 ---")
join_or = clientes.join(
    pedidos,
    (clientes.id == pedidos.cliente_id) & 
    ((clientes.ciudad == "Madrid") | (pedidos.total > 200))
)
join_or.show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 10] SELF JOIN — tabla consigo misma")
print("=" * 70)

print("\n--- Crear tabla de relaciones (empleado -> gerente) ---")
schema_empleados = StructType([
    StructField("id",       IntegerType(), True),
    StructField("nombre",   StringType(),  True),
    StructField("gerente_id", IntegerType(), True),
])

data_empleados = [
    (1, "Director",  None),
    (2, "Gerente A", 1),
    (3, "Gerente B", 1),
    (4, "Analista",  2),
    (5, "Desarrollador", 2),
    (6, "Disenador", 3),
]

empleados = spark.createDataFrame(data_empleados, schema_empleados)

print("Empleados:")
empleados.show(truncate=False)

print("\n--- Self join: empleado -> gerente ---")
empleados.alias("e").join(
    empleados.alias("g"),
    col("e.gerente_id") == col("g.id"),
    "inner"
).select(
    col("e.nombre").alias("empleado"),
    col("g.nombre").alias("gerente")
).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 11] INNER JOIN con agregacion")
print("=" * 70)

print("\n--- Total de ventas por cliente (solo clientes con pedidos) ---")
clientes.join(
    pedidos,
    clientes.id == pedidos.cliente_id
).groupBy(
    clientes.id, clientes.nombre
).agg(
    count("pedido_id").alias("num_pedidos"),
    round(sum("total"), 2).alias("total_gastado")
).orderBy(col("total_gastado").desc()).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 12] INNER JOIN con多重 tabla")
print("=" * 70)

schema_categorias = StructType([
    StructField("producto",  StringType(), True),
    StructField("categoria", StringType(), True),
])

data_categorias = [
    ("Laptop",    "Electronica"),
    ("Mouse",     "Electronica"),
    ("Teclado",   "Electronica"),
    ("Monitor",   "Electronica"),
    ("Silla",     "Muebles"),
    ("Escritorio","Muebles"),
]

categorias = spark.createDataFrame(data_categorias, schema_categorias)

print("Categorias:")
categorias.show(truncate=False)

print("\n--- Join clientes + pedidos + categorias ---")
clientes.join(pedidos, clientes.id == pedidos.cliente_id) \
    .join(categorias, pedidos.producto == categorias.producto) \
    .select(
        clientes.nombre,
        pedidos.producto,
        categorias.categoria,
        pedidos.total
    ).orderBy(clientes.nombre).show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 13] Verificar plan fisico del join")
print("=" * 70)

join_plan = clientes.join(pedidos, clientes.id == pedidos.cliente_id)
join_plan.explain("formatted")

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 14")
print("=" * 70)

spark.stop()
