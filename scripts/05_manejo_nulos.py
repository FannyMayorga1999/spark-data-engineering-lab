import os
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, sum as _sum, when, count, isnan, isnull, avg, round as _round, lit, coalesce, lag

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("ManejoNulos") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 05: MANEJO DE NULLOS")
print("=" * 70)

print("\n[PASO 1] Creando DataFrame con valores nulos intencionalmente...\n")

schema = StructType([
    StructField("id",           IntegerType(), True),
    StructField("nombre",       StringType(),  True),
    StructField("departamento", StringType(),  True),
    StructField("salario",      DoubleType(),  True),
    StructField("edad",         IntegerType(), True),
    StructField("fecha_ingreso", StringType(),   True),
])

data = [
    (1,  "Alice",   "Ingenieria", 50000.0, 34, "2020-01-15"),
    (2,  "Bob",     "Ventas",     None,     45, "2019-11-20"),
    (3,  "Carlos",  "Ingenieria", 45000.0, None, "2021-03-10"),
    (4,  "Diana",   None,         55000.0, 38, "2018-08-05"),
    (5,  None,      "Ventas",     60000.0, 27, "2022-06-01"),
    (6,  "Frank",   "Ingenieria", 52000.0, 29, None),
    (7,  "Gloria",  "RH",         None,     42, "2020-02-14"),
    (8,  "Hector",  "Ventas",     58000.0, 31, None),
    (9,  "Irene",   None,         None,     None, None),
    (10, "Juan",    "RH",         48000.0, 36, "2019-12-01"),
]

df = spark.createDataFrame(data, schema)

print("DataFrame original:")
df.show(10, truncate=False)
print(f"\nTotal filas: {df.count()}")
print(f"Total columnas: {len(df.columns)}")

print("\n" + "=" * 70)
print("[PASO 2] Contando valores nulos por columna")
print("=" * 70)



print("\nMetodo 1: Recorriendo cada columna con isNull()")
for c in df.columns:
    nulos = df.filter(col(c).isNull()).count()
    print(f"  {c:20s} -> {nulos} nulos")

print("\nMetodo 2: Usando expresion SQL con sum(when(...))")
df.select(
    count("*").alias("total_filas"),
    _sum(when(col("id").isNull(), 1).otherwise(0)).alias("nulos_id"),
    _sum(when(col("nombre").isNull(), 1).otherwise(0)).alias("nulos_nombre"),
    _sum(when(col("departamento").isNull(), 1).otherwise(0)).alias("nulos_departamento"),
    _sum(when(col("salario").isNull(), 1).otherwise(0)).alias("nulos_salario"),
    _sum(when(col("edad").isNull(), 1).otherwise(0)).alias("nulos_edad"),
    _sum(when(col("fecha_ingreso").isNull(), 1).otherwise(0)).alias("nulos_fecha_ingreso"),
).show()

print("\nMetodo 3: Usando SQL nativo")
df.createOrReplaceTempView("empleados")
spark.sql("""
    SELECT
        COUNT(*) AS total_filas,
        SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS nulos_id,
        SUM(CASE WHEN nombre IS NULL THEN 1 ELSE 0 END) AS nulos_nombre,
        SUM(CASE WHEN departamento IS NULL THEN 1 ELSE 0 END) AS nulos_depto,
        SUM(CASE WHEN salario IS NULL THEN 1 ELSE 0 END) AS nulos_salario,
        SUM(CASE WHEN edad IS NULL THEN 1 ELSE 0 END) AS nulos_edad,
        SUM(CASE WHEN fecha_ingreso IS NULL THEN 1 ELSE 0 END) AS nulos_fecha
    FROM empleados
""").show()

print("\n" + "=" * 70)
print("[PASO 3] dropna() — eliminar filas con nulos")
print("=" * 70)

print("\ndropna() sin argumentos (elimina fila si CUALQUIER columna es nula):")
df_sin_nulos = df.dropna()
df_sin_nulos.show(10, truncate=False)
print(f"Filas restantes: {df_sin_nulos.count()} (se eliminaron {df.count() - df_sin_nulos.count()})")

print("\ndropna(thresh=4) (elimina fila si tiene < 4 valores no-nulos):")
df_thresh = df.dropna(thresh=4)
df_thresh.show(10, truncate=False)
print(f"Filas restantes: {df_thresh.count()} (se eliminaron {df.count() - df_thresh.count()})")

print("\ndropna(subset=['salario', 'edad']) (elimina fila si salario O edad son nulos):")
df_subset = df.dropna(subset=["salario", "edad"])
df_subset.show(10, truncate=False)
print(f"Filas restantes: {df_subset.count()} (se eliminaron {df.count() - df_subset.count()})")

print("\ndropna(how='all') (elimina fila solo si TODAS las columnas son nulas):")
df_all = df.dropna(how="all")
df_all.show(10, truncate=False)
print(f"Filas restantes: {df_all.count()} (se eliminaron {df.count() - df_all.count()})")

print("\n" + "=" * 70)
print("[PASO 4] fillna() — rellenar valores nulos")
print("=" * 70)

print("\nfillna() con valor fijo en todas las columnas:")
df_fijo = df.fillna("SIN_DATO")
df_fijo.show(10, truncate=False)

print("\nfillna() con valor especifico por columna (diccionario):")
df_dict = df.fillna({
    "nombre": "DESCONOCIDO",
    "departamento": "SIN_DEPTO",
    "salario": 0.0,
    "edad": 0,
})
df_dict.show(10, truncate=False)

print("\nfillna() con la media del salario:")
media_salario = df.select(avg("salario")).collect()[0][0]
print(f"  Salario promedio (ignorando nulos): {media_salario:.2f}")
df_media = df.fillna({"salario": _round(media_salario, 2)})
df_media.show(10, truncate=False)

print("\n" + "=" * 70)
print("[PASO 5] drop() — eliminar columnas")
print("=" * 70)

print("\ndrop() columnas con demasiados nulos (ej: eliminar 'fecha_ingreso'):")
df_sin_fecha = df.drop("fecha_ingreso")
print("Columnas restantes:", df_sin_fecha.columns)
df_sin_fecha.show(10, truncate=False)

print("\n" + "=" * 70)
print("[PASO 6] isnull() / isNotNull() para filtrar")
print("=" * 70)

print("\nSolo filas donde 'nombre' NO es nulo:")
df.filter(col("nombre").isNotNull()).show(10, truncate=False)

print("\nSolo filas donde 'departamento' ES nulo:")
df.filter(col("departamento").isNull()).show(10, truncate=False)

print("\n" + "=" * 70)
print("[PASO 7] Forward fill — arrastrar valor anterior")
print("=" * 70)

print("\nUsando last() con Window para rellenar con el valor anterior (ordenado por id):")
w = Window.orderBy("id").rowsBetween(Window.unboundedPreceding, 0)
df_ffill = df.withColumn("nombre_ffill", coalesce(col("nombre"), last(col("nombre"), True).over(w)))
df_ffill.select("id", "nombre", "nombre_ffill").show(10, truncate=False)

print("\n" + "=" * 70)
print("[PASO 8] Ejemplo realista: limpiar datos")
print("=" * 70)

print("\nPipeline de limpieza tipico:")
df_limpio = df \
    .dropna(subset=["id"]) \
    .fillna({
        "nombre": "DESCONOCIDO",
        "departamento": "SIN_ASIGNAR",
        "edad": 0,
    }) \
    .fillna({"salario": media_salario})

df_limpio.show(10, truncate=False)
print(f"\nFilas originales: {df.count()} -> Filas finales: {df_limpio.count()}")

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 05")
print("=" * 70)

spark.stop()
