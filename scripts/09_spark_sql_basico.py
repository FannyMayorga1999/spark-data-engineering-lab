import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
if "hadoop" not in os.environ.get("PATH", "").lower():
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")

spark = SparkSession.builder \
    .appName("SparkSQLBasico") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("=" * 70)
print("EJERCICIO 09: SPARK SQL BASICO")
print("=" * 70)

print("\n[PASO 1] Creando DataFrame y vista temporal...\n")

schema = StructType([
    StructField("id",            IntegerType(), True),
    StructField("nombre",        StringType(),  True),
    StructField("departamento",  StringType(),  True),
    StructField("salario",       DoubleType(),  True),
    StructField("edad",          IntegerType(), True),
    StructField("ciudad",        StringType(),  True),
])

data = [
    (1,  "Alice",   "Ingenieria", 50000.0, 34, "Madrid"),
    (2,  "Bob",     "Ventas",     60000.0, 45, "Barcelona"),
    (3,  "Carlos",  "Ingenieria", 45000.0, 29, "Madrid"),
    (4,  "Diana",   "RH",         55000.0, 38, "Valencia"),
    (5,  "Eva",     "Ventas",     62000.0, 27, "Barcelona"),
    (6,  "Frank",   "Ingenieria", 52000.0, 42, "Sevilla"),
    (7,  "Gloria",  "RH",         48000.0, 31, "Madrid"),
    (8,  "Hector",  "Ventas",     58000.0, 36, "Barcelona"),
    (9,  "Irene",   "Marketing",  51000.0, 33, "Valencia"),
    (10, "Juan",    "Marketing",  47000.0, 28, "Sevilla"),
    (11, "Karina",  "Ingenieria", 53000.0, 39, "Madrid"),
    (12, "Luis",    "RH",         49000.0, 41, "Barcelona"),
]

df = spark.createDataFrame(data, schema)

print("DataFrame original:")
df.show(12, truncate=False)

print("\nCrear vista temporal 'empleados':")
df.createOrReplaceTempView("empleados")

print("\n" + "=" * 70)
print("[PASO 2] SELECT basico")
print("=" * 70)

print("\nSeleccionar todas las columnas:")
spark.sql("SELECT * FROM empleados").show(truncate=False)

print("\nSeleccionar nombre y salario:")
spark.sql("SELECT nombre, salario FROM empleados").show(truncate=False)

print("\nSeleccionar con alias:")
spark.sql("SELECT nombre AS empleado, salario AS sueldo FROM empleados").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 3] WHERE — filtrar registros")
print("=" * 70)

print("\nEmpleados de Ingenieria:")
spark.sql("SELECT * FROM empleados WHERE departamento = 'Ingenieria'").show(truncate=False)

print("\nEmpleados con salario > 55000:")
spark.sql("SELECT * FROM empleados WHERE salario > 55000").show(truncate=False)

print("\nCondiciones compuestas (AND/OR):")
spark.sql("""
    SELECT * FROM empleados
    WHERE departamento = 'Ventas' AND ciudad = 'Barcelona'
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 4] ORDER BY — ordenar resultados")
print("=" * 70)

print("\nOrdenar por salario descendente:")
spark.sql("SELECT * FROM empleados ORDER BY salario DESC").show(truncate=False)

print("\nOrdenar por departamento y luego salario:")
spark.sql("SELECT * FROM empleados ORDER BY departamento ASC, salario DESC").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 5] LIMIT — limitar filas")
print("=" * 70)

print("\nTop 5 empleados con mayor salario:")
spark.sql("SELECT * FROM empleados ORDER BY salario DESC LIMIT 5").show(truncate=False)

print("\nPrimeros 3 registros:")
spark.sql("SELECT * FROM empleados LIMIT 3").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 6] GROUP BY y COUNT")
print("=" * 70)

print("\nCantidad por departamento:")
spark.sql("SELECT departamento, COUNT(*) AS total FROM empleados GROUP BY departamento").show(truncate=False)

print("\nCantidad por ciudad:")
spark.sql("SELECT ciudad, COUNT(*) AS total FROM empleados GROUP BY ciudad ORDER BY total DESC").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 7] Funciones de agregacion")
print("=" * 70)

print("\nSalario promedio por departamento:")
spark.sql("SELECT departamento, AVG(salario) AS salario_promedio FROM empleados GROUP BY departamento").show(truncate=False)

print("\nSalario minimo y maximo:")
spark.sql("SELECT MIN(salario) AS minimo, MAX(salario) AS maximo FROM empleados").show(truncate=False)

print("\nSuma total de salarios:")
spark.sql("SELECT SUM(salario) AS total_salarios FROM empleados").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 8] DISTINCT — valores unicos")
print("=" * 70)

print("\nDepartamentos distintos:")
spark.sql("SELECT DISTINCT departamento FROM empleados ORDER BY departamento").show(truncate=False)

print("\nCiudades distintas:")
spark.sql("SELECT DISTINCT ciudad FROM empleados ORDER BY ciudad").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 9] PASAR VARIABLES DE PYTHON A SQL")
print("=" * 70)

min_salario = 50000
departamento_filtro = "Ingenieria"

print(f"\nEmpleados con salario > {min_salario} en {departamento_filtro}:")
spark.sql(f"""
    SELECT * FROM empleados
    WHERE salario > {min_salario} AND departamento = '{departamento_filtro}'
""").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 10] BETWEEN, IN, LIKE")
print("=" * 70)

print("\nEmpleados con salario BETWEEN 48000 y 55000:")
spark.sql("SELECT * FROM empleados WHERE salario BETWEEN 48000 AND 55000").show(truncate=False)

print("\nEmpleados IN ('Madrid', 'Barcelona'):")
spark.sql("SELECT * FROM empleados WHERE ciudad IN ('Madrid', 'Barcelona')").show(truncate=False)

print("\nEmpleados cuyo nombre empieza con 'A' (LIKE):")
spark.sql("SELECT * FROM empleados WHERE nombre LIKE 'A%'").show(truncate=False)

print("\n" + "=" * 70)
print("[PASO 11] CASE WHEN en SQL")
print("=" * 70)

print("\nClasificar salario:")
spark.sql("""
    SELECT nombre, salario,
        CASE
            WHEN salario >= 58000 THEN 'Alto'
            WHEN salario >= 50000 THEN 'Medio'
            ELSE 'Bajo'
        END AS clasificacion
    FROM empleados
""").show(truncate=False)

print("\n" + "=" * 70)
print("FIN DEL EJERCICIO 09")
print("=" * 70)

spark.stop()
