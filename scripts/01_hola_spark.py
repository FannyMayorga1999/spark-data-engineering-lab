from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HolaSpark") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print(f"Spark version: {spark.version}")

data = [("Alice", 34), ("Bob", 45), ("Carlos", 29), ("Diana", 38)]
columns = ["Nombre", "Edad"]
df = spark.createDataFrame(data, columns)

df.show()

df_mayores = df.filter(df.Edad > 30)
df_mayores.show()

df.createOrReplaceTempView("personas")
resultado = spark.sql("SELECT Nombre, Edad FROM personas WHERE Edad BETWEEN 30 AND 40")
resultado.show()

print(f"Total filas: {df.count()}")
spark.stop()
