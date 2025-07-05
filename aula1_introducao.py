from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Aula 1 - Introducao").getOrCreate()

df = spark.createDataFrame([(1, "João"), (2, "Maria")], ["id", "nome"])
df.show()

spark.stop()