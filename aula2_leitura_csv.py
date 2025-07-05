from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Aula 2 - Leitura CSV").getOrCreate()

df = spark.read.csv("funcionarios_10000.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)

spark.stop()