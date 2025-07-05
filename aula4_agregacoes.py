from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, countDistinct

spark = SparkSession.builder.appName("Aula 4 - Agregações").getOrCreate()
df = spark.read.csv("funcionarios_10000.csv", header=True, inferSchema=True)

df.groupBy("departamento").agg(avg("salario").alias("media")).show()
df.select(max("salario"), min("salario")).show()
df.select(countDistinct("departamento")).show()

spark.stop()