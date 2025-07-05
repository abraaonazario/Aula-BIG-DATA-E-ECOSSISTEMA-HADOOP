from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Aula 3 - Operações Básicas").getOrCreate()
df = spark.read.csv("funcionarios_10000.csv", header=True, inferSchema=True)

df.filter(df.salario > 8000).show(5)
df.select("nome", "salario").orderBy("salario", ascending=False).show(5)
df.groupBy("departamento").count().show()

spark.stop()