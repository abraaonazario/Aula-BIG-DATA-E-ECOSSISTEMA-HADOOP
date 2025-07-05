from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Aula 7 - Escrita de Arquivo").getOrCreate()
df = spark.read.csv("funcionarios_10000.csv", header=True, inferSchema=True)

salario_por_departamento = df.groupBy("departamento").avg("salario")
salario_por_departamento.write.csv("saida_salario_departamento", header=True, mode="overwrite")

spark.stop()