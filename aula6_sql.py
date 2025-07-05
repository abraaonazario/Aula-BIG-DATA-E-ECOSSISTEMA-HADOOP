from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Aula 6 - SQL com Spark").getOrCreate()
df = spark.read.csv("funcionarios_10000.csv", header=True, inferSchema=True)

df.createOrReplaceTempView("funcionarios")

resultado = spark.sql("SELECT departamento, AVG(salario) as media FROM funcionarios GROUP BY departamento")
resultado.show()

spark.stop()