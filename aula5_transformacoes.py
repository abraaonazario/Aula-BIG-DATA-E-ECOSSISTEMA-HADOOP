from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder.appName("Aula 5 - Transformações").getOrCreate()
df = spark.read.csv("funcionarios_10000.csv", header=True, inferSchema=True)

df2 = df.withColumn("faixa_salarial", when(df.salario < 5000, "Baixa")
                                     .when(df.salario <= 10000, "Média")
                                     .otherwise("Alta"))
df2.select("nome", "salario", "faixa_salarial").show(10)

spark.stop()