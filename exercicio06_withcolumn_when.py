# Criação de colunas condicionais com withColumn e when
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Exercicio06_WithColumnWhen")
    .getOrCreate()
)

dados = [("Ana", 85), ("Bruno", 47), ("Carla", 73), ("Daniel", 30)]
df = spark.createDataFrame(dados, ["nome", "pontuacao"])

df.withColumn("resultado", when(df.pontuacao >= 60, "Aprovado").otherwise("Reprovado")).show()

spark.stop()