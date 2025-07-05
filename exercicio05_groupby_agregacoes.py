# Agrupamentos e agregações (avg, count, max, min)
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, max, min

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Exercicio05_GroupByAgregacoes")
    .getOrCreate()
)

dados = [("A", 10), ("B", 20), ("A", 30), ("B", 40), ("A", 50), ("C", 60)]
df = spark.createDataFrame(dados, ["categoria", "valor"])

df.groupBy("categoria").agg(
    count("*").alias("contagem"),
    avg("valor").alias("media"),
    max("valor").alias("maximo"),
    min("valor").alias("minimo")
).show()

spark.stop()