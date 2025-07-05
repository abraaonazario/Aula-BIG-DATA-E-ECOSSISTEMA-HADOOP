# Junção entre DataFrames usando PySpark
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Exercicio04_JoinDataFrames")
    .getOrCreate()
)

pessoas = [(1, "Alice", 101), (2, "Bruno", 102), (3, "Carla", 101)]
cidades = [(101, "São Paulo"), (102, "Rio de Janeiro"), (103, "Curitiba")]

df_pessoas = spark.createDataFrame(pessoas, ["id", "nome", "cidade_id"])
df_cidades = spark.createDataFrame(cidades, ["cidade_id", "cidade"])

df_join = df_pessoas.join(df_cidades, on="cidade_id", how="inner")
df_join.show()

spark.stop()