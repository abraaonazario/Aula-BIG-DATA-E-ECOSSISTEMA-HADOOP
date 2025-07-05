# Consultas SQL com Spark SQL e visões temporárias
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Exercicio07_SparkSQL")
    .getOrCreate()
)

pessoas = [(1, "Alice", 101), (2, "Bruno", 102), (3, "Clara", 101)]
cidades = [(101, "São Paulo"), (102, "Rio de Janeiro"), (103, "Curitiba")]

df_pessoas = spark.createDataFrame(pessoas, ["id", "nome", "cidade_id"])
df_cidades = spark.createDataFrame(cidades, ["cidade_id", "cidade"])

df_pessoas.createOrReplaceTempView("pessoas")
df_cidades.createOrReplaceTempView("cidades")

spark.sql("""
    SELECT p.id, p.nome, c.cidade
    FROM pessoas p
    JOIN cidades c ON p.cidade_id = c.cidade_id
    WHERE c.cidade = 'São Paulo'
""").show()

spark.stop()