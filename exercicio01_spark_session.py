# Criação de SparkSession e DataFrame simples
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Exercicio01_SparkSession")
    .getOrCreate()
)

dados = [(1, "Alice", 29), (2, "Bob", 31), (3, "Cathy", 25)]
colunas = ["id", "nome", "idade"]
df = spark.createDataFrame(dados, schema=colunas)

df.printSchema()
df.show()

spark.stop()