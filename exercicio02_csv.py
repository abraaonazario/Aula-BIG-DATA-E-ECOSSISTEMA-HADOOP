# Leitura e escrita de arquivos CSV com PySpark
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Exercicio02_CSV")
    .getOrCreate()
)

dados = [("Ana", 34, "São Paulo"), ("Bruno", 28, "Rio"), ("Carla", 45, "São Paulo")]
colunas = ["nome", "idade", "cidade"]
df = spark.createDataFrame(dados, schema=colunas)

df.write.format("csv").option("header", "true").mode("overwrite").save("dados_exemplo.csv")
df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dados_exemplo.csv")

df2.show()
spark.stop()