# Integração com banco de dados relacional via JDBC
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Exercicio11_IntegracaoJDBC")
    .getOrCreate()
)

url = "jdbc:postgresql://servidor:5432/meu_banco"
properties = {"user": "meu_usuario", "password": "minha_senha"}
df = spark.read.jdbc(url=url, table="schema.tabela_entrada", properties=properties)
df.show()
df.filter("idade >= 18").write.jdbc(url=url, table="schema.tabela_saida", mode="overwrite", properties=properties)

spark.stop()