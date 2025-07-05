# Filtros, seleções e ordenações em DataFrames com PySpark
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Exercicio03_FiltrosSelecoesOrdenacoes")
    .getOrCreate()
)

dados = [(1, "Ana", 23, "SP"), (2, "Bruno", 35, "RJ"), (3, "Carlos", 17, "SP"), (4, "Daniela", 40, "PR")]
colunas = ["id", "nome", "idade", "cidade"]
df = spark.createDataFrame(dados, schema=colunas)

df.filter(df.idade >= 30).select("nome", "cidade").orderBy("nome").show()
spark.stop()