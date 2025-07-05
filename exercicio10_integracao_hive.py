# Integração com Hive: salvar e consultar tabela Hive
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Exercicio10_IntegracaoHive")
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

dados = [(1, "João"), (2, "Maria"), (3, "Pedro")]
df = spark.createDataFrame(dados, ["id", "nome"])

df.write.mode("overwrite").saveAsTable("pessoas_hive")
spark.sql("SELECT * FROM pessoas_hive").show()

spark.stop()