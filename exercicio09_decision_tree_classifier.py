# Classificação com DecisionTreeClassifier em PySpark
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Exercicio09_DecisionTreeClassifier")
    .getOrCreate()
)

dados = [(0, 0.1, 0.2), (0, 0.4, 0.3), (0, 0.2, 0.4),
         (1, 0.6, 0.7), (1, 0.8, 0.4), (1, 0.3, 0.9), (1, 0.7, 0.8)]
df = spark.createDataFrame(dados, ["label", "feature1", "feature2"])

vec = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
df_vec = vec.transform(df)

dt = DecisionTreeClassifier(featuresCol="features", labelCol="label")
modelo = dt.fit(df_vec)

modelo.transform(df_vec).select("feature1", "feature2", "label", "prediction").show()

spark.stop()