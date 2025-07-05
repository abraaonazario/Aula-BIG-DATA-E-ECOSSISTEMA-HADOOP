# Regressão Linear com PySpark MLlib
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import LinearRegression

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Exercicio08_RegressaoLinear")
    .getOrCreate()
)

dados = [(Vectors.dense([0.0]), 1.0), (Vectors.dense([1.0]), 3.0),
         (Vectors.dense([2.0]), 5.0), (Vectors.dense([3.0]), 7.0), (Vectors.dense([4.0]), 9.0)]
df = spark.createDataFrame(dados, ["features", "label"])

lr = LinearRegression(featuresCol="features", labelCol="label")
modelo = lr.fit(df)

print(f"Coeficiente: {modelo.coefficients[0]:.2f}")
print(f"Intercepto: {modelo.intercept:.2f}")
modelo.transform(df).select("features", "label", "prediction").show()

spark.stop()