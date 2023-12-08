from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim

spark = SparkSession.builder \
    .appName("Pipeline de Exploracao de dados com Spark SQL - Job 4") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel('ERROR')

df = spark.read.text('./data/dados2_cap03.txt')

df_clean = df.select(
    split(col("value"), ",")[0].alias("id"),
    split(col("value"), ",")[1].alias("nome"),
    split(col("value"), ",")[2].alias("idade")
)

df_clean.show()

df_clean = df_clean.withColumn("nome", trim(col("nome"))) \
    .withColumn("idade", col("idade").cast("integer"))

df_clean.createOrReplaceTempView("pessoas")

resultado = spark.sql("SELECT * FROM pessoas WHERE idade > 30")

resultado.show()

resultado.write.mode('overwrite').csv('data/resultado_pipe4', header=True)

spark.stop()