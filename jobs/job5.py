from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim, when

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
    .withColumn("idade", col("idade").cast("integer")) \
    .withColumn("faixa_etaria", when(col("idade") < 18, "Jovem")
                .when((col("idade") >= 18) & (col("idade") <= 60), "Adulto")
                .otherwise("Idoso"))
    
df_clean.createOrReplaceTempView("pessoas")

resultado = spark.sql("SELECT * FROM pessoas WHERE faixa_etaria = 'Adulto' OR faixa_etaria = 'Idoso'")

resultado.show()

resultado.write.mode('overwrite').csv('data/resultado_pipe5', header=True)

spark.stop()