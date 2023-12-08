from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Pipeline de Exploracao de dados com Spark SQL") \
    .getOrCreate()

# Configura o nivel de log para mostrar apenas erros    
spark.sparkContext.setLogLevel('ERROR')

df = spark.read.csv('./data/dados1_cap03.csv', header = True, inferSchema = True)

df.show()

print(f"Total de registros: {df.count()}")
print("Distribuicao de registros por ano:")
df.groupBy("ano").count().show()

df.createOrReplaceTempView("vendas")

resultado = spark.sql("""
                      SELECT ano, ROUND(AVG(unidades_vendidas), 2) media_unidades_vendidas
                      FROM vendas
                      GROUP BY ano
                      ORDER BY media_unidades_vendidas DESC
                      """)

resultado.show()

resultado.write.csv('./data/resultado_pipe1', header=True)

spark.stop()