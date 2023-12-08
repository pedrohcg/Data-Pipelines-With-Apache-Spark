import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Pipeline de Exploracao de dados com Spark SQL - Job 2") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

df = spark.read.csv('./data/dados1_cap03.csv', header = True, inferSchema = True)

df.createOrReplaceTempView("vendas")

resultado = spark.sql("""
                      SELECT ano, funcionario, SUM(unidades_vendidas) total_unidades_vendidas
                      FROM vendas
                      GROUP BY ano, funcionario
                      ORDER BY ano, funcionario
                      """)

resultado.write.csv('./data/resultado_pipe2', header=True, mode='overwrite')

resultado_pd = resultado.toPandas()

plt.figure(figsize=(10, 6))
plt.bar(resultado_pd['funcionario'], resultado_pd['total_unidades_vendidas'])
plt.xlabel('Funcionario')
plt.ylabel('Total Unidades Vendidas')
plt.title('Vendas por Funcionario Anuais')

plt.savefig('data/resultado_pipe2/pipe2.png')

spark.stop()