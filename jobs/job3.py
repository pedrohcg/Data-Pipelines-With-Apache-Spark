import io
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Pipeline de Exploracao de dados com Spark SQL - Job 3") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel('ERROR')

df = spark.read.csv('./data/dados1_cap03.csv', header = True, inferSchema = True)

df.createOrReplaceTempView("vendas")

resultado = spark.sql("""
                      WITH vendas_agregadas AS (
                          SELECT ano, funcionario, SUM(unidades_vendidas) total_unidades_vendidas
                          FROM vendas
                          GROUP BY ano, funcionario
                      ), total_ano AS (
                          SELECT ano, SUM(total_unidades_vendidas) total_unidades_ano
                          FROM vendas_agregadas
                          GROUP BY ano
                      )
                      SELECT v.ano, v.funcionario, v.total_unidades_vendidas, t.total_unidades_ano, ROUND(v.total_unidades_vendidas / t.total_unidades_ano * 100, 2) proporcional_func_ano
                      FROM vendas_agregadas v
                      JOIN total_ano t
                      ON v.ano = t.ano
                      ORDER BY v.ano, v.funcionario;
                       """)

resultado.show()

resultado.write.mode('overwrite').csv('data/resultado_pipe3', header=True)

resultado_explain = spark.sql("""
                      WITH vendas_agregadas AS (
                          SELECT ano, funcionario, SUM(unidades_vendidas) total_unidades_vendidas
                          FROM vendas
                          GROUP BY ano, funcionario
                      ), total_ano AS (
                          SELECT ano, SUM(total_unidades_vendidas) total_unidades_ano
                          FROM vendas_agregadas
                          GROUP BY ano
                      )
                      SELECT v.ano, v.funcionario, v.total_unidades_vendidas, t.total_unidades_ano, ROUND(v.total_unidades_vendidas / t.total_unidades_ano * 100, 2) proporcional_func_ano
                      FROM vendas_agregadas v
                      JOIN total_ano t
                      ON v.ano = t.ano
                      ORDER BY v.ano, v.funcionario;
                       """)

old_stdout = sys.stdout
new_stdout = io.StringIO()
sys.stdout = new_stdout

resultado_explain.explain()

sys.stdout = old_stdout
plano_execucao = new_stdout.getvalue()

with open('data/resultado_pipe3/plano_execucao.txt', 'w') as file:
    file.write(plano_execucao)

spark.stop()