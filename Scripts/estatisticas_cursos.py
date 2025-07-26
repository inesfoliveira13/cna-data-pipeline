# Objetivo
## Analisa quantos alunos se candidataram e quantos foram colocados em cada curso de cada instituição


# Bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Criar a sessão do Spark
spark = SparkSession.builder \
    .appName("Número de Candidatos e Colocados") \
    .getOrCreate()

# Definir o esquema para os arquivos CSV
schema = StructType([
    StructField("codigo_instituicao", StringType(), True),
    StructField("nome_instituicao", StringType(), True),
    StructField("codigo_curso", StringType(), True),
    StructField("nome_curso", StringType(), True)
])

# Caminhos para os arquivos CSV
candidatos_csv = "candidatos_corrigido.csv"  
colocados_csv = "colocados_corrigido.csv"  

# Carregar os arquivos CSV como DataFrames do Spark 
candidatos_df = spark.read.csv(candidatos_csv, header=True, schema=schema)
colocados_df = spark.read.csv(colocados_csv, header=True, schema=schema)

# Agrupar por código de instituição e curso e contar os candidatos
candidatos_por_curso = candidatos_df.groupBy("codigo_instituicao", "nome_instituicao", "codigo_curso", "nome_curso") \
    .count() \
    .withColumnRenamed("count", "numero_candidatos") \
    .withColumn("numero_candidatos", col("numero_candidatos").cast(IntegerType()))

# Agrupar por código de instituição e curso e contar os colocados
colocados_por_curso = colocados_df.groupBy("codigo_instituicao", "nome_instituicao", "codigo_curso", "nome_curso") \
    .count() \
    .withColumnRenamed("count", "numero_colocados") \
    .withColumn("numero_colocados", col("numero_colocados").cast(IntegerType()))

# Fazer o join para combinar as informações de candidatos e colocados
estatisticas_cursos = candidatos_por_curso.join(
    colocados_por_curso,
    on=["codigo_instituicao", "nome_instituicao", "codigo_curso", "nome_curso"],
    how="outer"
)

# Converter DataFrames do Spark para um DataFrame do Pandas antes de guardar como CSV
resultado_pandas = estatisticas_cursos.toPandas()

# Guardar o DataFrame do Pandas como um arquivo CSV sem índice
output_csv = "estatisticas_cursos.csv"
resultado_pandas.to_csv(output_csv, index=False, encoding='utf-8')
print(f"CSV gerado com sucesso em: {output_csv}")

# Terminar a sessão do Spark
spark.stop()

