# Bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import monotonically_increasing_id

# Iniciar sessão Spark
spark = SparkSession.builder \
    .appName("Fases por grupo de 0140") \
    .getOrCreate()

# Definir esquema para preservar zeros à esquerda
schema = StructType([
    StructField("codigo_instituicao", StringType(), True),
    StructField("nome_faculdade", StringType(), True),
    StructField("codigo_curso", StringType(), True),
    StructField("nome_curso", StringType(), True),
    StructField("nota_ultimo_colocado", StringType(), True)
])

# Carregar CSV como DataFrame
df = spark.read.csv("nota_ultimo_colocado.csv", header=True, schema=schema)

# Adicionar ID de linha para manter a ordem original
df = df.withColumn("row_id", monotonically_increasing_id())

# Converter para RDD para aplicar lógica linha a linha
rdd = df.sort("row_id").rdd

# Aplicar lógica de fases em RDD
def atribuir_fases(rdd):
    fase_num = 0
    fase_atual = ""
    em_novo_grupo = True

    resultado = []

    for row in rdd.collect():
        cod = row["codigo_instituicao"]
        if cod == "0140":
            if em_novo_grupo:
                fase_num += 1
                fase_atual = f"{fase_num}ª fase"
                em_novo_grupo = False
        else:
            em_novo_grupo = True
        resultado.append((row["codigo_instituicao"], row["nome_faculdade"], row["codigo_curso"],
                          row["nome_curso"], row["nota_ultimo_colocado"], fase_atual, row["row_id"]))
    return resultado

# Aplicar transformação
resultado_rdd = spark.sparkContext.parallelize(atribuir_fases(rdd))

# Criar novo DataFrame com a coluna 'fase'
resultado_df = resultado_rdd.toDF([
    "codigo_instituicao", "nome_faculdade", "codigo_curso",
    "nome_curso", "nota_ultimo_colocado", "fase", "row_id"
])

# Converter para Pandas e guardar CSV
resultado_pandas = resultado_df.drop("row_id").toPandas()

# Guardar CSV com encoding utf-8 e sem índice
output_csv = "nota_ultimo_colocado.csv"
resultado_pandas.to_csv(output_csv, index=False, encoding="utf-8")
print(f"CSV gerado com sucesso em: {output_csv}")

# Terminar a sessão do Spark
spark.stop()