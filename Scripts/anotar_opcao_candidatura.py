# Objetivo
## Identificar, para cada aluno colocado, qual foi a opção (1ª, 2ª, etc.) com que ele foi colocado


# Bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.functions import lpad

# Criar a sessão do Spark  
spark = SparkSession.builder \
    .appName("Alunos Colocados e correspondência nos Candidatos") \
    .getOrCreate()

# Caminhos para os CSVs de colocados e candidatos
candidatos_csv = "candidatos_corrigido.csv"
colocados_csv = "colocados_corrigido.csv"

# Carregar os CSVs em DataFrames do Spark
colocados_df = spark.read.csv(colocados_csv, header=True, inferSchema=True)
candidatos_df = spark.read.csv(candidatos_csv, header=True, inferSchema=True)

# Garantir que codigo_instituicao tem 4 caracteres (preenchido com zeros à esquerda)
colocados_df = colocados_df.withColumn("codigo_instituicao", lpad("codigo_instituicao", 4, "0"))
candidatos_df = candidatos_df.withColumn("codigo_instituicao", lpad("codigo_instituicao", 4, "0"))

# Realizar o join para manter apenas os alunos que aparecem em ambos os datasets
resultado_df = colocados_df.join(
    candidatos_df,
    on=[
        "codigo_instituicao",  # Combinar por código da instituição
        "nome_instituicao",    # Nome da instituição
        "codigo_curso",        # Código do curso
        "nome_curso",          # Nome do curso
        "nome",                # Nome do aluno
        "fase"                 # Fase 
    ],
    how="inner"
).select(
    colocados_df["codigo_instituicao"],
    colocados_df["nome_instituicao"],
    colocados_df["codigo_curso"],
    colocados_df["nome_curso"],
    colocados_df["nome"],
    candidatos_df["opcao"],  
    colocados_df["fase"]
)

# Guardar o resultado num arquivo CSV
output_csv = "opcao_candidatura.csv"
resultado_df.toPandas().to_csv(output_csv, index=False, encoding="utf-8")
print(f"CSV gerado com sucesso em: {output_csv}")

# Terminar a sessão do Spark
spark.stop()