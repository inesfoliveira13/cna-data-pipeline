# Objetivo
## Identificar os alunos que foram colocados num curso que não era a sua primeira opção, e depois mostrar qual era de facto a sua primeira opção.


# Bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.functions import lpad

# Criar a sessão do Spark
spark = SparkSession.builder \
    .appName("Curso da Primeira Opcao dos Alunos") \
    .getOrCreate()

# Caminhos para os CSVs
colocados_com_respetiva_opcao_csv = "opcao_candidatura.csv"
candidatos_csv = "candidatos_corrigido.csv"

# Carregar os datasets
colocados_df = spark.read.csv(colocados_com_respetiva_opcao_csv, header=True, inferSchema=True)
candidatos_df = spark.read.csv(candidatos_csv, header=True, inferSchema=True)

# Garantir que codigo_instituicao tem 4 caracteres (preenchido com zeros à esquerda)
colocados_df = colocados_df.withColumn("codigo_instituicao", lpad("codigo_instituicao", 4, "0"))
candidatos_df = candidatos_df.withColumn("codigo_instituicao", lpad("codigo_instituicao", 4, "0"))

# Garante que opcao é tratado como número
colocados_df = colocados_df.withColumn("opcao", colocados_df["opcao"].cast("int"))
candidatos_df = candidatos_df.withColumn("opcao", candidatos_df["opcao"].cast("int"))

# Filtrar alunos que entraram numa opção diferente de 1 (primeira opção))
nao_primeira_opcao_df = colocados_df.filter(colocados_df["opcao"] != "1")

# Filtrar candidatos que entraram na opção 1 (primeira opção)
primeira_opcao_df = candidatos_df.filter(candidatos_df["opcao"] == "1")

# Fazer o join entre os datasets pelo nome, instituição e fase
resultado_df = nao_primeira_opcao_df.join(
    primeira_opcao_df,
    on=[
        "nome",                # Combinar pelo nome do aluno
        "codigo_instituicao",  # Código da instituição
        "fase"                 # Fase de candidatura
    ],
    how="inner" 
).select(
    nao_primeira_opcao_df["nome"],
    nao_primeira_opcao_df["codigo_instituicao"],
    nao_primeira_opcao_df["nome_instituicao"],
    nao_primeira_opcao_df["codigo_curso"].alias("curso_colocado"),
    nao_primeira_opcao_df["nome_curso"].alias("curso_colocado_nome"),
    nao_primeira_opcao_df["opcao"].alias("opcao_colocado"),
    primeira_opcao_df["codigo_curso"].alias("curso_primeira_opcao"),
    primeira_opcao_df["nome_curso"].alias("curso_primeira_opcao_nome"),
    nao_primeira_opcao_df["fase"]
)

# Guardar o resultado num arquivo CSV
output_csv = "primeira_opcao_vs_colocado.csv"
resultado_df.toPandas().to_csv(output_csv, index=False, encoding="utf-8")
print(f"CSV gerado com sucesso: {output_csv}")

# Terminar a sessão do Spark
spark.stop()
