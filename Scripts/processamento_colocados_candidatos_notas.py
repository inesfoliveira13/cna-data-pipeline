# Bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, length, lpad, lower, trim
import pandas as pd

# Criar SparkSession
spark = SparkSession.builder.appName("TratamentoCSV").getOrCreate()



## 1. "nota_ultimo_colocado.csv"
df = spark.read.option("header", True).csv("nota_ultimo_colocado.csv")

# Encontrar colunas que contenham "a)"
colunas_com_a_parentese = [coluna for coluna in df.columns if df.filter(col(coluna) == "a)").count() > 0]
print("Colunas que contêm 'a)':", colunas_com_a_parentese)

# Substituir "a)" por "NÃO DISPONÍVEL"
if 'nota_ultimo_colocado' in df.columns:
    df = df.withColumn(
        "nota_ultimo_colocado",
        when(col("nota_ultimo_colocado") == "a)", "NÃO DISPONÍVEL").otherwise(col("nota_ultimo_colocado"))
    )

# Preencher zeros à esquerda em 'codigo_instituicao'
if 'codigo_instituicao' in df.columns:
    df = df.withColumn("codigo_instituicao", lpad(col("codigo_instituicao"), 4, "0"))

# Converter para Pandas e guardar
resultado_pandas = df.toPandas()
output_csv = "nota_ultimo_colocado_corrigido.csv"
resultado_pandas.to_csv(output_csv, index=False, encoding="utf-8")
print(f"CSV gerado com sucesso em: {output_csv}")



## 2. "candidatos.csv"
df = spark.read.option("header", True).csv("candidatos.csv")

# Detectar coluna 'nome'
coluna_nome = next((c for c in df.columns if c.strip().lower() == "nome"), None)

if coluna_nome:
    linhas_nome = df.filter(col(coluna_nome) == "Nome").count()
    linhas_nulas = df.filter(col(coluna_nome).isNull()).count()

    print(f"Linhas com valor 'Nome': {linhas_nome}")
    print(f"Linhas com valor nulo: {linhas_nulas}")

    # Remover essas linhas
    df_corrigido = df.filter(~((col(coluna_nome) == "Nome") | col(coluna_nome).isNull()))

    # Preencher zeros à esquerda
    if 'codigo_instituicao' in df_corrigido.columns:
        df_corrigido = df_corrigido.withColumn("codigo_instituicao", lpad(col("codigo_instituicao"), 4, "0"))

    # Guardar com Pandas
    resultado_pandas = df_corrigido.toPandas()
    output_csv = "candidatos_corrigido.csv"
    resultado_pandas.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"CSV gerado com sucesso em: {output_csv}")
else:
    print("Coluna 'nome' não encontrada.")



## 3. "colocados.csv"
df = spark.read.option("header", True).csv("colocados.csv")

# Detectar coluna 'nome'
coluna_nome = next((c for c in df.columns if c.strip().lower() == "nome"), None)

if not coluna_nome:
    print("Coluna 'nome' não encontrada.")
else:
    count_nome_literal = df.filter(col(coluna_nome) == "Nome").count()
    print(f"Linhas com valor exatamente 'Nome': {count_nome_literal}")

    # Remover essas linhas
    df_corrigido = df.filter(col(coluna_nome) != "Nome")

    # Agrupamento
    colunas_agrupamento = ['codigo_instituicao', 'nome_instituicao', 'codigo_curso', 'nome_curso', 'fase']
    em_falta = [c for c in colunas_agrupamento if c not in df_corrigido.columns]

    if em_falta:
        print(f"Colunas em falta: {em_falta}")
    else:
        # Contar por grupo
        contagem = df_corrigido.groupBy(*colunas_agrupamento).count().withColumnRenamed("count", "quantidade")

        # Juntar de volta
        df_corrigido = df_corrigido.join(contagem, on=colunas_agrupamento, how="left")

        # Substituir 'nome' por 'SEM REGISTOS DE COLOCAÇÃO' se quantidade == 1
        df_corrigido = df_corrigido.withColumn(
            coluna_nome,
            when((col("quantidade") == 1), "SEM REGISTOS DE COLOCAÇÃO").otherwise(col(coluna_nome))
        )

        # Remover linhas com 'webmaster'
        count_webmaster = df_corrigido.filter(col(coluna_nome) == "webmaster").count()
        print(f"Linhas com 'webmaster': {count_webmaster}")
        df_corrigido = df_corrigido.filter(col(coluna_nome) != "webmaster")

        # Remover coluna auxiliar
        df_corrigido = df_corrigido.drop("quantidade")
        
        # Reordenar colunas: colocar 'nome' antes de 'fase'
        todas_colunas = df_corrigido.columns
        if "nome" in todas_colunas and "fase" in todas_colunas:
            colunas_ordenadas = [
                "codigo_instituicao", "nome_instituicao", "codigo_curso", "nome_curso", "nome", "fase"
            ] + [c for c in todas_colunas if c not in {"codigo_instituicao", "nome_instituicao", "codigo_curso", "nome_curso", "nome", "fase"}]
            df_corrigido = df_corrigido.select(*colunas_ordenadas)


        # Guardar com Pandas
        resultado_pandas = df_corrigido.toPandas()
        output_csv = "colocados_corrigido.csv"
        resultado_pandas.to_csv(output_csv, index=False, encoding="utf-8")
        print(f"CSV gerado com sucesso em: {output_csv}")

# Finalizar Spark
spark.stop()
