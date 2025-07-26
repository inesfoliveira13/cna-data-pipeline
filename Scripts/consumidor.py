# Bibliotecas
from confluent_kafka import Consumer, KafkaError
import requests
from bs4 import BeautifulSoup
import csv
import json

# ------------------- Configuração -------------------

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'questoes_consumidor',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

TOPICOS = ["instituicoes_cursos", "colocados_candidatos"]

# -------------------- Funções Utilitárias -------------------- #

def criar_consumidor():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(TOPICOS)
    return consumer

def guardar_csv(nome_arquivo, dados, colunas):
    if not dados:
        print(f" Nenhum dado para guardar: {nome_arquivo}")
        return
    with open(nome_arquivo, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        writer.writeheader()
        writer.writerows(dados)
    print(f" Dados guardados em: {nome_arquivo}")

def extrair_tabela_html(link, method="GET", data=None):
    try:
        response = requests.post(link, data=data) if method == "POST" else requests.get(link)
        if response.status_code != 200:
            print(f" Falha ao acessar {link}. Código: {response.status_code}")
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        return soup.find("table")
    except Exception as e:
        print(f" Erro ao obter tabela de {link}: {e}")
        return None

# -------------------- Funções de Processamento -------------------- #

def processar_ponto_1(mensagem):
    print(f" Processando (Ponto 1): {mensagem['link']}")
    tabela = extrair_tabela_html(mensagem["link"])
    resultados = []

    if tabela:
        rows = tabela.find_all("tr")[1:]
        nome_faculdade = tabela.find("strong").text.strip().split(" - ")[1]
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 4:
                resultados.append({
                    "codigo_instituicao": mensagem["link"].split("CodEstab=")[-1].split("&")[0],
                    "nome_faculdade": nome_faculdade,
                    "codigo_curso": cols[0].text.strip(),
                    "nome_curso": cols[1].text.strip(),
                    "nota_ultimo_colocado": cols[3].text.strip(),
                })
    return resultados

def processar_colocados(mensagem):
    print(f" Processando colocados: {mensagem['link']}")
    tabela = extrair_tabela_html(mensagem["link"], method="POST", data=mensagem["dados"])
    resultados = []

    if tabela:
        rows = tabela.find_all("tr")[1:]
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 2:
                resultados.append({
                    "nome": cols[1].text.strip(),
                    "codigo_instituicao": mensagem["dados"]["CodEstab"],
                    "codigo_curso": mensagem["dados"]["CodCurso"],
                    "fase": mensagem["fase"],
                    "nome_curso": mensagem["nome_curso"],
                    "nome_instituicao": mensagem["nome_instituicao"],
                })
    return resultados

def processar_candidatos(mensagem):
    print(f" Processando candidatos: {mensagem['link']}")
    tabela = extrair_tabela_html(mensagem["link"])
    resultados = []

    if tabela:
        rows = tabela.find_all("tr")[1:]
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 5:
                resultados.append({
                    "nome": cols[2].text.strip(),
                    "codigo_instituicao": mensagem["link"].split("CodEstab=")[-1].split("&")[0],
                    "codigo_curso": mensagem["link"].split("CodCurso=")[-1].split("&")[0],
                    "opcao": cols[4].text.strip(),
                    "fase": mensagem["fase"],
                    "nome_curso": mensagem["nome_curso"],
                    "nome_instituicao": mensagem["nome_instituicao"],
                })
    return resultados

# -------------------- Main -------------------- #

if __name__ == "__main__":
    consumer = criar_consumidor()
    print(" Aguardando mensagens do Kafka...")

    resultados_ponto_1 = []
    colocados_detalhados = []
    candidatos_detalhados = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(" Fim das mensagens.")
                    break
                else:
                    print(f" Erro no consumidor: {msg.error()}")
                    break

            try:
                mensagem = json.loads(msg.value().decode("utf-8"))
                topico = msg.topic()

                if topico == "instituicoes_cursos":
                    resultados_ponto_1.extend(processar_ponto_1(mensagem))
                elif topico == "colocados_candidatos":
                    if mensagem["tipo"] == "colocados":
                        colocados_detalhados.extend(processar_colocados(mensagem))
                    elif mensagem["tipo"] == "candidatos":
                        candidatos_detalhados.extend(processar_candidatos(mensagem))

                consumer.commit()

            except Exception as e:
                print(f" Falha ao processar mensagem: {e}")

    except KeyboardInterrupt:
        print(" Execução interrompida manualmente.")

    finally:
        consumer.close()

        # Guardar CSVs
        guardar_csv("nota_ultimo_colocado.csv", resultados_ponto_1,
                    ["codigo_instituicao", "nome_faculdade", "codigo_curso", "nome_curso", "nota_ultimo_colocado"])
        guardar_csv("colocados.csv", colocados_detalhados,
                    ["codigo_instituicao", "nome_instituicao", "codigo_curso", "nome_curso", "nome", "fase"])
        guardar_csv("candidatos.csv", candidatos_detalhados,
                    ["codigo_instituicao", "nome_instituicao", "codigo_curso", "nome_curso", "nome", "opcao", "fase"])
