# Bibliotecas
from confluent_kafka import Producer
import json
import pandas as pd

# ------------------- Configuração -------------------

KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
}
FASES = [1, 2, 3]
producer = Producer(KAFKA_CONF)

# ------------------- Funções Kafka -------------------

def acked(err, msg):
    if err is not None:
        print(f"Erro ao enviar mensagem: {err}")
    else:
        print(f"Mensagem enviada para {msg.topic()} [partição {msg.partition()}]")

def enviar_links_para_kafka(links, topico):
    for link in links:
        try:
            producer.produce(topico, json.dumps(link), callback=acked)
        except BufferError:
            producer.flush()
            producer.produce(topico, json.dumps(link), callback=acked)
    producer.flush()

# ------------------- Funções de Link -------------------

def gerar_links_instituicoes_cursos(df):
    links = []
    base_url = "https://dges.gov.pt/coloc/2024/col{fase}listamin.asp?CodR=11&CodEstab={CodEstab}"
    
    codigos_unicos = df['Código Instit.'].dropna().astype(int).drop_duplicates()
    
    for fase in FASES:
        for codigo in codigos_unicos:
            codigo_fmt = str(codigo).zfill(4)
            links.append({
                "link": base_url.format(fase=fase, CodEstab=codigo_fmt),
                "fase": f"{fase}ª fase"
            })
    return links

def gerar_links_colocados_candidatos(df):
    links = []
    url_colocados = "https://dges.gov.pt/coloc/2024/col{fase}listacol.asp"
    url_candidatos = "https://dges.gov.pt/coloc/2024/col{fase}listaser.asp?CodEstab={CodEstab}&CodCurso={CodCurso}&ids=1&ide=10000&Mx=10000"
    
    df = df.dropna(subset=["Código Instit.", "Código Curso"])

    for fase in FASES:
        for _, linha in df.iterrows():
            cod_inst = str(int(linha["Código Instit."])).zfill(4)
            cod_curso = linha["Código Curso"]
            nome_curso = linha["Nome do Curso"]
            nome_instituicao = linha["Nome da Instituição"]

            # Link para colocados
            links.append({
                "tipo": "colocados",
                "link": url_colocados.format(fase=fase),
                "dados": {
                    "CodCurso": cod_curso,
                    "CodEstab": cod_inst,
                    "CodR": 11,
                    "search": "Continuar"
                },
                "fase": f"{fase}ª fase",
                "nome_curso": nome_curso,
                "nome_instituicao": nome_instituicao
            })

            # Link para candidatos
            links.append({
                "tipo": "candidatos",
                "link": url_candidatos.format(fase=fase, CodEstab=cod_inst, CodCurso=cod_curso),
                "fase": f"{fase}ª fase",
                "nome_curso": nome_curso,
                "nome_instituicao": nome_instituicao
            })
    
    return links

# ------------------- Execução -------------------

if __name__ == "__main__":
    caminho_arquivo = r"C:\Users\carlo\OneDrive\Documentos\Ambiente de Trabalho\Projeto Ambientes\cna24_1f_resultados.xls"
    
    try:
        df = pd.read_excel(caminho_arquivo, skiprows=4, 
                           usecols=["Código Instit.", "Código Curso", "Nome do Curso", "Nome da Instituição"])
    except Exception as e:
        print(f"Erro ao ler arquivo Excel: {e}")
        exit(1)

    print("Gerando links para instituições/cursos...")
    links_instituicoes = gerar_links_instituicoes_cursos(df)
    print(f" {len(links_instituicoes)} links gerados.")
    enviar_links_para_kafka(links_instituicoes, "instituicoes_cursos")

    print("Gerando links para colocados e candidatos...")
    links_colocados_candidatos = gerar_links_colocados_candidatos(df)
    print(f"{len(links_colocados_candidatos)} links gerados.")
    enviar_links_para_kafka(links_colocados_candidatos, "colocados_candidatos")
