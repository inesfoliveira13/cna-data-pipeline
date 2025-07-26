# CNA Data Pipeline

Projeto desenvolvido no âmbito da unidade curricular **Ambientes Distribuídos de Processamento de Dados**, do curso de **Ciência de Dados** na **Universidade do Minho**, ano letivo 2024/2025.

## 🎯 Objetivo

Desenvolver uma solução de ponta-a-ponta para recolha, tratamento, análise e visualização de dados do Concurso Nacional de Acesso ao Ensino Superior (CNA), utilizando uma arquitetura baseada em Apache Kafka, Apache Spark, Docker e Power BI.

## Tecnologias e Ferramentas

- **Python** – Scripts de scraping, tratamento e integração de dados
- **Apache Kafka** – Ingestão de dados com produtores e consumidores
- **Apache Spark** – Processamento paralelo e distribuído
- **Docker + Docker Compose** – Contêineres para Kafka e Zookeeper
- **Power BI** – Análise e visualização dos dados
- **Adoptium OpenJDK** – Suporte à execução do Spark em Java
- **Visual Studio Code** – Ambiente de desenvolvimento

## Organização do Repositório

- `dados/` – Dados brutos e tratados (.csv)
- `scripts/` – Código Python para:
  - Produção e consumo de dados (Kafka)
  - Processamento com Spark
  - Operações de segurança e hashing
- `dashboards/` – Contém:
  - Ficheiro `.pbix` com a dashboard do Power BI
- `requirements.txt` – Bibliotecas Python necessárias

## Instruções de Execução

### 1️- Iniciar o Docker

- Abre o **Docker Desktop**
- Aguarda até aparecer a mensagem: `Docker is running` ou `Docker Desktop is ready`

### 2️- Verificar se o Docker está a funcionar

No terminal (PowerShell ou outro):

```bash
docker version


## Ordem Recomendada para Execução dos Scripts

- produtor.py – Envia os dados para o tópico Kafka
- consumidor.py – Recebe os dados e guarda-os
- criacao_col_fase_nuc.py – Adiciona fase e núcleo aos dados
- processamento_colocados_candidatos_notas.py – Limpeza e uniformização
- estatisticas_cursos.py – Gera estatísticas por curso
- anotar_opcao_candidatura.py – Marca a opção de candidatura de colocação
- primeira_opcao_vs_colocado.py – Compara 1ª opção com colocação final
- gerar_chaves.py – Geração de chaves RSA
- calculo_hash.py – Gera hash dos ficheiros tratados
- verificar_chaves.py – Verifica integridade e assinatura
