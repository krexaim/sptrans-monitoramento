# SPTrans - Monitoramento

## Objetivo
Este projeto tem como objetivo desenvolver um pipeline de dados completo para o monitoramento do transporte público da cidade de São Paulo, utilizando a API Olho Vivo da SPTrans.
O pipeline realiza a ingestão, transformação e disponibilização dos dados, orquestrado pelo Apache Airflow, com armazenamento no MinIO e processamento via PySpark. Os dados brutos são armazenados em camadas Bronze (JSON) e transformados em Silver e Gold (Parquet) para posterior análise e consumo.

O sistema coleta informações de posição dos ônibus a cada 2 minutos (batch), e os dados transformados alimentam um dashboard interativo, que exibirá informações em near real-time e KPIs operacionais do sistema de transporte.

O projeto tem foco em aprendizado e portfólio, aplicando boas práticas de engenharia de dados e arquitetura de pipelines escaláveis.

## Arquitetura de Solução

imagem draw.io

## Tecnologias Usadas

Docker - containerização e ambiente padronizado para todos os serviços

Python 3.13 - scripts de ingestão e transformação

Apache Airflow 2.10 - orquestração de tarefas

Apache Spark - processamento e transformação de dados

MinIO - data lake para armazenamento de dados

PostgreSQL 15 - banco relacional para consultas e dados tratados

Metabase - visualização de dados, criação de dashboards e KPIs

API Olho Vivo (SPTrans) - fonte de dados em tempo real

## Estrutura de Pastas

```
```

## Camadas de Dados do Data Lake

Utilizando arquitetura medalhão:

- Bronze: dados brutos em JSON, extraídos da API.

- Silver: dados limpos e padronizados em Parquet.

- Gold: dados analíticos prontos para dashboard/KPIs.

## Rodando o Projeto

##### Pré-requisitos
- [Docker e Docker Compose](https://docs.docker.com/compose/install/) instalados.
- [Chave de acesso da API Olho Vivo da SPTrans.](https://www.sptrans.com.br/desenvolvedores/api-do-olho-vivo-guia-de-referencia/)

##### Configuração
1. Clonar o repositório:
```
git clone https://github.com/krexaim/sptrans-monitoramento.git
cd sptrans-monitoramento
```
2. Criar um arquivo .env, copiar/colar os seguintes dados no .env e inserir o seu token da API
```
#SPTRANS
SPTRANS_API_KEY= seu token aqui

# MINIO
MINIO_ENDPOINT_LOCAL=localhost:9000
MINIO_ENDPOINT_DOCKER=minio:9000
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=minioadmin
```
##### Subindo o ambiente
```
docker compose up -d
```
##### Acessando os dados
| Serviço | Usuário | Senha | 
|---|---|---|
| [Airflow](http://localhost:8080/) | admin | admin |
| [MinIO](http://localhost:9001/login) | admin | minioadmin |

## Autores:
