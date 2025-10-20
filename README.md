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

## Autores:
