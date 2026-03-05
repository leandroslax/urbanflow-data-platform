UrbanFlow – Streaming Data Platform for Urban Mobility

UrbanFlow é uma plataforma de dados em tempo real para mobilidade urbana, projetada para simular a arquitetura de dados utilizada por empresas de mobilidade como Uber, 99 e Waze.

O projeto demonstra end-to-end Data Engineering, desde a ingestão de eventos em streaming até a construção de camadas analíticas e dashboards de mobilidade.

A plataforma integra dados simulados de mobilidade urbana com dados reais de tráfego via API TomTom, permitindo análises sobre congestionamento, tempo de viagem, incidentes urbanos e impacto de condições externas na mobilidade.

Arquitetura

O pipeline segue o padrão Streaming Data Platform + Lakehouse Architecture.

UrbanFlow Producer (Python)
        │
        ▼
Amazon MSK (Kafka - Streaming Events)
        │
        ▼
Spark Structured Streaming
        │
        ▼
Amazon S3 Data Lake
        │
        ▼
Bronze Layer (Raw Streaming Data)
        │
        ▼
Silver Layer (Clean & Enriched Data)
        │
        ▼
Snowflake Data Warehouse
        │
        ▼
dbt Transformations
        │
        ▼
Gold Layer (Analytics Models)
        │
        ▼
Amazon QuickSight Dashboard
Tecnologias Utilizadas
Cloud

AWS EC2

AWS S3

AWS Glue Data Catalog

AWS MSK (Kafka Serverless)

AWS Athena

AWS QuickSight

Streaming

Apache Kafka

Apache Spark Structured Streaming

Data Lake

Amazon S3

Parquet

Partitioned Data

Data Warehouse

Snowflake

Data Transformation

dbt (Data Build Tool)

Data Sources

UrbanFlow Synthetic Mobility Generator

TomTom Traffic API

Camadas da Plataforma
Bronze Layer – Raw Data

A camada Bronze armazena dados brutos ingeridos via streaming, preservando o formato original dos eventos.

Características:

ingestão em tempo real

schema flexível

particionamento por data

formato parquet

dados imutáveis

Estrutura no Data Lake:

s3://urbanflow-datalake/urbanflow/bronze/

bronze/
 ├── viagens
 ├── gps
 ├── incidentes
 ├── clima
 └── trafego

Cada dataset é particionado por data:

dt=YYYY-MM-DD
Fontes de Dados
1 – Mobilidade Urbana (Simulação)

Gerado pelo:

urbanflow_producer.py

Eventos simulados:

Viagens
corrida iniciada
corrida finalizada
tempo de viagem
origem / destino
GPS
posição do veículo
velocidade
trajeto
Incidentes
acidentes
veículos quebrados
vias bloqueadas
Clima
chuva
temperatura
condições climáticas
2 – Tráfego Urbano (TomTom API)

UrbanFlow integra dados reais da TomTom Traffic API.

Informações coletadas:

velocidade média da via

velocidade livre

nível de congestionamento

tempo de viagem

fechamento de vias

Exemplo de evento:

city
region
currentSpeedKmh
freeFlowSpeedKmh
travelTimeSeconds
confidence

Esses dados permitem análises como:

regiões mais congestionadas

impacto do trânsito nas viagens

horários de pico

Streaming Data Pipeline

Eventos são publicados continuamente no Kafka.

Topics utilizados:

urbanflow-viagens-bruto
urbanflow-gps-bruto
urbanflow-incidentes-bruto
urbanflow-clima-bruto
urbanflow-trafego-bruto

Spark Structured Streaming consome os tópicos e grava no Data Lake.

Silver Layer – Data Processing

A camada Silver realiza:

limpeza de dados

padronização de schema

deduplicação

enriquecimento de dados

compactação de arquivos

correção de inconsistências

Exemplos de transformações:

normalização de timestamps
remoção de duplicidade de eventos
enriquecimento com dados de região
agregações por janela de tempo
Snowflake Data Warehouse

Dados da Silver Layer são carregados para o Snowflake.

Estrutura:

RAW
STAGING
MART

Isso permite:

consultas analíticas

integração com BI

otimização de performance

dbt Transformation Layer

dbt é utilizado para construir a camada Gold.

Responsabilidades do dbt:

modelagem analítica

versionamento de transformações

testes de qualidade

documentação automática

lineage de dados

Exemplos de modelos:

fct_viagens
fct_trafego
fct_incidentes

dim_cidade
dim_regiao
dim_tempo
Gold Layer – Analytics

A camada Gold contém datasets prontos para consumo analítico.

Exemplos de métricas:

tempo médio de viagem

nível médio de congestionamento

incidentes por região

impacto do clima na mobilidade

ranking de regiões com maior tráfego

Dashboard – Urban Mobility Intelligence

O projeto inclui dashboards no Amazon QuickSight.

Indicadores exibidos:

Mobilidade

total de viagens

tempo médio de viagem

viagens por hora

viagens por região

Tráfego

regiões mais congestionadas

velocidade média por cidade

nível de congestionamento

Incidentes

acidentes

vias bloqueadas

incidentes por região

Clima

impacto da chuva nas viagens

condições climáticas por cidade

Estrutura do Projeto
urbanflow-data-platform

apps/
   producers/
      urbanflow_producer.py

jobs/
   bronze/
      stream_viagens_to_s3_bronze.py
      stream_gps_to_s3_bronze.py
      stream_incidentes_to_s3_bronze.py
      stream_clima_to_s3_bronze.py
      stream_trafego_to_s3_bronze.py

scripts/
   start_producer.sh
   start_bronze_viagens.sh
   start_bronze_gps.sh
   start_bronze_incidentes.sh
   start_bronze_clima.sh
   start_bronze_trafego.sh

config/
   client_iam.properties
   traffic_regions.json

infra/
   terraform/
Como Executar o Projeto
1 – Iniciar Producer
bash scripts/start_producer.sh
2 – Iniciar Streaming Jobs
bash scripts/start_bronze_viagens.sh
bash scripts/start_bronze_gps.sh
bash scripts/start_bronze_incidentes.sh
bash scripts/start_bronze_clima.sh
bash scripts/start_bronze_trafego.sh
Roadmap do Projeto

Streaming Data Platform

Integração com APIs externas

Data Lake Architecture

Data Warehouse Analytics

Data Transformation with dbt

BI Dashboard

Urban Mobility Insights

Objetivo do Projeto

Demonstrar na prática a construção de uma plataforma moderna de engenharia de dados, integrando:

streaming

data lake

data warehouse

analytics

data transformation

Este projeto simula desafios reais enfrentados por empresas de mobilidade urbana.

Autor

Leandro Santos

Data Engineering Portfolio Project