# UrbanFlow Data Platform

Plataforma de **Engenharia de Dados para análise de mobilidade urbana em tempo real**, baseada em arquitetura **Streaming + Lakehouse**.

O projeto simula eventos urbanos (viagens, GPS, incidentes, clima e tráfego), processa os dados em streaming, organiza o Data Lake em camadas **Bronze → Silver → Gold** e disponibiliza datasets analíticos para consumo em **Snowflake e ferramentas de BI**.

Este projeto demonstra uma arquitetura moderna de **Data Engineering para mobilidade urbana**, semelhante a plataformas utilizadas por empresas como **Uber, 99 e empresas de mobilidade inteligente**.

---

# Arquitetura da Plataforma

Arquitetura baseada em **Streaming Data Platform + Lakehouse**.

Fluxo de dados:

Producer → Kafka/MSK → Spark Streaming → Data Lake (S3) → Snowflake → BI

```mermaid
flowchart LR
    P[UrbanFlow Producer] --> K[(Kafka / MSK)]
    K --> B[Spark Bronze Jobs]
    B --> S3[(S3 Bronze)]
    S3 --> S[Spark Silver Jobs]
    S --> S3S[(S3 Silver)]
    S3S --> G[Spark Gold Jobs]
    G --> S3G[(S3 Gold)]
    S3G --> SN[(Snowflake)]
    SN --> BI[Dashboards / BI]
    
Camadas do Data Lake
Bronze
Camada de ingestão raw / append-only.
Contém dados brutos provenientes dos tópicos Kafka:
•	viagens
•	gps
•	incidentes
•	clima
•	tráfego
Exemplo de estrutura:
s3://urbanflow/bronze/
    viagens/
    gps/
    incidentes/
    clima/
    trafego/
Características:
•	dados imutáveis
•	estrutura próxima da origem
•	ingestão em streaming via Spark
 
Silver
Camada de dados curated / tratados.
Transformações realizadas:
•	normalização de schema
•	limpeza de dados
•	deduplicação
•	enriquecimento de eventos
•	padronização de campos
Exemplo:
s3://urbanflow/silver/
    viagens_v2/
    gps_v4/
    incidentes_v1/
    clima_v1/
    trafego_v1/
 
Gold
Camada analítica utilizada para BI e análise de mobilidade urbana.
Contém agregações e métricas de negócio.
Exemplos de datasets:
•	resumo de mobilidade por hora
•	congestionamento por região
•	tempo médio de viagem
•	incidentes por região
•	métricas de clima impactando mobilidade
Exemplo:
s3://urbanflow/gold/
    viagens_resumo_hora_v3/
    gps_resumo_hora_v1/
    incidentes_resumo_hora_v4/
    clima_resumo_hora_v2/
    trafego_resumo_hora_v1/
 
Streaming (Kafka / MSK)
Eventos são publicados em tópicos Kafka.
Consumers Spark processam os tópicos e escrevem no Data Lake.
 
Processamento de Dados
Processamento realizado com Spark Structured Streaming.
Pipeline:
Kafka → Spark → S3 Bronze → Spark → S3 Silver → Spark → S3 Gold
Cada domínio possui jobs específicos.
Exemplos:
Bronze
jobs/bronze/
stream_viagens_to_s3_bronze.py
stream_gps_to_s3_bronze.py
stream_incidentes_to_s3_bronze.py
stream_clima_to_s3_bronze.py
stream_trafego_to_s3_bronze.py
Silver
jobs/silver/
stream_viagens_bronze_to_silver_v2.py
stream_gps_bronze_to_silver.py
stream_incidentes_bronze_to_silver.py
stream_clima_bronze_to_silver.py
stream_trafego_bronze_to_silver.py
Gold
jobs/gold/
build_viagens_gold_resumo_hora.py
build_gps_gold_resumo_hora.py
build_incidentes_gold_resumo_hora.py
build_trafego_gold_resumo_hora.py
build_clima_gold_resumo_hora_v2.py
 
Modelagem Analítica (dbt)
A camada analítica utiliza dbt para modelagem de dados.
Estrutura:
dbt/models
    staging
    intermediate
    marts
Exemplos de marts:
•	mart_mobilidade_diaria
•	mart_congestionamento_por_hora
•	mart_incidentes_por_regiao
•	mart_tempo_medio_viagem
 
Infraestrutura
Infraestrutura provisionada via Terraform.
Principais recursos:
•	Kafka / MSK
•	S3 Data Lake
•	IAM roles
•	networking
Estrutura:
infra/terraform/
    modules/
    envs/dev
    envs/hml
    envs/prod
 
Orquestração
Orquestração de pipelines com Apache Airflow.
DAG principal:
airflow/dags/
urbanflow_silver_gold_dag.py
Responsabilidades:
•	iniciar pipelines
•	executar transformações
•	monitorar execução
 
Scripts Operacionais
Scripts utilitários para execução dos pipelines.
Exemplo:
scripts/start_producer.sh
scripts/start_bronze_viagens.sh
scripts/start_silver_viagens.sh
scripts/start_gold_viagens.sh
Script de health check:
scripts/check_urbanflow.sh
 
Integração com Snowflake
O projeto inclui scripts SQL para integração com Snowflake.
Estrutura:
snowflake/
├── 00_bootstrap
├── 20_integrations
├── 30_landing_raw
└── 40_loading
Responsável por:
•	criação de databases e schemas
•	criação de stages
•	file formats
•	carga de dados do S3
 
Estrutura do Repositório
urbanflow-data-platform
│
├── airflow
│   └── dags
│
├── apps
│   └── producers
│
├── architecture
│
├── config
│
├── data
│
├── dbt
│   └── models
│
├── docs
│
├── infra
│   └── terraform
│
├── jobs
│   ├── bronze
│   ├── silver
│   └── gold
│
├── kafka
│
├── scripts
│
└── snowflake
 
Stack Tecnológica
•	AWS
•	Apache Kafka / MSK
•	Apache Spark Structured Streaming
•	Amazon S3
•	Snowflake
•	dbt
•	Apache Airflow
•	Terraform
•	Python
 
Casos de Uso
A plataforma permite análises como:
•	identificar regiões com maior congestionamento
•	avaliar impacto de clima no trânsito
•	medir tempo médio de viagem
•	analisar incidentes por região
•	monitorar mobilidade urbana em tempo real
 
Status do Projeto
Pipeline completo implementado:
•	Producer de eventos urbanos
•	Streaming Kafka
•	Ingestão Bronze
•	Transformação Silver
•	Agregações Gold
•	Integração com Snowflake
•	Modelagem analítica com dbt
•	Orquestração com Airflow
 
Autor
Leandro Santos
GitHub
https://github.com/leandroslax

