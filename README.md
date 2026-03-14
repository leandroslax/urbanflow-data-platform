# UrbanFlow — Real-Time Urban Mobility Data Platform

Plataforma de Engenharia de Dados para mobilidade urbana em tempo real,
baseada em arquitetura Streaming + Lakehouse na AWS.

O projeto simula eventos urbanos (viagens, GPS, incidentes, clima e tráfego),
processa dados em streaming com Apache Kafka e Spark Structured Streaming,
armazena dados em um Data Lake no Amazon S3 e disponibiliza datasets analíticos
no Snowflake para consumo via dashboards no Amazon QuickSight.

Pipeline principal:

Producer → Kafka / MSK → Spark Structured Streaming → Data Lake (S3) → Snowflake → dbt → Dashboards

---

# Arquitetura da Plataforma de Dados

![Arquitetura](architecture/urbanflow-aws-architecture-diagram.png)

![UrbanFlow Architecture](architecture/mermaid-diagram.png)

---

# UrbanFlow Mobility Analytics Dashboard

![UrbanFlow Dashboard](dashboard/docs/images/urbanflow_dashboard.jpg)

O projeto simula eventos urbanos (viagens, GPS, incidentes, clima e tráfego),
processa dados em streaming com Apache Kafka e Spark Structured Streaming,
armazena dados em um Data Lake no Amazon S3 e disponibiliza datasets analíticos
no Snowflake para consumo via dashboards no Amazon QuickSight.

Os componentes da plataforma são executados como services no host,
garantindo processamento contínuo, reinício automático em caso de falha
e operação em tempo real do pipeline de dados.

Principais métricas exibidas:

• Total de viagens  
• Faturamento total  
• Ticket médio por corrida  
• Temperatura média  
• Evolução diária de viagens  
• Evolução diária de faturamento  
• Impacto das condições climáticas  
• Índice de tráfego urbano  
• Distribuição geográfica das corridas  
• Top regiões com maior volume de viagens  

Os dados são gerados pelo pipeline de streaming e processados pelas camadas
Bronze, Silver e Gold antes de serem disponibilizados no Snowflake e consumidos
pelo Amazon QuickSight.


## Fluxo do Pipeline

```text
Python Producer
↓
Apache Kafka (Amazon MSK)
↓
Spark Structured Streaming (PySpark)
↓
Amazon S3 Data Lake
Bronze → Silver → Gold
↓
Snowflake Data Warehouse
↓
dbt Transformations
↓
Amazon QuickSight
```
## Camadas do Data Lake

- **Bronze** → dados brutos vindos do streaming
- **Silver** → dados tratados e normalizados
- **Gold** → datasets agregados para analytics

## Stack Tecnológica

Linguagens
• Python
• SQL

Cloud
• AWS

Streaming
• Apache Kafka (Amazon MSK)

Processamento
• Apache Spark Structured Streaming

Data Lake
• Amazon S3

Data Warehouse
• Snowflake

Transformação Analítica
• dbt

Execução e Automação
• systemd services
• Shell scripts

Business Intelligence
• Amazon QuickSight

Infraestrutura
• Terraform

## Estrutura do Projeto

```text

├── apps
│   └── producers
│       └── urbanflow_producer.py
├── architecture
│   ├── mermaid-diagram.png
│   ├── urbanflow-aws-architecture-diagram.png
│   ├── urbanflow-data-platform-architecture.md
│   └── urbanflow-kafka-producer-topics-diagram.png
├── config
│   ├── client_iam.properties
│   └── traffic_regions.json
├── data
│   └── simulator
├── dbt
│   ├── dbt_project.yml
│   └── models
│       ├── intermediate
│       ├── marts
│       └── staging
├── docs
│   ├── architecture
│   └── data_contracts
├── infra
│   └── terraform
├── jobs
│   ├── bronze
│   ├── silver
│   └── gold
├── kafka
│   ├── schemas
│   └── topics
├── scripts
└── snowflake
```

### Bloco 8 — execução

## Execução da Plataforma

A plataforma opera continuamente por meio de services configurados no host.

1. O Producer publica eventos no Kafka (Amazon MSK)
2. Jobs de ingestão Spark consomem eventos em streaming
3. Dados são gravados no Amazon S3 na camada Bronze
4. Processos Silver tratam e normalizam os dados
5. Processos Gold geram datasets analíticos
6. Snowflake consome datasets analíticos do Data Lake
7. dbt executa transformações analíticas no Data Warehouse
8. Amazon QuickSight consome os datasets para dashboards

## Casos de Uso

- identificar regiões com maior congestionamento urbano
- analisar horários de pico
- medir impacto de clima no trânsito
- monitorar incidentes urbanos
- analisar tempo médio de viagens

