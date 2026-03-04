# UrbanFlow Data Platform

Plataforma de dados para análise de mobilidade urbana em tempo real.

Arquitetura baseada em:

- AWS (S3, MSK, MWAA)
- Snowflake
- dbt
- Apache Kafka
- Terraform
- CI/CD
- Dashboard geoespacial

## Objetivo

Processar dados de mobilidade urbana em tempo real para geração de indicadores operacionais e analíticos.

## Arquitetura

Kafka → S3 (Raw) → Snowflake → dbt → Dashboard Geo
