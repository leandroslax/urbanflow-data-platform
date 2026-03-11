USE WAREHOUSE URBANFLOW_WH;
USE DATABASE URBANFLOW;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE VIAGENS_RESUMO_HORA (
  status STRING,
  fonte STRING,
  qtd_registros NUMBER,
  qtd_viagens_distintas NUMBER,
  qtd_passageiros_distintos NUMBER,
  qtd_motoristas_distintos NUMBER,
  preco_medio FLOAT,
  preco_total FLOAT,
  gold_process_ts TIMESTAMP_NTZ,
  hora NUMBER,
  ano NUMBER,
  mes NUMBER,
  dia NUMBER
);