create stage if not exists STG_URBANFLOW_S3
  url = 's3://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow/'
  file_format = FF_PARQUET;
