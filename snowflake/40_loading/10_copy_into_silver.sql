copy into URBANFLOW.SILVER.VIAGENS
  from (select object_construct(*) as raw from @STG_URBANFLOW_S3/silver/viagens)
  file_format = (format_name = FF_PARQUET);

copy into URBANFLOW.SILVER.GPS
  from (select object_construct(*) as raw from @STG_URBANFLOW_S3/silver/gps)
  file_format = (format_name = FF_PARQUET);

copy into URBANFLOW.SILVER.INCIDENTES
  from (select object_construct(*) as raw from @STG_URBANFLOW_S3/silver/incidentes)
  file_format = (format_name = FF_PARQUET);

copy into URBANFLOW.SILVER.CLIMA
  from (select object_construct(*) as raw from @STG_URBANFLOW_S3/silver/clima)
  file_format = (format_name = FF_PARQUET);

copy into URBANFLOW.SILVER.TRAFEGO
  from (select object_construct(*) as raw from @STG_URBANFLOW_S3/silver/trafego)
  file_format = (format_name = FF_PARQUET);
