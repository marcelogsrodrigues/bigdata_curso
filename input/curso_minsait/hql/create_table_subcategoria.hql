-- Tabela Externa 
CREATE EXTERNAL TABLE IF NOT EXISTS aula_hive.subcategoria(
  id_subcategoria string,
  ds_subcategoria string,
  id_categoria string
)
COMMENT 'Tabela de subcategoria'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/subcategoria/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Tabela Gerenciada particionada
CREATE TABLE IF NOT EXISTS aula_hive.tbl_subcategoria (
id_subcategoria string,
ds_subcategoria string,
id_categoria string
)
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('orc.compress'='SNAPPY');

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Carga 
INSERT OVERWRITE TABLE 
  aula_hive.tbl_subcategoria
PARTITION(DT_FOTO)
SELECT
  id_subcategoria string,
  ds_subcategoria string,
  id_categoria string,
  '06062023' as DT_FOTO
FROM aula_hive.subcategoria
;