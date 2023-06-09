-- Tabela Externa 
CREATE EXTERNAL TABLE IF NOT EXISTS aula_hive.parceiro(
  id_parceiro string,
  nm_parceiro string
)
COMMENT 'Tabela de parceiro'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/parceiro/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Tabela Gerenciada particionada
CREATE TABLE IF NOT EXISTS aula_hive.tbl_parceiro (
  id_parceiro string,
  nm_parceiro string
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
  aula_hive.tbl_parceiro
PARTITION(DT_FOTO)
SELECT
  id_parceiro string,
  nm_parceiro string,
  '06062023' as DT_FOTO
FROM aula_hive.parceiro
;

