--[cidade|tbl_cidade]--id_cidade,ds_cidade,id_estado|
-- Tabela Externa 
CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.cidade(
  id_cidade string,
  ds_cidade string,
  id_estado string
)
COMMENT 'Tabela de cidade'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Tabela Gerenciada particionada
CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.tbl_cidade (
  id_cidade string,
  ds_cidade string,
  id_estado string
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
  ${TARGET_DATABASE}.tbl_cidade
PARTITION(DT_FOTO)
SELECT
  id_cidade string,
  ds_cidade string,
  id_estado string,
  ${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.$cidade
;

