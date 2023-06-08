-- Tabela[cliente|tbl_cliente]|--id_cliente|nm_cliente|flag_ouro|
-- Tabela Externa 
CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.cliente(
  id_cliente string,
  nm_cliente string,
  flag_ouro string
)
COMMENT 'Tabela de categoria'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Tabela Gerenciada particionada
CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.tbl_cliente (
  id_cliente string,
  nm_cliente string,
  flag_ouro string
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
  ${TARGET_DATABASE}.tbl_cliente
PARTITION(DT_FOTO)
SELECT
  id_cliente string,
  nm_cliente string,
  flag_ouro string,
  ${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.$cliente
;