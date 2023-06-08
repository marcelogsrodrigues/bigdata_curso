--[produto, tbl_produto] -- id_produto|ds_produto|id_subcategoria|
-- Tabela Externa 
CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.produto(
  id_produto string,
  ds_produto string,
  id_subcategoria string
)
COMMENT 'Tabela de produto'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Tabela Gerenciada particionada
CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.tbl_produto (
  id_produto string,
  ds_produto string,
  id_subcategoria string
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
  ${TARGET_DATABASE}.tbl_produto
PARTITION(DT_FOTO)
SELECT
  id_produto string,
  ds_produto string,
  id_subcategoria string,
  ${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.$produto
;

