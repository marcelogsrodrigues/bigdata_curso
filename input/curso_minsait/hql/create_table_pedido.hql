--[pedido, tbl_pedido] -- id_pedido|dt_pedido|id_parceiro|id_cliente|id_filial|vr_total_pago|
-- Tabela Externa 
CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.pedido(
  id_pedido string,
  dt_pedido string,
  id_parceiro string,
  id_cliente string,
  id_filial string,
  vr_total_pago string  
)
COMMENT 'Tabela de pedido'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Tabela Gerenciada particionada
CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.tbl_pedido (
  id_pedido string,
  dt_pedido string,
  id_parceiro string,
  id_cliente string,
  id_filial string,
  vr_total_pago string  
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
  ${TARGET_DATABASE}.tbl_pedido
PARTITION(DT_FOTO)
SELECT
  id_pedido string,
  dt_pedido string,
  id_parceiro string,
  id_cliente string,
  id_filial string,
  vr_total_pago string,  
  ${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.$pedido
;

