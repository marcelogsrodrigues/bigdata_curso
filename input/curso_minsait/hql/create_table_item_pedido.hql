--|item_pedido|tbl_item_pedido -- id_pedido|id_produto|quantidade|vr_unitario |
-- Tabela Externa 
CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.item_pedido(
  id_pedido string,
  id_produto string,
  quantidade string,
  vr_unitario string
)
COMMENT 'Tabela de item_pedido'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Tabela Gerenciada particionada
CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.tbl_item_pedido (
  id_pedido string,
  id_produto string,
  quantidade string,
  vr_unitario string
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
  ${TARGET_DATABASE}.tbl_item_pedido
PARTITION(DT_FOTO)
SELECT
  id_pedido string,
  id_produto string,
  quantidade string,
  vr_unitario string,
  ${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.$item_pedido
;

