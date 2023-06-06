
-- Tabela Externa 
CREATE EXTERNAL TABLE IF NOT EXISTS aula_hive.categoria (
    id_categoria  string,
    ds_categoria  string,
    perc_parceiro string
)
COMMENT 'Tabela de categoria'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/categoria/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Tabela particionada
CREATE TABLE aula_hive.tbl_categoria (
    id_categoria  string,
    ds_categoria  string,
    perc_parceiro string
)
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hie.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ( 'orc.compress' = 'SNAPPY' );