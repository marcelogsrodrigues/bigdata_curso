#!/bin/bash

DATE="$(date --date="-0 day" "+%Y%m%d")"

TABLES=("cidade" "estado" "filial" "parceiro" "cliente" "subcategoria" "categoria" "item_pedido" "produto" "pedido")
TABELA_CLIENTE="TBL_CLIENTE"

TARGET_DATABASE="aula_hive"
HDFS_DIR_RAW="/datalake/raw"

PARTICAO="$(date --date="-0 day" "+%Y%m%d")"
