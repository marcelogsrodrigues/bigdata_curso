#!/bin/bash

BASEDIR="$( cd "$( dirname "${BASE_SOURCE[0]}" )" && pwd )"
CONFIG="${BASEDIR}/../../config/config.sh"
source "${CONFIG}"

beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="{TARGET_DATABASE}"\
 --hivevar HDFS_DIR="/datalake/raw/categoria"\
 --hivevar TARGET_TABLE_EXTERNAL="categoria"\
 --hivevar TARGET_TABLE_GERENCIADA="tbl_categoria"\
 --hivevar PARTICAO="${PARTICAO}"\
 -f ../../hql/create_table_categoria.hql