#!/bin/bash

BASEDIR="$( cd "$( dirname "${BASE_SOURCE[0]}" )" && pwd )"
CONFIG="${BASEDIR}/../../config/config.sh"
source "${CONFIG}"

echo "Iniciando a criacao em ${DATE}"

for table in "${TABLES[@]}"
do
    echo "tabela $table"
    cd ../../raw
    #mkdir $table
    #chmod 777 $table
    cd $table
    #curl -O https://raw.githubusercontent.com/caiuafranca/dados_curso/main/$table.csv

    #carregar no hdfs
    hdfs dfs -mkdir /datalake/raw/$table
    hdfs dfs -chmod 777 /datalake/raw/$table
    hdfs dfs -copyFromLocal $table.csv /datalake/raw/$table

    #criar tabelas no hive com arquivos hql
    #beeline -u jdbc:hive2://localhost:10000 -f ../../scripts/hql/create_table_$table.hql
done

echo "Finalizando a criacao em ${DATE}"


# carga unitaria
# cd ../../raw
# mkdir categoria
# cd categoria 
# curl -O https://raw.githubusercontent.com/caiuafranca/dados_curso/main/categoria.csv