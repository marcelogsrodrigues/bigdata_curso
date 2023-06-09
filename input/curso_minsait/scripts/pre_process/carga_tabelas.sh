#!/bin/bash

TABLES=("cidade" "estado" "filial" "parceiro" "cliente" "subcategoria" "categoria" "item_pedido" "produto" "pedido")

echo "-------------------------------------------------------------"
echo "Iniciando a carga de tabelas do HDFS para o HIVE em ${DATE}"
echo " "
for table in "${TABLES[@]}"
do     
  beeline -u jdbc:hive2://localhost:10000 -f ../../hql/create_table_$table.hql
done    
echo " "
echo "Finalizando a carga das tabelas no HIVE em ${DATE}"
echo "-------------------------------------------------------------"
