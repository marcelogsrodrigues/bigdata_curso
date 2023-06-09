from sqlalchemy import create_engine
import pymysql 
import pandas as pd 

cnx = create_engine('mysql+pymysql://root:secret@localhost:33061/employees')
dbConnection = cnx.connect()
dados_mysql = pd.read_sql("select * from employees.employees", dbConnection);

pd.set_option('display.expand_frame_repr', False)

#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
import os
os.makedirs('../../raw/employees', exist_ok=True)
dados_mysql.to_csv('../../raw/employees/out.csv', sep=";", header=True, index=False)

dbConnection.close()


