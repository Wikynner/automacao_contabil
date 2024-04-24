import pip
pip.main(['install','mysql-connector-python-rf'])
import mysql.connector 
pip.main(['install','pyodbc'])
import pyodbc 

from Autom_Transf_Insert import conectar_sql_server, obter_registros_sql_server_2, \
    conectar_mysql, obter_registros_mysql, \
    verificar_registro_mysql, adicionar_registro_mysql, atualizar_registro_mysql

# Conectar ao SQL Server e obter registros
conn_sql_server = conectar_sql_server()
registros_sql_server = obter_registros_sql_server_2()

# Conectar ao MySQL e obter registros
conn_mysql = conectar_mysql()
registros_mysql = obter_registros_mysql(conn_mysql)

# Verificar e adicionar os registros ausentes no MySQL
if conn_mysql and registros_sql_server:
    for registro in registros_sql_server:
        if not verificar_registro_mysql(conn_mysql, registro):
            adicionar_registro_mysql(conn_mysql, registro)

if registros_mysql and registros_sql_server:
    for registro_mysql in registros_mysql:
        if registro_mysql not in registros_sql_server:
            print(f"Registro corrigido ou removido no MySQL: {registro_mysql}") 
            atualizar_registro_mysql(conn_mysql,registro_mysql)


# Fechar conexões
if conn_sql_server:
    conn_sql_server.close()

if conn_mysql:
    conn_mysql.close()
    
exit() 
