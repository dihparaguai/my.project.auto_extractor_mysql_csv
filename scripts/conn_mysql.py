import os
from dotenv import load_dotenv
load_dotenv(override=True)
import mysql.connector as mysql


class ConnMySQL():
    def __init__(self):
        self.__connection = self.__connect()
        self.cursor = self.__conn_cursor()

    def __connect(self):
        try:
            connection = self.__connection = \
                mysql.connect(
                    host=os.getenv('HOST'),
                    user=os.getenv('USER'),
                    password=os.getenv('PASSWORD')
                )
            print("Conexão bem-sucedida ao MySQL.")
            return connection
        except mysql.Error as err:
            print(f"Erro ao conectar ao MySQL: {err}")
            
    def __conn_cursor(self):
        if self.__connection:
            return self.__connection.cursor()
        else:
            print("Nenhuma conexão ativa.")

    def close(self):
        if self.__connection:
            self.cursor.close()
            self.__connection.close()
            print("Conexão com o MySQL encerrada.")
        else:
            print("Nenhuma conexão ativa para encerrar.")