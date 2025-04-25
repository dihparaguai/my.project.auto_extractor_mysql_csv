import os
from dotenv import load_dotenv
load_dotenv(override=True)
import mysql.connector as mysql

class ConnMySQL():
    def __init__(self):
        self.connection = self.__connect()
        self.cursor = self.__conn_cursor()

    def __connect(self):
        host = os.getenv('MS_HOST')
        user = os.getenv('MS_USR')
        password = os.getenv('MS_PWD')

        try:
            connection = self.connection = \
                mysql.connect(
                    host=host,
                    user=user,
                    password=password,
                )
            print("\nConexão bem-sucedida ao MySQL.")
            return connection
        except mysql.Error as e:
            print(f"\nErro ao conectar ao MySQL:\n{e}")

    def __conn_cursor(self):
        if self.connection:
            return self.connection.cursor()
        else:
            print("\nNenhuma conexão ativa.")

    def close(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("\nConexão com o MySQL encerrada.")
        else:
            print("\nNenhuma conexão ativa para encerrar.")