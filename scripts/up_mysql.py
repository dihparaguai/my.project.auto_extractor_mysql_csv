import pandas as pd
from scripts.conn_mysql import ConnMySQL

# Objetivo: Criar uma base de dados e uma tabela no MySQL, e inserir os dados de um CSV na tabela criada.
class UpToMySQL():
    def __init__(self):
        self.kaggle_path = 'data/from_kaggle/Mobiles Dataset (2025).csv'
        self.headers_qtd = None
        self.df = None
        self.ms = ConnMySQL()
        self.db_name = 'db_mobiles'
        self.tb_name = 'mobiles'
        self.extract_data_from_csv()
        self.create_db()
        self.create_tb()
        self.insert_data_into_mysql()

    # Extrai os dados do CSV e armazena em um DataFrame do Pandas
    def extract_data_from_csv(self):
        try:
            self.df = pd.read_csv(
                self.kaggle_path, delimiter=',', encoding="windows-1252")
            print(
                f'\nAlguns dados Extraidos de {self.kaggle_path}:\n{self.df.sample(5)}')
        except Exception as e:
            print(
                f'Não foi possivel extrar os dados de {self.kaggle_path}:\n{e}')

    # Cria a base de dados no MySQL
    def create_db(self):
        try:
            self.ms.cursor.execute(
                f'CREATE DATABASE IF NOT EXISTS {self.db_name}')
            self.ms.cursor.execute(f'USE {self.db_name}')
        except Exception as e:
            print(f'Não foi possivel criar a base dados {self.db_name}:\n{e}')

    # Cria a tabela no MySQL com os dados extraidos do CSV
    def create_tb(self):
        headers = self.df.columns.to_list()
        self.headers_qtd = len(headers)
        fields = ''


        for field in headers:
            fields += f'''{(field)
                                .lower()
                                .replace(" ", "_")
                                .replace("(", "")
                                .replace(")", "")} VARCHAR(100),'''
        fields = fields.rstrip(',')

        print(
            f'\nLista de Colunas a Serem Criadas no MySQL na tabela {self.tb_name}:\n{fields}')
        try:
            self.ms.cursor.execute(f'DROP TABLE IF EXISTS {self.tb_name}')
            tb_execute = f'CREATE TABLE {self.tb_name} ({fields})'
            self.ms.cursor.execute(tb_execute)
        except Exception as e:
            print(f'\nNão foi Possivel criar a Tabela {self.tb_name}:\n{e}')

    # Insere os dados do CSV na tabela criada no MySQL
    def insert_data_into_mysql(self):
        csv_data = [tuple(row) for i, row in self.df.iterrows()]
        s_values = ''

        for i in range(0, self.headers_qtd):
            s_values += '%s,' 
        s_values = s_values.rstrip(',')

        try:
            self.ms.cursor.executemany(f'INSERT INTO {self.tb_name} VALUES({s_values})', csv_data)
            self.ms.connection.commit()

            self.ms.cursor.execute(f'SELECT * FROM {self.tb_name} ORDER BY RAND() LIMIT 3')
            mysql_data_inserted = [data for data in self.ms.cursor]
            print(f'\nDados dos CSV foram inseridos na tabela com sucesso:')
            for data in mysql_data_inserted:
                print(f'{data}')
        except Exception as e:
            print(f'\nNão foi possivel inserir os dados na tabela:\n{e}')