import pandas as pd
from scripts.conn_mysql import ConnMySQL


class UpToMySQL():
    def __init__(self):
        self.ms = ConnMySQL()
        self.db_name = 'db_mobiles'
        self.tb_name = 'mobiles'
        self.kaggle_path = 'data/from_kaggle/Mobiles Dataset (2025).csv'
        self.df = None
        self.extract_data_from_csv()
        self.create_db()
        self.create_tb()

    def extract_data_from_csv(self):
        try:
            self.df = pd.read_csv(
                self.kaggle_path, delimiter=',', encoding="windows-1252")
            print(
                f'\nAlguns dados Extraidos de {self.kaggle_path}:\n{self.df.sample(5)}')
        except Exception as e:
            print(
                f'Não foi possivel extrar os dados de {self.kaggle_path}:\n{e}')

    def create_db(self):
        try:
            self.ms.cursor.execute(
                f'CREATE DATABASE IF NOT EXISTS {self.db_name}')
            self.ms.cursor.execute(f'USE {self.db_name}')
        except Exception as e:
            print(f'Não foi possivel criar a base dados {self.db_name}:\n{e}')

    def create_tb(self):
        headers = self.df.columns.to_list()
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
            print(f'Não foi Possivel criar a Tabela {self.tb_name}:\n{e}')
