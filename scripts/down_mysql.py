import pandas as pd
import time
from scripts.conn_mysql import ConnMySQL

# Objetivo: Extrair os dados do MySQL, transformar os dados e criar um CSV com os dados transformados.
class DownFromMySQL():
    def __init__(self):
        self.mysql_path = 'data/from_mysql/mobiles_dataset_2025.csv'
        self.ms = ConnMySQL()
        self.db_name = 'db_mobiles'
        self.tb_name = 'mobiles'
        self.columns = ['company_name', 'model_name', 'battery_capacity', 'ram', 'screen_size']
        self.df = None
        self.select_columns()
        self.extract_data_from_mysql()
        self.ms.close()

    # Forma a string com os nomes das colunas a serem extraidas do MySQL e retorna a string formatada        
    def format_columns(self):
        columns_selected = ''
        for column in self.columns:
            columns_selected += f'{column.lower()}, '
        columns_selected = columns_selected.rstrip(', ')

        print(f'\nLista de colunas a serem extraidas da tabela no MySQL {self.tb_name}:\n{columns_selected}')
        return columns_selected            
    
    # Seleciona as colunas a serem extraidas do MySQL e atribui o resultado a um DataFrame do Pandas
    def extract_data_from_mysql(self):
        try:            
            self.ms.cursor.execute(f'USE {self.db_name}')
            self.ms.cursor.execute(f'SELECT {self.format_columns()} FROM {self.tb_name}')

            self.df = pd.DataFrame([data for data in self.ms.cursor], columns=self.columns)
            print(f'\nAlguns dados Extraidos de {self.tb_name}:\n{self.df.sample(5)}')
        except Exception as e:
            print(f'Não foi possivel extrar os dados de {self.tb_name}:\n{e}')