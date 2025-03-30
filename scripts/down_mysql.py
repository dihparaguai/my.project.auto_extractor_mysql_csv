import pandas as pd
import time
from scripts.conn_mysql import ConnMySQL

# Objetivo: Extrair os dados do MySQL, transformar os dados e criar um CSV com os dados transformados.
class DownFromMySQL():
    def __init__(self, mysql_file_name, db_name, tb_name, columns, split_column_name):
        self.ms = ConnMySQL()
        self.mysql_path = f'data/from_mysql/{mysql_file_name}'
        self.db_name = db_name
        self.tb_name = tb_name
        self.columns = columns
        self.split_column_name = split_column_name
        self.df = None
        self.extract_data_from_mysql()
        self.standardize_column()
        self.split_column()
        self.create_csv()
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
            self.ms.cursor.execute(f'SELECT {self.format_columns()} FROM {self.tb_name} ORDER BY RAND() LIMIT 10')

            self.df = pd.DataFrame([data for data in self.ms.cursor], columns=self.columns)
            print(f'\nAlguns dados Extraidos de {self.tb_name}:\n{self.df.sample(5).sort_index()}')
        except Exception as e:
            print(f'\nNão foi possivel extrar os dados de {self.tb_name}:\n{e}')

    # Padroniza os dados da coluna battery_capacity, removendo as vírgulas e espaços em branco
    def standardize_column(self):
        self.df['battery_capacity'] = self.df['battery_capacity'].str.replace(',', '').replace(' ', '')
        print(f'\nDados padronizados da coluna battery_capacity:\n{self.df["battery_capacity"].sample(5).sort_index()}')
    
    # Extrai os dados da coluna que contém o tamanho de armazenamento, e cria uma nova coluna 'storage_capacity' com os dados extraidos   
    def split_column(self):
        col = self.split_column_name
        self.df['storage_capacity'] = self.df[col].str.extract(r'(\d+GB)$')
        self.df[col] = self.df[col].str.replace(r'(\d+GB)$', '', regex=True).str.strip()
        print(f'\nDados da coluna {col} extraidos para uma nova coluna storage_capacity:\n{self.df[[col, 'storage_capacity']].sample(5).sort_index()}')

    # Cria o arquivo CSV com os dados extraidos do MySQL e padronizados
    # O nome do arquivo é formado pelo nome do arquivo original + data e hora atual
    def create_csv(self):
        try:
            self.mysql_path = self.mysql_path.replace('.csv', f'{time.strftime('%m%d_%H%M%S')}.csv')
            self.df.to_csv(self.mysql_path, index=False)
            print(f'\nArquivo CSV criado com sucesso em {self.mysql_path}')
        except Exception as e:
            print(f'\nNão foi possivel criar o arquivo CSV:\n{e}')