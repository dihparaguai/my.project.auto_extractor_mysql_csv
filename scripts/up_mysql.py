import pandas as pd
from scripts.conn_mysql import ConnMySQL


class UpToMySQL():
    def __init__(self):
        self.ms = ConnMySQL()
        self.kaggle_path = 'data/from_kaggle/Mobiles Dataset (2025).csv'
        self.df = None
        self.extract_data_from_csv()

    def extract_data_from_csv(self):
        self.df = pd.read_csv(
            self.kaggle_path, delimiter=',', encoding="windows-1252")
        print(
            f'Alguns dados Extraidos de {self.kaggle_path}: \n{self.df.sample(5)}')
