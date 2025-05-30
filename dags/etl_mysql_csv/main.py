from script.up_mysql import UpToMySQL
from script.down_mysql import DownFromMySQL

def subir_e_extrair_dados():
    if __name__ == "__main__":
        # Executa a classe UpToMySQL para criar a base de dados e a tabela no MySQL, e inserir os dados do CSV na tabela criada.
        kaggle_file_name = 'Mobiles Dataset (2025).csv'
        db_name = 'db_mobiles'
        tb_name = 'mobiles'
        UpToMySQL(kaggle_file_name, db_name, tb_name)

        # Executa a classe DownFromMySQL para extrair os dados do MySQL, transformar os dados e criar um CSV com os dados transformados.
        my_sql_file_name = 'mobiles_dataset_2025.csv'
        db_name = 'db_mobiles'
        tb_name = 'mobiles'
        split_column_name = 'model_name'
        columns = ['company_name', 'battery_capacity', 'ram', 'screen_size', split_column_name]
        DownFromMySQL(my_sql_file_name, db_name, tb_name, columns, split_column_name)

