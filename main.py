from scripts.up_mysql import UpToMySQL
from scripts.down_mysql import DownFromMySQL

if __name__ == "__main__":
    # Executa a classe UpToMySQL para criar a base de dados e a tabela no MySQL, e inserir os dados do CSV na tabela criada.
    kaggle_file_name = 'Mobiles Dataset (2025).csv'
    db_name = 'db_mobiles'
    tb_name = 'mobiles'
    UpToMySQL(kaggle_file_name, db_name, tb_name)
    
    # Executa a classe DownFromMySQL para extrair os dados do MySQL, transformar os dados e criar um CSV com os dados transformados.
    DownFromMySQL()

