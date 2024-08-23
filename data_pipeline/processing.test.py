import os
import pandas as pd
from data_pipeline.data_processing import process_data  # Substitua 'your_module' pelo nome do seu módulo ou caminho para o arquivo

# Dados de exemplo para teste
data_example = {
    "date": 1692556800,  # Exemplo de timestamp (2024-08-20 00:00:00)
    "dados": 42  # Exemplo de valor de dados
}

# Executa a função process_data com os dados de exemplo
parquet_file = process_data(data_example)

# Verifica se o arquivo Parquet foi criado
if os.path.exists(parquet_file):
    print(f"Arquivo Parquet '{parquet_file}' criado com sucesso!")
    
    # Carrega o arquivo Parquet para verificação
    df = pd.read_parquet(parquet_file)
    print("Dados carregados do Parquet:")
    print(df)
else:
    print("Erro: Arquivo Parquet não foi criado.")
