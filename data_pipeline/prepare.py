import pandas as pd
from data_pipeline.data_processing import prepare_dataframe_for_insert # Substitua 'your_module' pelo nome do seu módulo ou caminho para o arquivo

# Carregar o DataFrame do arquivo Parquet gerado anteriormente
df = pd.read_parquet('raw_data_20240820124303.parquet')

# Executa a função prepare_dataframe_for_insert
df_prepared = prepare_dataframe_for_insert(df)

# Verifica o resultado
print("DataFrame preparado para inserção no ClickHouse:")
print(df_prepared)
