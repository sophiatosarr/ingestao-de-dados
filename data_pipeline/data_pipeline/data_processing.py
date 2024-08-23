import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def process_data(data):
    """
    Processa os dados recebidos, criando um DataFrame, convertendo-o em uma tabela Arrow,
    e salvando-o como um arquivo Parquet.

    Args:
    data (dict): Os dados recebidos da API para serem processados.

    Returns:
    str: O nome do arquivo Parquet criado.
    """
    # Criar DataFrame a partir dos dados recebidos
    df = pd.DataFrame([data])
    
    # Gerar um nome de arquivo único com base na data e hora atuais
    filename = f"raw_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    
    # Converter o DataFrame em uma tabela Arrow
    table = pa.Table.from_pandas(df)
    
    # Salvar a tabela como um arquivo Parquet
    pq.write_table(table, filename)
    
    # Retornar o nome do arquivo para referência futura
    return filename

def prepare_dataframe_for_insert(df):
    """
    Prepara o DataFrame para inserção no banco de dados ClickHouse, adicionando colunas de metadados
    e transformando as linhas em formato JSON.

    Args:
    df (pd.DataFrame): O DataFrame que foi lido do arquivo Parquet.

    Returns:
    pd.DataFrame: O DataFrame preparado para inserção no ClickHouse.
    """
    # Adicionar uma coluna com a data e hora de ingestão
    df['data_ingestao'] = datetime.now()
    
    # Converter cada linha do DataFrame em uma string JSON
    df['dado_linha'] = df.apply(lambda row: row.to_json(), axis=1)
    
    # Adicionar uma coluna de tag para identificação ou categorização
    df['tag'] = 'example_tag'
    
    # Retornar apenas as colunas relevantes para a inserção no banco de dados
    return df[['data_ingestao', 'dado_linha', 'tag']]
