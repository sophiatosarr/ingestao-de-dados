from flask import Flask, request, jsonify
from datetime import datetime
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd
import requests  # Para fazer requisições à Dog API

app = Flask(__name__)

# Criar bucket se não existir
create_bucket_if_not_exists("raw-data")

# Executar o script SQL para criar a tabela
execute_sql_script('sql/create_table.sql')

@app.route('/data', methods=['POST'])
def receive_data():
    data = request.get_json()
    if not data or 'date' not in data or 'dados' not in data:
        return jsonify({"error": "Formato de dados inválido"}), 400

    try:
        datetime.fromtimestamp(data['date'])
        int(data['dados'])
    except (ValueError, TypeError):
        return jsonify({"error": "Tipo de dados inválido"}), 400

    # Processar e salvar dados
    filename = process_data(data)
    upload_file("raw-data", filename)

    # Ler arquivo Parquet do MinIO
    download_file("raw-data", filename, f"downloaded_{filename}")
    df_parquet = pd.read_parquet(f"downloaded_{filename}")

    # Preparar e inserir dados no ClickHouse
    df_prepared = prepare_dataframe_for_insert(df_parquet)
    client = get_client()  # Obter o cliente ClickHouse
    insert_dataframe(client, 'working_data', df_prepared)

    return jsonify({"message": "Dados recebidos, armazenados e processados com sucesso"}), 200

@app.route('/ingest_dog_data', methods=['GET'])
def ingest_dog_data():
    # Fazer requisição à Dog API
    url = "https://dogapi.dog/api/v2/breeds"
    response = requests.get(url)
    if response.status_code != 200:
        return jsonify({"error": "Erro ao acessar a Dog API"}), 500

    dog_data = response.json()

    # Processar dados (você pode adaptar esta parte conforme necessário)
    processed_data = {"date": datetime.now().timestamp(), "dados": len(dog_data['data'])}  # Exemplo de processamento

    # Salvar os dados
    filename = process_data(processed_data)
    upload_file("raw-data", filename)

    # Ler arquivo Parquet do MinIO
    download_file("raw-data", filename, f"downloaded_{filename}")
    df_parquet = pd.read_parquet(f"downloaded_{filename}")

    # Preparar e inserir dados no ClickHouse
    df_prepared = prepare_dataframe_for_insert(df_parquet)
    client = get_client()  # Obter o cliente ClickHouse
    insert_dataframe(client, 'working_data', df_prepared)

    return jsonify({"message": "Dados da Dog API ingeridos com sucesso"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
