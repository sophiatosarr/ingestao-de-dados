import pytest
from flask import Flask
from flask.testing import FlaskClient
from app import app  # Certifique-se de que 'app' é o nome do seu arquivo principal

@pytest.fixture
def client() -> FlaskClient:
    app.config['TESTING'] = True
    return app.test_client()

def test_ingest_dog_data(client):
    response = client.get('/ingest_dog_data')
    assert response.status_code == 200
    assert b'Dados da Dog API ingeridos com sucesso' in response.data

def test_receive_data(client):
    sample_data = {
        "date": 1692345600,
        "dados": 12345
    }
    response = client.post('/data', json=sample_data)
    assert response.status_code == 200
    assert b'Dados recebidos, armazenados e processados com sucesso' in response.data
