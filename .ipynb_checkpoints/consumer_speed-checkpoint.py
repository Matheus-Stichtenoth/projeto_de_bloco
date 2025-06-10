from kafka import KafkaConsumer
from datetime import datetime
import boto3
import os
from dotenv import load_dotenv

#Carrega o conteúdo do .env
load_dotenv(dotenv_path=".env")

ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')
BUCKET_NAME = os.getenv('BUCKET_NAME')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_SERVER = os.getenv('KAFKA_SERVER')
MINIO_ENDPOINT = os.getenv('MINIO_ROOT_BOTO3')
BUCKET_BRONZE = 'bronze'

#Inicializando o consumidor Kafka
consumer = KafkaConsumer(
    TOPICO,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='grupo-speed'
)

#Configurando boto3 pra acessar o minio
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

print('Consumer iniciado. Escutando o tópico...')

#Consumindo mensagens e enviado pro MinIO
for mensagem in consumer:
    conteudo = mensagem.value.decode('utf-8')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nome_arquivo = f'speed_{timestamp}.csv'

    s3.put_object(Bucket=BUCKET_BRONZE, Key=nome_arquivo, Body=conteudo.encode('utf-8'))

    print(f'✅ Arquivo enviado para o bucket {BUCKET_BRONZE}: {nome_arquivo}')