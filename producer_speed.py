import boto3
import pandas as pd
import os
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
#Carrega o conteúdo do .env
load_dotenv(dotenv_path=".env")

ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')
BUCKET_NAME = os.getenv('BUCKET_NAME')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_SERVER = os.getenv('KAFKA_SERVER')
MINIO_ENDPOINT = os.getenv('MINIO_ROOT_BOTO3')

#Configurando boto3 pra acessar o minio
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

#Criando producer Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#Listando arquivos CSV do bucket 'speed'
resposta = s3.list_objects_v2(Bucket=BUCKET_NAME)
arquivos = [obj['Key'] for obj in resposta.get('Contents', []) if obj['Key'].endswith('.csv')]

if not arquivos:
    print("⚠️ Nenhum arquivo CSV encontrado no bucket speed.")
    exit(0)  #Finaliza o script com sucesso, mas sem enviar nada se não houver novos arquivos

#Lendo arquivos e enviando mensagens ao kafka
for arquivo in arquivos:
    print(f"Lendo arquivo: {arquivo}")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=arquivo)
    df = pd.read_csv(obj['Body'])
    
    for _, row in df.iterrows():
        data = row.to_dict()
        producer.send(KAFKA_TOPIC, value=data)
        print(f"Mensagem enviada: {data}")

print("✅ Producer finalizado com sucesso!")