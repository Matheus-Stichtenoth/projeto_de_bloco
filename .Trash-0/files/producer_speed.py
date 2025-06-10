{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "3da7d9cc-107f-4652-a94b-0682afee73ba",
   "metadata": {},
   "outputs": [],
   "source": [
    "import boto3\n",
    "import pandas as pd\n",
    "import os\n",
    "import json\n",
    "from kafka import KafkaProducer\n",
    "from dotenv import load_dotenv\n",
    "# Carrega o conteúdo do .env\n",
    "load_dotenv(dotenv_path=\".env\")\n",
    "\n",
    "# Configurações\n",
    "BUCKET_NAME = 'speed'\n",
    "KAFKA_TOPIC = 'speed-topic'\n",
    "KAFKA_SERVER = 'kafka:9092'\n",
    "MINIO_ENDPOINT = 'http://minio:9000'\n",
    "\n",
    "# Conectar ao MinIO com boto3\n",
    "s3 = boto3.client(\n",
    "    's3',\n",
    "    endpoint_url=MINIO_ENDPOINT,\n",
    "    aws_access_key_id=os.getenv(\"MINIO_USER\", \"minioadmin\"),\n",
    "    aws_secret_access_key=os.getenv(\"MINIO_PASSWORD\", \"minioadmin\")\n",
    ")\n",
    "\n",
    "# Criar producer Kafka\n",
    "producer = KafkaProducer(\n",
    "    bootstrap_servers=KAFKA_SERVER,\n",
    "    value_serializer=lambda v: json.dumps(v).encode('utf-8')\n",
    ")\n",
    "\n",
    "# Listar arquivos CSV do bucket 'speed'\n",
    "response = s3.list_objects_v2(Bucket=BUCKET_NAME)\n",
    "arquivos = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]\n",
    "\n",
    "# Ler arquivos e enviar mensagens\n",
    "for arquivo in arquivos:\n",
    "    print(f\"Lendo arquivo: {arquivo}\")\n",
    "    obj = s3.get_object(Bucket=BUCKET_NAME, Key=arquivo)\n",
    "    df = pd.read_csv(obj['Body'])\n",
    "    \n",
    "    for _, row in df.iterrows():\n",
    "        data = row.to_dict()\n",
    "        producer.send(KAFKA_TOPIC, value=data)\n",
    "        print(f\"Mensagem enviada: {data}\")\n",
    "\n",
    "print(\"✅ Producer finalizado com sucesso!\")\n"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.10"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
