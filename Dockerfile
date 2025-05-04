FROM python:3.12.9-slim

RUN apt-get update \
    && apt-get install -y build-essential openjdk-17-jre \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work

RUN pip install --upgrade jupyterlab
RUN pip install pyspark

EXPOSE 8888

CMD jupyter lab --port-retries=0 --ip=0.0.0.0 --allow-root --IdentityProvider.token="" --ServerApp.password=""