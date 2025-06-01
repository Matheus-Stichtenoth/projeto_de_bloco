# Usa a imagem oficial do Spark 3.5 da Bitnami (compatível com master e workers)
FROM bitnami/spark:3.5

# Troca para o usuário root para instalar pacotes
USER root

# Atualiza os pacotes e instala Python + pip + dependências úteis
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Instala JupyterLab e bibliotecas Python comuns para ciência de dados
RUN pip install --upgrade pip && \
    pip install jupyterlab pandas matplotlib seaborn pyspark

# Define diretório de trabalho padrão (você pode mudar se quiser)
WORKDIR /home/jovyan/work

# Expõe as portas do Jupyter (8888) e Spark UI local (4040)
EXPOSE 8888 4040

# Comando para iniciar o JupyterLab sem senha/token, acessível externamente
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''", "--ServerApp.disable_check_xsrf=True"]