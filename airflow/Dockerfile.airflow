FROM apache/airflow:2.10.2-python3.11

USER root

# Instala Java (se necessário) e dependências de sistema
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
WORKDIR /opt/airflow

# Copia o requirements
COPY requirements.txt /opt/airflow/requirements.txt

USER airflow
ENV PYTHONPATH="${PYTHONPATH}:/home/airflow/.local/lib/python3.11/site-packages"

# Instala dependências Python do projeto
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt