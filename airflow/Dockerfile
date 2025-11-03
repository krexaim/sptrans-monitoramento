FROM apache/airflow:2.10.2-python3.11

USER root

# Instala Java + dependências
RUN apt-get update -o Acquire::Retries=5 \
 && apt-get install -y --no-install-recommends openjdk-11-jdk-headless curl wget ca-certificates gnupg \
 && rm -rf /var/lib/apt/lists/*

# Instala o Spark client 
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -qO- https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar -xz -C /usr/local/ && \
    mv /usr/local/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /usr/local/spark

# Configura variáveis
ENV SPARK_HOME=/usr/local/spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER airflow

WORKDIR /opt/airflow