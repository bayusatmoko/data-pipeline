FROM apache/airflow:2.7.3

USER root

# System deps (must be root)
RUN apt-get update && apt-get install -y \
      wget \
      curl \
      openjdk-11-jdk \
      libkrb5-dev \
      gcc \
      g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch to airflow user for pip installs
USER airflow

# Install Airflow providers under airflow user
RUN pip install --no-cache-dir \
      apache-airflow-providers-apache-hdfs \
      apache-airflow-providers-apache-spark

# Back to root for Spark install
USER root
ENV SPARK_VERSION=3.4.1
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -O /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
    && rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Drop back down
USER airflow