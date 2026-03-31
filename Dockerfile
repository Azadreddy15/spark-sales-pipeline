FROM python:3.11-slim

RUN apt-get update && apt-get install -y default-jdk procps && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3

WORKDIR /opt/spark-apps

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .