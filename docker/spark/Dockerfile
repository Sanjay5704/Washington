FROM python:3.11-slim-bookworm

# PySpark needs a JVM; Spark runs in local[*] mode inside this one container.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "pyspark==3.5.3" "requests==2.32.3" "boto3==1.35.99" "psycopg2-binary==2.9.10"

# PostgreSQL JDBC driver for spark.write.jdbc
RUN mkdir -p /opt/jars \
    && curl -fsSL -o /opt/jars/postgresql.jar \
       https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
WORKDIR /opt/washington
