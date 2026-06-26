# docker/spark/Dockerfile
# Phase 3-compatible Spark image:
#   - delta-spark 3.0.0
#   - spark-sql-kafka connector (matches Spark 3.5 + Scala 2.12)
#   - PostgreSQL JDBC driver
#   - ML libraries (scikit-learn, pandas, numpy)
#   - boto3 for MinIO model metadata writes

FROM apache/spark:3.5.0

USER root

# Python ML + utility dependencies
RUN pip install --no-cache-dir \
    delta-spark==3.0.0 \
    kafka-python-ng==2.2.3 \
    boto3==1.34.0 \
    scikit-learn==1.3.2 \
    pandas==2.0.3 \
    numpy==1.24.4 \
    pyarrow==12.0.1 \
    prometheus-client==0.19.0 \
    psycopg2-binary==2.9.9

ENV SPARK_JARS_DIR=/opt/spark/jars

# Hadoop-AWS (S3A) + AWS SDK — for MinIO access
RUN curl -sL -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -sL -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Delta Lake JARs
RUN curl -sL -o ${SPARK_JARS_DIR}/delta-spark_2.12-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar && \
    curl -sL -o ${SPARK_JARS_DIR}/delta-storage-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

# Kafka Structured Streaming connector (Spark 3.5 + Scala 2.12)
RUN curl -sL -o ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -sL -o ${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -sL -o ${SPARK_JARS_DIR}/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -sL -o ${SPARK_JARS_DIR}/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# PostgreSQL JDBC driver (for PySpark JDBC writes to postgres-db)
RUN curl -sL -o ${SPARK_JARS_DIR}/postgresql-42.7.1.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar

# Copy Spark config
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Create app directory for scripts
RUN mkdir -p /app/src

USER spark