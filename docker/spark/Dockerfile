# docker/spark/Dockerfile
# Custom Spark image with Delta Lake, MinIO (S3A), and ML dependencies

FROM bitnami/spark:3.5.0

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    delta-spark==3.0.0 \
    pyspark==3.5.0 \
    kafka-python==2.0.2 \
    boto3==1.34.0 \
    scikit-learn==1.4.0 \
    pandas==2.1.4 \
    numpy==1.26.3 \
    pyarrow==14.0.2 \
    prometheus-client==0.19.0

# Download Spark/Hadoop AWS + Delta JARs
ENV SPARK_JARS_DIR=/opt/bitnami/spark/jars

RUN curl -sL -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -sL -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -sL -o ${SPARK_JARS_DIR}/delta-spark_2.12-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar && \
    curl -sL -o ${SPARK_JARS_DIR}/delta-storage-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

# Spark defaults — S3A config for MinIO
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

USER 1001
