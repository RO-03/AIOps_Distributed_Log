# docker/spark/Dockerfile
# apache/spark:3.5.0 ships Python 3.8 — all pins are py38-compatible
FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir \
    delta-spark==3.0.0 \
    kafka-python==2.0.2 \
    boto3==1.34.0 \
    scikit-learn==1.3.2 \
    pandas==2.0.3 \
    numpy==1.24.4 \
    pyarrow==12.0.1 \
    prometheus-client==0.19.0

ENV SPARK_JARS_DIR=/opt/spark/jars

RUN curl -sL -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -sL -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -sL -o ${SPARK_JARS_DIR}/delta-spark_2.12-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar && \
    curl -sL -o ${SPARK_JARS_DIR}/delta-storage-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark