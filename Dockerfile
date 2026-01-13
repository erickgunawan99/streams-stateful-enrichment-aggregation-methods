# USE SPARK 3.5.3 (Stable standard for Delta 3.2)
FROM apache/spark:3.5.3-python3

USER root

# Environment setup
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
# Spark 3.5.3 uses Java 17 by default in this image usually, or 11. 
# The base image handles JAVA_HOME, but usually:
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

WORKDIR /opt/spark/jars

# 1. POSTGRES (Keep as is)
RUN curl -fLo postgresql-42.7.3.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar || true

# 2. KAFKA (Switched to Scala 2.12 to match Spark 3.5 default)
# Note: Spark 3.5.x aligns best with Kafka Clients 3.4/3.5 usually, but 3.7 often works. 
# We use the correct "spark-sql-kafka" version for Spark 3.5.3.
RUN curl -fLO https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fLO https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fLO https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar

# 3. DELTA LAKE (Bake these in!)
# You need BOTH delta-spark and delta-storage
RUN curl -fLO https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar && \
    curl -fLO https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar

# Python Dependencies
RUN python3 -m pip install --upgrade pip && \
    pip install pandas pyarrow protobuf delta-spark==3.2.1

# Permissions
RUN mkdir -p /opt/spark/jobs /opt/spark/logs /opt/spark/checkpoints /opt/spark/warehouse && \
    chown -R 185:185 /opt/spark

USER spark
WORKDIR /opt/spark
CMD ["/bin/bash"]