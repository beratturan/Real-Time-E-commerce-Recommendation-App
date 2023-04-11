FROM --platform=linux/amd64 python:3.9.1

RUN apt-get update && apt-get install -y curl wget nano
RUN pip install pyarrow && pip install numpy && pip install pyspark && pip install findspark

# https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
# VERSIONS
ENV SPARK_VERSION=3.3.2 \
HADOOP_VERSION=3 \
JAVA_VERSION=17

# Official JavaDK17 Dockerfile code
ENV JAVA_PKG=https://download.oracle.com/java/17/latest/jdk-17_linux-x64_bin.tar.gz \
    JAVA_HOME=/usr/java/jdk-17
ENV PATH $JAVA_HOME/bin:$PATH

RUN set -eux; \
      JAVA_SHA256=$(curl "$JAVA_PKG".sha256) ; \
      curl --output /tmp/jdk.tgz "$JAVA_PKG" && \
      echo "$JAVA_SHA256 */tmp/jdk.tgz" | sha256sum -c; \
      mkdir -p "$JAVA_HOME"; \
      tar --extract --file /tmp/jdk.tgz --directory "$JAVA_HOME" --strip-components 1

# DOWNLOAD SPARK AND INSTALL
RUN DOWNLOAD_URL_SPARK="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && wget --no-verbose -O apache-spark.tgz  "${DOWNLOAD_URL_SPARK}"\
    && mkdir -p /opt/spark \
    && tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
    && rm apache-spark.tgz

# SET SPARK ENV VARIABLES
ENV SPARK_HOME="/opt/spark"
ENV PATH="${SPARK_HOME}/bin/:${PATH}"

# SET PYSPARK VARIABLES
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"


# Let's change to  "$NB_USER" command so the image runs as a non root user by default
USER $NB_UID
EXPOSE 4040

RUN mkdir -p /usr/src/apps/spark
WORKDIR /usr/src/apps/spark
COPY StreamReaderApp.py .


CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.elasticsearch:elasticsearch-spark-30_2.12:8.6.2","StreamReaderApp.py"]
