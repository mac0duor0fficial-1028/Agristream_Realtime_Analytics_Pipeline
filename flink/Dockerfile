FROM flink:1.18.1-java11

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.18.1

ARG MAVEN=https://repo.maven.apache.org/maven2
RUN wget -q -P /opt/flink/lib \
      ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar && \
    wget -q -P /opt/flink/lib \
      ${MAVEN}/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar && \
    wget -q -P /opt/flink/lib \
      ${MAVEN}/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar