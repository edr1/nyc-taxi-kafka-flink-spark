FROM flink:1.20.1-scala_2.12-java17
USER root
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
COPY flink/streaming.sql /opt/flink/usrlib/streaming.sql
USER flink
