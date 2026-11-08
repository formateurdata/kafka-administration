# Image commune aux noeuds, outils, clients de charge et Kafka Connect.
# Les configurations des TP, certificats et volumes restent sur le poste.
FROM apache/kafka:4.3.2

COPY monitoring/jmx-exporter/jmx_prometheus_javaagent-1.7.0.jar /opt/jmx-exporter/
RUN echo '0ad843261c567d1924a017d542e14c40f2afff523d940c25ee000742fde4b3a9  /opt/jmx-exporter/jmx_prometheus_javaagent-1.7.0.jar' | sha256sum -c -
COPY config/kafka/common/start-node.sh /opt/lab/common/start-node.sh
COPY scripts/loadgen/producer.sh scripts/loadgen/consumer.sh /opt/lab/loadgen/
