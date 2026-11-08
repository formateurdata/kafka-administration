#!/usr/bin/env bash
# ------------------------------------------------------------------
# Consommateur continu (profil load) du topic $LOADGEN_TOPIC.
# Groupe : loadgen-consumers. Les messages sont lus puis ignores.
#
# Arreter ce service fait croitre le lag du groupe (TP6, incident 2) :
#     docker compose stop loadgen-consumer
# Ses metriques client (records-lag-max...) sont sur loadgen-consumer:9404.
# ------------------------------------------------------------------
set -euo pipefail
TOPIC="${LOADGEN_TOPIC:-loadgen}"
BIN=/opt/kafka/bin

echo "[loadgen-consumer] Attente du topic $TOPIC"
until "$BIN/kafka-topics.sh" --bootstrap-server "$BS" --describe --topic "$TOPIC" >/dev/null 2>&1; do
  sleep 3
done

export KAFKA_OPTS="-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar=9404:/opt/jmx-exporter/kafka-client.yml"

echo "[loadgen-consumer] Consommation du topic $TOPIC (groupe loadgen-consumers)"
exec "$BIN/kafka-console-consumer.sh" \
  --bootstrap-server "$BS" \
  --topic "$TOPIC" \
  --group loadgen-consumers \
  --command-property client.id=loadgen-consumer \
  > /dev/null
