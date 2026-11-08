#!/usr/bin/env bash
# ------------------------------------------------------------------
# Generateur de charge (profil load) : production continue et bridee
# sur le topic $LOADGEN_TOPIC, pour alimenter les dashboards.
#
# Client Java standard (kafka-producer-perf-test) instrumente par l'agent
# JMX exporter : ses metriques sont exposees sur loadgen-producer:9404.
# ------------------------------------------------------------------
set -euo pipefail
TOPIC="${LOADGEN_TOPIC:-loadgen}"
RATE="${LOADGEN_THROUGHPUT:-100}"
SIZE="${LOADGEN_RECORD_SIZE:-256}"
BIN=/opt/kafka/bin

echo "[loadgen-producer] Creation du topic $TOPIC (si absent)"
"$BIN/kafka-topics.sh" --bootstrap-server "$BS" --create --if-not-exists \
  --topic "$TOPIC" --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=3600000 \
  --config retention.bytes=10485760 \
  --config segment.bytes=2097152
# Volume borne par la taille : 10 Mo par partition plus un segment actif de
# 2 Mo, soit 60 a 72 Mo par broker (6 partitions), environ 35 min d'historique
# a 100 msg/s. Mesure : la charge par defaut ecrit environ 100 Mo/h par broker ;
# avec des segments de 16 Mo, le volume oscillait entre 100 et 200 Mo et
# pouvait declencher a tort l'alerte KafkaBrokerLogSizeHigh (seuil 250 Mo).

# Agent JMX reserve au processus de production (pas a kafka-topics.sh ci-dessus).
export KAFKA_OPTS="-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar=9404:/opt/jmx-exporter/kafka-client.yml"

echo "[loadgen-producer] Production continue : ${RATE} msg/s de ${SIZE} octets, acks=all"
# --num-records tres grand : le test tourne pendant toute la formation.
exec "$BIN/kafka-producer-perf-test.sh" \
  --bootstrap-server "$BS" \
  --topic "$TOPIC" \
  --num-records 1000000000 \
  --record-size "$SIZE" \
  --throughput "$RATE" \
  --reporting-interval 30000 \
  --command-property acks=all client.id=loadgen-producer linger.ms=20 compression.type=lz4
