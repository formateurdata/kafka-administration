#!/usr/bin/env bash
# ------------------------------------------------------------------
# Demarrage du noeud unique du cluster "cloud" (profil mirror, TP9).
#
# Meme principe que start-node.sh (formatage puis lancement), avec deux
# differences :
#   - quorum d'un seul controleur (--standalone) ;
#   - creation du compte SCRAM de MirrorMaker 2 au formatage (--add-scram),
#     comme un compte fourni avec un cluster gere (AWS Secrets Manager
#     pour MSK, par exemple).
#
# Variables attendues (docker-compose.yml) :
#   CLOUD_CLUSTER_ID   identifiant du cluster cloud
#   MM2_PASSWORD       mot de passe du compte mm2
# ------------------------------------------------------------------
set -euo pipefail

CONFIG=/opt/lab/node/server.properties

echo "[start-cloud] Formatage du stockage (cluster ${CLOUD_CLUSTER_ID})"
/opt/kafka/bin/kafka-storage.sh format \
  --config "$CONFIG" \
  --cluster-id "$CLOUD_CLUSTER_ID" \
  --ignore-formatted \
  --standalone \
  --add-scram "SCRAM-SHA-512=[name=mm2,password=${MM2_PASSWORD}]"

export KAFKA_HEAP_OPTS="${KAFKA_NODE_HEAP_OPTS:--Xms512m -Xmx512m}"
export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=/opt/lab/common/log4j2.yaml"
export LOG_DIR=/opt/kafka/logs

echo "[start-cloud] Lancement de Kafka"
exec /opt/kafka/bin/kafka-server-start.sh "$CONFIG"
