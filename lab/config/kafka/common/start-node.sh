#!/usr/bin/env bash
# ------------------------------------------------------------------
# Demarrage d'un noeud Kafka en mode KRaft.
#
# Ce script reproduit les deux gestes d'une installation sur serveur :
#   1. formater le stockage (kafka-storage.sh format) ;
#   2. lancer le serveur (kafka-server-start.sh).
#
# Il est monte en lecture seule dans chaque conteneur Kafka et appele
# par docker-compose.yml. La configuration du noeud est lue dans
# /opt/lab/node/server.properties (dossier config/kafka/kafka-N/ de l'hote).
#
# Variables attendues (fournies par docker-compose.yml depuis .env) :
#   CLUSTER_ID                 identifiant du cluster (commun a tous les noeuds)
#   KAFKA_INITIAL_CONTROLLERS  quorum initial id@hote:port:directory.id,...
#   KAFKA_NODE_HEAP_OPTS       options memoire de la JVM du noeud
#   JMX_EXPORTER_VERSION       version de l'agent Prometheus JMX exporter
# ------------------------------------------------------------------
set -euo pipefail

CONFIG=/opt/lab/node/server.properties
JMX_AGENT=/opt/jmx-exporter/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar
JMX_RULES=/opt/jmx-exporter/kafka-broker.yml

if [[ ! -f "$CONFIG" ]]; then
  echo "[start-node] ERREUR : $CONFIG introuvable" >&2
  exit 1
fi

# --- 1. Formatage du stockage --------------------------------------
# --ignore-formatted : les repertoires deja formates sont ignores, ce qui
# rend l'operation idempotente a chaque redemarrage. Un nouveau repertoire
# ajoute a log.dirs est formate au demarrage suivant.
if grep -Eq '^process\.roles=.*controller' "$CONFIG"; then
  # Noeud controleur : le quorum initial est declare (quorum dynamique).
  FORMAT_MODE=(--initial-controllers "${KAFKA_INITIAL_CONTROLLERS}")
else
  # Broker seul : il rejoint un quorum existant sans en faire partie.
  FORMAT_MODE=(--no-initial-controllers)
fi

echo "[start-node] Formatage du stockage (cluster ${CLUSTER_ID})"
/opt/kafka/bin/kafka-storage.sh format \
  --config "$CONFIG" \
  --cluster-id "$CLUSTER_ID" \
  --ignore-formatted \
  "${FORMAT_MODE[@]}"

# --- 2. Options JVM reservees au processus serveur -----------------
# Elles sont exportees ici et non dans l'environnement du conteneur :
# ainsi, un outil lance par "docker compose exec kafka-1 kafka-topics.sh"
# n'herite ni de l'agent JMX (conflit sur le port 9404) ni du heap serveur.
export KAFKA_HEAP_OPTS="${KAFKA_NODE_HEAP_OPTS:--Xms512m -Xmx512m}"
export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=/opt/lab/common/log4j2.yaml"
export LOG_DIR=/opt/kafka/logs
if [[ -f "$JMX_AGENT" ]]; then
  export KAFKA_OPTS="-javaagent:${JMX_AGENT}=9404:${JMX_RULES}"
else
  echo "[start-node] ATTENTION : agent JMX absent ($JMX_AGENT)." >&2
  echo "[start-node] Lancer scripts/prepare.sh. Le noeud demarre sans metriques Prometheus." >&2
fi

echo "[start-node] Lancement de Kafka"
exec /opt/kafka/bin/kafka-server-start.sh "$CONFIG"
