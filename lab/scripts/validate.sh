#!/usr/bin/env bash
# ------------------------------------------------------------------
# Validation de l'environnement : verifie que le lab est operationnel.
#
# Usage : scripts/validate.sh
# Les services optionnels (monitoring, ecosystem, kafka-4) sont testes
# seulement s'ils sont demarres. Code retour 0 si tout est correct.
# ------------------------------------------------------------------
set -uo pipefail
source "$(dirname "$0")/lib.sh"

ERRORS=0
check() {  # check "<libelle>" <commande...>
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then ok "$label"; else fail "$label"; ERRORS=$((ERRORS + 1)); fi
}
running() { [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == "true" ]]; }
healthy() { [[ "$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null)" == "healthy" ]]; }

echo "== Conteneurs"
for c in kafka-1 kafka-2 kafka-3; do check "$c sain (healthcheck)" healthy "$c"; done
check "kafka-tools demarre" running kafka-tools
check "kafka-ui demarre" running kafka-ui
check "jupyter sain (healthcheck)" healthy jupyter
EXPECTED_BROKERS=3
if running kafka-4; then check "kafka-4 sain (healthcheck)" healthy kafka-4; EXPECTED_BROKERS=4; fi

echo "== Cluster Kafka (depuis le conteneur tools)"
# Un seul appel au conteneur pour limiter le nombre de JVM lancees.
cluster_report="$(dc exec -T -e EXPECTED_BROKERS="$EXPECTED_BROKERS" tools bash -s <<'EOF'
set -u
q="$(kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status 2>&1)"
leader="$(awk '/^LeaderId:/ {print $2}' <<<"$q")"
voters="$(grep -o '"id": [0-9]*' <<<"$(grep CurrentVoters <<<"$q")" | wc -l | tr -d ' ')"
maxlag="$(awk '/^MaxFollowerLag:/ {print $2}' <<<"$q")"
echo "QUORUM_LEADER=${leader:-none}"
echo "QUORUM_VOTERS=${voters:-0}"
echo "QUORUM_MAXLAG=${maxlag:-unknown}"
brokers="$(kafka-broker-api-versions.sh --bootstrap-server "$BS" 2>/dev/null | grep -c '^kafka-[0-9]*:19092')"
echo "BROKERS=$brokers"
echo "URP=$(kafka-topics.sh --bootstrap-server "$BS" --describe --under-replicated-partitions 2>/dev/null | grep -c Partition)"
echo "UNAVAILABLE=$(kafka-topics.sh --bootstrap-server "$BS" --describe --unavailable-partitions 2>/dev/null | grep -c Partition)"
# Aller-retour production / consommation sur un topic dedie.
kafka-topics.sh --bootstrap-server "$BS" --create --if-not-exists --topic lab-validate \
  --partitions 1 --replication-factor 3 >/dev/null 2>&1
token="validate-$(date +%s)-$RANDOM"
echo "$token" | kafka-console-producer.sh --bootstrap-server "$BS" --topic lab-validate \
  --request-required-acks all >/dev/null 2>&1
# Relecture de la partition depuis le debut et recherche du jeton.
found="$(kafka-console-consumer.sh --bootstrap-server "$BS" --topic lab-validate --partition 0 \
  --offset earliest --timeout-ms 10000 2>/dev/null | grep -c "^$token$")"
[[ "$found" -ge 1 ]] && got="$token" || got=""
[[ "$got" == "$token" ]] && echo "ROUNDTRIP=ok" || echo "ROUNDTRIP=ko"
# Interfaces web joignables depuis le reseau Docker.
wget -qO- http://kafka-ui:8080/api/clusters 2>/dev/null | grep -q '"status":"ONLINE"' && echo "UI=ok" || echo "UI=ko"
EOF
)"
val() { grep "^$1=" <<<"$cluster_report" | cut -d= -f2; }

check "Quorum KRaft : leader elu (node $(val QUORUM_LEADER))" test "$(val QUORUM_LEADER)" -ge 1
check "Quorum KRaft : 3 votants" test "$(val QUORUM_VOTERS)" -eq 3
check "Brokers enregistres : $(val BROKERS)/$EXPECTED_BROKERS" test "$(val BROKERS)" -eq "$EXPECTED_BROKERS"
check "Aucune partition sous-repliquee (URP=$(val URP))" test "$(val URP)" -eq 0
check "Aucune partition indisponible ($(val UNAVAILABLE))" test "$(val UNAVAILABLE)" -eq 0
check "Aller-retour production/consommation (topic lab-validate)" test "$(val ROUNDTRIP)" = ok
check "Kafka UI : cluster ONLINE" test "$(val UI)" = ok
# Client Python des notebooks : bibliotheque chargee et cluster joignable.
# Le client est garde dans une variable : detruit avant la reponse, il echoue (_DESTROY).
check "Jupyter : client Python Kafka connecte ($EXPECTED_BROKERS brokers)" docker exec jupyter python3 -c \
  "import os, sys; from confluent_kafka.admin import AdminClient; \
a = AdminClient({'bootstrap.servers': os.environ['BS']}); r = a.describe_cluster(request_timeout=15).result(); \
sys.exit(0 if len(r.nodes) == $EXPECTED_BROKERS else 1)"

if running prometheus; then
  echo "== Monitoring"
  check "Prometheus sain" healthy prometheus
  up_count="$(dc exec -T tools bash -c \
    "wget -qO- 'http://prometheus:9090/api/v1/query?query=sum(up%7Bjob%3D%22kafka%22%7D)'" 2>/dev/null \
    | grep -o '"value":\[[^]]*\]' | grep -o '"[0-9]*"' | tr -d '"')"
  check "Prometheus : 3 noeuds Kafka collectes (up=${up_count:-0})" test "${up_count:-0}" -eq 3
  # Serie ALERTS de Prometheus : une serie par alerte en etat firing.
  firing="$(dc exec -T tools bash -c \
    "wget -qO- 'http://prometheus:9090/api/v1/query?query=ALERTS%7Balertstate%3D%22firing%22%7D'" 2>/dev/null \
    | grep -o '"alertname":"[A-Za-z]*"' | cut -d'"' -f4 | sort -u | tr '\n' ' ')"
  if [[ -z "$firing" ]]; then ok "Aucune alerte en cours"; else warn "Alertes en cours : $firing"; fi
  if running grafana; then
    check "Grafana sain" healthy grafana
    check "Grafana : dashboard kafka-cluster provisionne" \
      dc exec -T tools bash -c "wget -qO- http://grafana:3000/api/dashboards/uid/kafka-cluster | grep -q NovaShop"
  fi
  if running kafka-exporter; then
    check "kafka-exporter repond" dc exec -T tools bash -c "wget -qO- http://kafka-exporter:9308/metrics | grep -q '^kafka_brokers'"
  fi
fi

if running loadgen-producer; then
  echo "== Charge"
  check "loadgen-producer demarre" running loadgen-producer
  check "loadgen-consumer demarre" running loadgen-consumer
fi

if running schema-registry || running connect; then
  echo "== Ecosysteme"
  running schema-registry && check "Schema Registry repond" \
    dc exec -T tools bash -c "wget -qO- http://schema-registry:8081/subjects"
  running connect && check "Kafka Connect repond" \
    dc exec -T tools bash -c "wget -qO- http://connect:8083/ | grep -q version"
fi

if running kafka-cloud || running mirrormaker; then
  echo "== Replication entre clusters (profil mirror)"
  if running kafka-cloud; then
    check "kafka-cloud sain (healthcheck)" healthy kafka-cloud
    check "kafka-cloud : certificat present (security/certs/kafka-cloud.keystore.p12)" \
      test -f "$LAB_DIR/security/certs/kafka-cloud.keystore.p12"
  fi
  if running mirrormaker; then
    # Le conteneur peut rester demarre alors que MirrorMaker 2 s'est arrete
    # (erreur d'autorisation, par exemple) : on verifie que les heartbeats
    # emis dans le cluster cloud progressent.
    hb() { dc exec -T tools bash -c "kafka-get-offsets.sh --bootstrap-server kafka-cloud:19092 --topic heartbeats 2>/dev/null | awk -F: '{s += \$3} END {print s + 0}'" </dev/null; }
    hb1="$(hb)"; sleep 5; hb2="$(hb)"
    check "MirrorMaker 2 actif (heartbeats du cluster cloud : ${hb1:-0} puis ${hb2:-0})" test "${hb2:-0}" -gt "${hb1:-0}"
  fi
fi

echo
if (( ERRORS == 0 )); then
  ok "Environnement valide."
else
  fail "$ERRORS verification(s) en echec. Voir formateur/depannage.md."
fi
exit "$ERRORS"
