#!/usr/bin/env bash
# ------------------------------------------------------------------
# TP final : installe un etat degrade du cluster NovaShop.
#
# A lancer par le FORMATEUR sur chaque poste, juste avant le TP final,
# sur un lab vierge en mode extended :
#     scripts/reset.sh --yes && scripts/start.sh extended
#     scripts/final/prepare-final.sh
#
# Ne pas communiquer le contenu de ce script aux stagiaires avant la fin
# du TP : la liste des anomalies injectees est la solution.
# Verification : scripts/final/check-final.sh
# ------------------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/../lib.sh"

running() { [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == "true" ]]; }

for c in kafka-1 kafka-2 kafka-3 kafka-tools prometheus grafana kafka-exporter loadgen-producer loadgen-consumer; do
  running "$c" || die "$c n'est pas demarre : scripts/start.sh extended"
done
if kt bash -c 'kafka-topics.sh --bootstrap-server $BS --list' 2>/dev/null | grep -qx 'payments'; then
  die "Le topic payments existe deja : lancer scripts/reset.sh --yes puis scripts/start.sh extended."
fi

info "Mise en place du scenario (environ 30 s)"
kt bash -s <<'EOF' >/dev/null 2>&1
# Topic critique cree "en urgence" : une seule replique par partition,
# une partition sur chaque broker.
kafka-topics.sh --bootstrap-server $BS --create --topic payments --replica-assignment 1,2,3
# 300 paiements, cles = identifiants clients.
seq 1 300 | awk '{printf "C%03d|{\"payment_id\":\"PAY-%05d\",\"customer\":\"C%03d\",\"amount\":%d.%02d}\n", $1 % 40, $1, $1 % 40, ($1 * 37) % 500, $1 % 100}' \
  | kafka-console-producer.sh --bootstrap-server $BS --topic payments --request-required-acks all \
      --reader-property parse.key=true --reader-property 'key.separator=|'
# Topic de navigation sans limite de retention.
kafka-topics.sh --bootstrap-server $BS --create --topic clickstream --partitions 3 --replication-factor 3 \
  --config retention.ms=-1 --config retention.bytes=-1
# Quota par defaut applique a TOUS les clients : 2 Ko/s par broker en lecture.
kafka-configs.sh --bootstrap-server $BS --alter --entity-type clients --entity-default \
  --add-config consumer_byte_rate=2048
EOF
# Collecteur de navigation en continu (20 messages/s de 500 octets).
dc exec -d tools bash -c 'kafka-producer-perf-test.sh --bootstrap-server $BS --topic clickstream \
  --num-records 2000000 --throughput 20 --record-size 500 \
  --command-property client.id=clickstream-collector > /tmp/final-clickstream.log 2>&1'
# Panne d'un noeud.
dc stop kafka-3 >/dev/null 2>&1

ok "Scenario du TP final en place. Les stagiaires peuvent commencer (55 min)."
echo "    Verification en fin de TP : scripts/final/check-final.sh"
