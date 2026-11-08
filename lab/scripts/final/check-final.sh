#!/usr/bin/env bash
# ------------------------------------------------------------------
# TP final : verification automatique de l'etat du cluster.
#
# Usage : scripts/final/check-final.sh
#
# Note la partie technique sur 13 points (voir evaluation/
# tp-final-corrige-bareme.md). Les 7 autres points (methode, preuves
# par les metriques, compte rendu) sont notes par le formateur.
# Le script ne modifie rien : il peut etre lance plusieurs fois.
# ------------------------------------------------------------------
set -uo pipefail
source "$(dirname "$0")/../lib.sh"

healthy() { [[ "$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null)" == "healthy" ]]; }

SCORE=0
MAX=0
crit() {  # crit <points> <libelle> <commande...>
  local pts="$1" label="$2"; shift 2
  MAX=$((MAX + pts))
  if "$@" >/dev/null 2>&1; then ok "[$pts/$pts] $label"; SCORE=$((SCORE + pts))
  else fail "[0/$pts] $label"; fi
}

echo "== A. Cluster (3 points)"
crit 1 "kafka-1, kafka-2 et kafka-3 sains" bash -c "$(declare -f healthy); healthy kafka-1 && healthy kafka-2 && healthy kafka-3"

report="$(kt bash -s <<'EOF' 2>/dev/null
set -u
q="$(kafka-metadata-quorum.sh --bootstrap-server $BS describe --status 2>/dev/null)"
echo "LEADER=$(awk '/^LeaderId:/ {print $2}' <<<"$q")"
echo "VOTERS=$(grep CurrentVoters <<<"$q" | grep -o '"id": [0-9]*' | wc -l)"
echo "URP=$(kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions 2>/dev/null | grep -c Partition)"
echo "UNAVAILABLE=$(kafka-topics.sh --bootstrap-server $BS --describe --unavailable-partitions 2>/dev/null | grep -c Partition)"
d="$(kafka-topics.sh --bootstrap-server $BS --describe --topic payments 2>/dev/null)"
echo "PAY_PARTITIONS=$(grep -c 'Partition: ' <<<"$d")"
echo "PAY_RF3=$(awk '/Partition: / {for (i = 1; i <= NF; i++) if ($i == "Replicas:") {n = split($(i + 1), r, ","); if (n == 3) c++}} END {print c + 0}' <<<"$d")"
echo "PAY_ISR3=$(awk '/Partition: / {for (i = 1; i <= NF; i++) if ($i == "Isr:") {n = split($(i + 1), r, ","); if (n == 3) c++}} END {print c + 0}' <<<"$d")"
echo "PAY_MINISR=$(kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name payments --all 2>/dev/null | awk -F'[= ]+' '$2 == "min.insync.replicas" {print $3}')"
echo "PAY_END=$(kafka-get-offsets.sh --bootstrap-server $BS --topic payments --time latest 2>/dev/null | awk -F: '{s += $3} END {print s + 0}')"
echo "PAY_START=$(kafka-get-offsets.sh --bootstrap-server $BS --topic payments --time earliest 2>/dev/null | awk -F: '{s += $3} END {print s + 0}')"
quotas="$(kafka-configs.sh --bootstrap-server $BS --describe --entity-type clients --entity-default 2>/dev/null; \
          kafka-configs.sh --bootstrap-server $BS --describe --entity-type clients --entity-name loadgen-consumer 2>/dev/null)"
echo "QUOTA=$(grep -c 'consumer_byte_rate' <<<"$quotas")"
echo "LAG=$(kafka-consumer-groups.sh --bootstrap-server $BS --describe --group loadgen-consumers 2>/dev/null | awk '$1 == "loadgen-consumers" && $6 ~ /^[0-9]+$/ {s += $6} END {print s + 0}')"
c="$(kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name clickstream 2>/dev/null)"
echo "CS_RMS=$(awk -F'[= ]+' '$2 == "retention.ms" {print $3}' <<<"$c")"
echo "CS_RBYTES=$(awk -F'[= ]+' '$2 == "retention.bytes" {print $3}' <<<"$c")"
EOF
)"
val() { local v; v="$(grep "^$1=" <<<"$report" | cut -d= -f2)"; echo "${v:-x}"; }
num() { [[ "$1" =~ ^-?[0-9]+$ ]]; }
between() { num "$1" && (( $1 > $2 && $1 <= $3 )); }   # between <v> <min exclu> <max inclus>

crit 1 "Quorum KRaft : leader elu et 3 votants" bash -c "[[ '$(val LEADER)' =~ ^[0-9]+$ && '$(val VOTERS)' == 3 ]]"
crit 1 "Aucune partition sous-repliquee ni indisponible (URP=$(val URP), indisponibles=$(val UNAVAILABLE))" \
  bash -c "[[ '$(val URP)' == 0 && '$(val UNAVAILABLE)' == 0 ]]"

echo "== B. Topic payments (5 points)"
crit 2 "Facteur de replication 3 sur toutes les partitions ($(val PAY_RF3)/$(val PAY_PARTITIONS))" \
  bash -c "[[ '$(val PAY_PARTITIONS)' -ge 3 && '$(val PAY_RF3)' == '$(val PAY_PARTITIONS)' ]]"
crit 1 "ISR complet (3 repliques en ISR) sur toutes les partitions ($(val PAY_ISR3)/$(val PAY_PARTITIONS))" \
  bash -c "[[ '$(val PAY_PARTITIONS)' -ge 3 && '$(val PAY_ISR3)' == '$(val PAY_PARTITIONS)' ]]"
crit 1 "min.insync.replicas effectif = 2 (valeur : $(val PAY_MINISR))" test "$(val PAY_MINISR)" = 2
crit 1 "Aucune perte : 300 paiements, premier offset 0 (fin=$(val PAY_END), debut=$(val PAY_START))" \
  bash -c "[[ '$(val PAY_END)' -ge 300 && '$(val PAY_START)' == 0 ]]"

echo "== C. Consommateurs (3 points)"
crit 2 "Quota consumer_byte_rate supprime (defaut clients et loadgen-consumer)" test "$(val QUOTA)" = 0
crit 1 "Lag du groupe loadgen-consumers resorbe (< 1000, valeur : $(val LAG))" bash -c "num() { [[ \$1 =~ ^[0-9]+$ ]]; }; num '$(val LAG)' && (( $(val LAG) < 1000 ))"

echo "== D. Retention de clickstream (2 points)"
crit 1 "retention.ms <= 24 h (valeur : $(val CS_RMS))" bash -c "$(declare -f num between); between '$(val CS_RMS)' 0 86400000"
crit 1 "retention.bytes <= 100 Mo par partition (valeur : $(val CS_RBYTES))" bash -c "$(declare -f num between); between '$(val CS_RBYTES)' 0 104857600"

echo
if [[ "$(docker inspect -f '{{.State.Running}}' prometheus 2>/dev/null)" == "true" ]]; then
  firing="$(kt bash -c "wget -qO- 'http://prometheus:9090/api/v1/query?query=ALERTS%7Balertstate%3D%22firing%22%7D'" 2>/dev/null \
    | grep -o '"alertname":"[A-Za-z]*"' | cut -d'"' -f4 | sort -u | tr '\n' ' ')"
  if [[ -n "$firing" ]]; then warn "Alertes encore actives (information, non notee) : $firing"
  else ok "Aucune alerte active (information, non notee)"; fi
fi
echo
info "Partie technique : $SCORE / $MAX"
info "Partie formateur (methode, preuves, compte rendu) : / 7 -> voir evaluation/tp-final-corrige-bareme.md"
