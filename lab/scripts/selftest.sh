#!/usr/bin/env bash
# ------------------------------------------------------------------
# Auto-test du lab : rejoue les commandes cles des TP1 a TP7 et du TP9.
#
# Usage : scripts/selftest.sh [--verbose] [tp1 tp2 ... tp7 tp9]
#         (sans argument : tous les TP, environ 10 minutes)
#
# A lancer AVANT la session, sur un lab demarre en mode extended ou full :
#     scripts/reset.sh --yes && scripts/start.sh full
#     scripts/validate.sh && scripts/selftest.sh
#     scripts/reset.sh --yes          # repartir d'un lab vierge pour les stagiaires
#
# Le script n'utilise que des objets prefixes "selftest" (topics, groupes,
# utilisateurs st-producer et st-consumer) et les supprime a la fin.
# Exception, TP9 : MirrorMaker 2 recopie les topics orders et payments
# (crees s'ils sont absents, puis supprimes) ; le cluster cloud et son
# volume sont supprimes a la fin.
# Il arrete et redemarre des noeuds (TP4, TP7) : ne pas le lancer pendant
# qu'un stagiaire travaille sur le meme lab.
# Code retour : nombre de verifications en echec.
# ------------------------------------------------------------------
set -uo pipefail
source "$(dirname "$0")/lib.sh"

VERBOSE=0
TPS=()
for arg in "$@"; do
  case "$arg" in
    --verbose|-v) VERBOSE=1 ;;
    tp[1-7]|tp9) TPS+=("$arg") ;;
    *) die "Argument inconnu : $arg (attendu : --verbose, tp1 ... tp7, tp9)" ;;
  esac
done
(( ${#TPS[@]} )) || TPS=(tp1 tp2 tp3 tp4 tp5 tp6 tp7 tp9)

ERRORS=0
T0=$SECONDS

# ------------------------------------------------------------------
# Outils
# ------------------------------------------------------------------
# Fonctions disponibles dans les scripts executes dans le conteneur tools.
PRELUDE='set -u
check() { local label="$1"; shift; if "$@" >/dev/null 2>&1; then echo "CHECK|ok|$label"; else echo "CHECK|ko|$label"; fi; }
note()  { echo "NOTE|$*"; }
group_lag() { kafka-consumer-groups.sh --bootstrap-server "$BS" --describe --group "$1" 2>/dev/null | awk -v g="$1" "\$1 == g && \$6 ~ /^[0-9]+$/ {s += \$6} END {print s + 0}"; }
end_offsets() { kafka-get-offsets.sh --bootstrap-server "$BS" --topic "$1" --time "${2:-latest}" 2>/dev/null | awk -F: "{s += \$3} END {print s + 0}"; }
urp_count() { kafka-topics.sh --bootstrap-server "$BS" --describe --under-replicated-partitions 2>/dev/null | grep -c Partition; }
prom() { wget -qO- --post-data "query=$1" http://prometheus:9090/api/v1/query 2>/dev/null; }
'

report() {
  local line
  while IFS= read -r line; do
    case "$line" in
      "CHECK|ok|"*) ok "${line#CHECK|ok|}" ;;
      "CHECK|ko|"*) fail "${line#CHECK|ko|}"; ERRORS=$((ERRORS + 1)) ;;
      "NOTE|"*)     info "${line#NOTE|}" ;;
      *)            (( VERBOSE )) && printf '        %s\n' "$line" ;;
    esac
  done <<<"$1"
}

# Execute dans tools le script lu sur l'entree standard.
in_tools() {
  local out
  out="$( { printf '%s\n' "$PRELUDE"; cat; } | kt bash -s 2>&1 )"
  report "$out"
}

hcheck() {  # verification cote hote
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then ok "$label"; else fail "$label"; ERRORS=$((ERRORS + 1)); fi
}

running() { [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == "true" ]]; }
healthy() { [[ "$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null)" == "healthy" ]]; }
wait_healthy() { wait_for "${2:-120}" healthy "$1"; }
urp_zero() { [[ "$(kt bash -c 'kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions 2>/dev/null | grep -c Partition')" == "0" ]]; }

section() { echo; info "===== $* ($(( SECONDS - T0 )) s)"; }

# ------------------------------------------------------------------
# Prerequis
# ------------------------------------------------------------------
for c in kafka-1 kafka-2 kafka-3 kafka-tools; do
  running "$c" || die "$c n'est pas demarre : scripts/start.sh extended (ou full)"
done
for n in 1 2 3 4; do
  cmp -s "$LAB_DIR/config/kafka/kafka-$n/server.properties" "$LAB_DIR/config/kafka/pristine/kafka-$n.server.properties" \
    || die "config/kafka/kafka-$n/server.properties differe de l'original : lancer scripts/reset.sh avant le selftest."
done

# ------------------------------------------------------------------
# TP1 : cluster KRaft
# ------------------------------------------------------------------
tp1() {
  section "TP1 Cluster KRaft"
  in_tools <<'EOF'
q="$(kafka-metadata-quorum.sh --bootstrap-server $BS describe --status 2>/dev/null)"
leader="$(awk '/^LeaderId:/ {print $2}' <<<"$q")"
check "kafka-metadata-quorum describe --status : leader elu (node ${leader:-?})" test -n "$leader"
check "describe --status : 3 votants" test "$(grep CurrentVoters <<<"$q" | grep -o '"id": [0-9]*' | wc -l)" -eq 3
r="$(kafka-metadata-quorum.sh --bootstrap-server $BS describe --replication 2>/dev/null)"
check "describe --replication : 1 Leader et 2 Follower" test "$(grep -c 'Leader\|Follower' <<<"$r")" -eq 3
check "kafka-broker-api-versions : 3 brokers" test "$(kafka-broker-api-versions.sh --bootstrap-server $BS 2>/dev/null | grep -c '^kafka-[0-9]*:19092')" -eq 3
f="$(kafka-features.sh --bootstrap-server $BS describe 2>/dev/null)"
check "kafka-features : metadata.version 4.3-IV0" grep -q 'Feature: metadata.version.*FinalizedVersionLevel: 4.3-IV0' <<<"$f"
check "kafka-features : kraft.version 1 (quorum dynamique)" grep -q 'Feature: kraft.version.*FinalizedVersionLevel: 1' <<<"$f"
check "Liste des topics accessible" kafka-topics.sh --bootstrap-server $BS --list
EOF
  hcheck "kafka-storage info sur kafka-1 (meta.properties)" \
    bash -c "cd '$LAB_DIR' && docker compose exec -T kafka-1 /opt/kafka/bin/kafka-storage.sh info -c /opt/lab/node/server.properties | grep -q 'cluster.id'"
  hcheck "Trace de demarrage 'Kafka Server started' dans les logs de kafka-1" \
    bash -c "cd '$LAB_DIR' && docker compose logs kafka-1 2>/dev/null | grep -q 'Kafka Server started'"
}

# ------------------------------------------------------------------
# TP2 : topics et messages
# ------------------------------------------------------------------
tp2() {
  section "TP2 Topics et messages"
  in_tools <<'EOF'
T=selftest-orders
kafka-topics.sh --bootstrap-server $BS --delete --topic $T --if-exists >/dev/null 2>&1; sleep 2
check "Creation du topic $T (3 partitions, RF 3)" \
  kafka-topics.sh --bootstrap-server $BS --create --topic $T --partitions 3 --replication-factor 3
check "Describe : 3 partitions, RF 3" bash -c "kafka-topics.sh --bootstrap-server $BS --describe --topic $T | grep -q 'PartitionCount: 3.*ReplicationFactor: 3'"
check "Pas de creation automatique (auto.create.topics.enable=false)" bash -c \
  "echo x | kafka-console-producer.sh --bootstrap-server $BS --topic selftest-absent --command-property max.block.ms=3000 2>&1 | grep -q 'not present in metadata'"
check "Ajout de partitions (3 vers 6)" kafka-topics.sh --bootstrap-server $BS --alter --topic $T --partitions 6
check "Production avec cle (orders.txt, separateur |)" \
  bash -c "kafka-console-producer.sh --bootstrap-server $BS --topic $T --reader-property parse.key=true --reader-property 'key.separator=|' < /opt/lab/data/orders.txt"
check "kafka-get-offsets : 30 messages au total" test "$(end_offsets $T)" -eq 30
out="$(kafka-console-consumer.sh --bootstrap-server $BS --topic $T --from-beginning --max-messages 30 --timeout-ms 15000 \
  --formatter-property print.key=true --formatter-property print.partition=true 2>/dev/null)"
check "Consommation des 30 messages" test "$(grep -c '^Partition:' <<<"$out")" -eq 30
# Meme cle => meme partition : nombre de couples (cle, partition) distincts == nombre de cles distinctes.
keys="$(awk '{print $2}' <<<"$out" | sort -u | wc -l)"
pairs="$(awk '{print $1, $2}' <<<"$out" | sort -u | wc -l)"
check "Meme cle, meme partition ($keys cles, $pairs couples cle/partition)" test "$keys" -eq "$pairs"
check "Configuration de topic : retention.ms=86400000" \
  kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name $T --add-config retention.ms=86400000
check "Describe --topics-with-overrides affiche retention.ms" \
  bash -c "kafka-topics.sh --bootstrap-server $BS --describe --topic $T --topics-with-overrides | grep -q retention.ms=86400000"
check "Suppression du topic" kafka-topics.sh --bootstrap-server $BS --delete --topic $T
EOF
}

# ------------------------------------------------------------------
# TP3 : consumer groups, offsets, retention, compaction
# ------------------------------------------------------------------
tp3() {
  section "TP3 Consumer groups, offsets, retention, compaction"
  in_tools <<'EOF'
T=selftest-events; G=selftest-group; R=selftest-retention; C=selftest-prices
for t in $T $R $C; do kafka-topics.sh --bootstrap-server $BS --delete --topic $t --if-exists >/dev/null 2>&1; done
kafka-consumer-groups.sh --bootstrap-server $BS --delete --group $G >/dev/null 2>&1; sleep 2
kafka-topics.sh --bootstrap-server $BS --create --topic $T --partitions 3 --replication-factor 3 >/dev/null
# Retention par le temps : 60 s, segments de 10 s. Un segment est supprime quand son message
# le plus recent a expire, segment actif compris.
check "Topic $R cree (retention.ms=60000, segment.ms=10000)" kafka-topics.sh --bootstrap-server $BS --create --topic $R \
  --partitions 1 --replication-factor 3 --config retention.ms=60000 --config segment.ms=10000
seq 1 100 | kafka-console-producer.sh --bootstrap-server $BS --topic $R 2>/dev/null
t100=$SECONDS
check "Topic compacte $C cree" kafka-topics.sh --bootstrap-server $BS --create --topic $C --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact --config segment.ms=10000 --config min.cleanable.dirty.ratio=0.01 --config delete.retention.ms=1000
kafka-console-producer.sh --bootstrap-server $BS --topic $C --reader-property parse.key=true --reader-property 'key.separator=|' \
  < /opt/lab/data/product-prices.txt 2>/dev/null
seq 1 30 | sed 's/^/evt-/' | kafka-console-producer.sh --bootstrap-server $BS --topic $T 2>/dev/null
check "Consommation en groupe ($G) de 30 messages" bash -c \
  "test \$(kafka-console-consumer.sh --bootstrap-server $BS --topic $T --group $G --from-beginning --max-messages 30 --timeout-ms 15000 2>/dev/null | wc -l) -eq 30"
check "Lag du groupe = 0 apres consommation" test "$(group_lag $G)" -eq 0
seq 31 40 | sed 's/^/evt-/' | kafka-console-producer.sh --bootstrap-server $BS --topic $T 2>/dev/null
check "Lag du groupe = 10 apres 10 nouveaux messages" test "$(group_lag $G)" -eq 10
note "Type de groupe : $(kafka-consumer-groups.sh --bootstrap-server $BS --list --type 2>/dev/null | awk -v g=$G '$1 == g {print $2}')"
check "Reset --to-earliest --dry-run" bash -c \
  "kafka-consumer-groups.sh --bootstrap-server $BS --group $G --topic $T --reset-offsets --to-earliest --dry-run | grep -q NEW-OFFSET"
check "Lag inchange apres dry-run (10)" test "$(group_lag $G)" -eq 10
check "Reset --to-earliest --execute" \
  kafka-consumer-groups.sh --bootstrap-server $BS --group $G --topic $T --reset-offsets --to-earliest --execute
check "Lag = 40 apres reset" test "$(group_lag $G)" -eq 40
check "Reset --shift-by -5 sur un groupe a jour" bash -c \
  "kafka-console-consumer.sh --bootstrap-server $BS --topic $T --group $G --max-messages 40 --timeout-ms 15000 >/dev/null 2>&1 &&
   kafka-consumer-groups.sh --bootstrap-server $BS --group $G --topic $T --reset-offsets --shift-by -5 --execute >/dev/null"
# Sans cle, le partitionneur "sticky" envoie un lot entier dans une seule partition :
# le recul de 5 ne s'applique qu'aux partitions qui contiennent des messages.
l="$(group_lag $G)"
check "Lag = $l apres shift-by -5 (5 par partition non vide)" test "$l" -ge 5 -a "$l" -le 15
# Roulement des segments : un message apres segment.ms ferme le segment courant.
# Au moins 35 s apres les 100 messages : plus que la verification de la retention (30 s),
# pour qu'une verification passe entre l'expiration des deux segments (sinon 0 -> 101 directement).
attente=$(( 35 - (SECONDS - t100) )); if (( attente > 0 )); then sleep $attente; fi
echo 101 | kafka-console-producer.sh --bootstrap-server $BS --topic $R 2>/dev/null
echo 'P-500|9.99' | kafka-console-producer.sh --bootstrap-server $BS --topic $C --reader-property parse.key=true --reader-property 'key.separator=|' 2>/dev/null
start=$SECONDS; n=99
while (( SECONDS - start < 60 )); do
  n="$(kafka-console-consumer.sh --bootstrap-server $BS --topic $C --from-beginning --timeout-ms 4000 2>/dev/null | wc -l)"
  (( n <= 5 )) && break; sleep 5
done
check "Compaction : 21 messages reduits a $n (une valeur par cle)" test "$n" -le 5
start=$SECONDS; e=0
while (( SECONDS - start < 150 )); do
  e="$(end_offsets $R earliest)"; (( e == 100 )) && break; sleep 10
done
check "Retention : premier offset disponible = $e (100 attendu, apres $(( SECONDS - start )) s d'attente)" test "$e" -eq 100
kafka-consumer-groups.sh --bootstrap-server $BS --delete --group $G >/dev/null 2>&1
for t in $T $R $C; do kafka-topics.sh --bootstrap-server $BS --delete --topic $t >/dev/null 2>&1; done
EOF
}

# ------------------------------------------------------------------
# TP4 : fiabilite et panne de broker
# ------------------------------------------------------------------
tp4() {
  section "TP4 Fiabilite et panne de broker"
  in_tools <<'EOF'
P=selftest-payments; S=selftest-strict
for t in $P $S; do kafka-topics.sh --bootstrap-server $BS --delete --topic $t --if-exists >/dev/null 2>&1; done; sleep 2
check "min.insync.replicas=2 par defaut au niveau cluster (--entity-default)" \
  kafka-configs.sh --bootstrap-server $BS --alter --entity-type brokers --entity-default --add-config min.insync.replicas=2
check "Refus de min.insync.replicas au niveau d'un broker (ELR active)" bash -c \
  "kafka-configs.sh --bootstrap-server $BS --alter --entity-type brokers --entity-name 1 --add-config min.insync.replicas=2 2>&1 | grep -q 'ELR is enabled'"
kafka-topics.sh --bootstrap-server $BS --create --topic $P --partitions 3 --replication-factor 3 >/dev/null
kafka-topics.sh --bootstrap-server $BS --create --topic $S --partitions 1 --replication-factor 3 --config min.insync.replicas=3 >/dev/null
check "Production acks=all, 3 brokers" bash -c "! seq 1 10 | kafka-console-producer.sh --bootstrap-server $BS --topic $P --request-required-acks all 2>&1 | grep -q ErrorLoggingCallback"
EOF
  info "Arret controle de kafka-3"
  dc stop kafka-3 >/dev/null 2>&1
  in_tools <<'EOF'
P=selftest-payments; S=selftest-strict
start=$SECONDS; until (( $(urp_count) > 0 )) || (( SECONDS - start > 60 )); do sleep 2; done
check "Partitions sous-repliquees apres l'arret ($(urp_count) URP)" test "$(urp_count)" -gt 0
check "Leaders sans le broker 3" bash -c "! kafka-topics.sh --bootstrap-server $BS --describe --topic $P | grep -q 'Leader: 3'"
check "acks=all accepte avec ISR=2 et min ISR=2" bash -c \
  "! seq 11 20 | kafka-console-producer.sh --bootstrap-server $BS --topic $P --request-required-acks all \
     --command-property delivery.timeout.ms=15000 --command-property request.timeout.ms=5000 2>&1 | grep -q ErrorLoggingCallback"
out="$(echo strict | kafka-console-producer.sh --bootstrap-server $BS --topic $S --request-required-acks all \
  --command-property delivery.timeout.ms=10000 --command-property request.timeout.ms=5000 2>&1)"
check "acks=all refuse si ISR < min ISR (NOT_ENOUGH_REPLICAS)" grep -q 'NOT_ENOUGH_REPLICAS\|NotEnoughReplicas' <<<"$out"
check "acks=1 accepte sur le meme topic" bash -c \
  "! echo strict | kafka-console-producer.sh --bootstrap-server $BS --topic $S --request-required-acks 1 \
     --command-property delivery.timeout.ms=10000 --command-property request.timeout.ms=5000 2>&1 | grep -q ErrorLoggingCallback"
note "$(kafka-topics.sh --bootstrap-server $BS --describe --topic $S | tail -1 | tr '\t' ' ')"
check "Quorum KRaft toujours disponible (2 votants sur 3)" bash -c \
  "kafka-metadata-quorum.sh --bootstrap-server $BS describe --status | grep -q '^LeaderId'"
EOF
  info "Redemarrage de kafka-3"
  dc start kafka-3 >/dev/null 2>&1
  hcheck "kafka-3 de nouveau sain" wait_healthy kafka-3
  hcheck "URP revenu a 0 (rattrapage)" wait_for 90 urp_zero
  in_tools <<'EOF'
P=selftest-payments; S=selftest-strict
check "Election des leaders preferes" bash -c \
  "kafka-leader-election.sh --bootstrap-server $BS --election-type preferred --all-topic-partitions 2>&1 | grep -q 'Successfully completed\|Valid replica already elected\|no election'"
check "Lecture des 20 messages acks=all" test "$(end_offsets $P)" -eq 20
for t in $P $S; do kafka-topics.sh --bootstrap-server $BS --delete --topic $t >/dev/null 2>&1; done
EOF
}

# ------------------------------------------------------------------
# TP5 : securite (SCRAM, SASL_SSL, ACL, quotas)
# ------------------------------------------------------------------
tp5() {
  section "TP5 Securite"
  in_tools <<'EOF'
T=selftest-secure; G=selftest-billing
had_admin="$(kafka-configs.sh --bootstrap-server $BS --describe --entity-type users --entity-name admin 2>/dev/null | grep -c SCRAM)"
echo "$had_admin" > /tmp/selftest-had-admin
kafka-topics.sh --bootstrap-server $BS --delete --topic $T --if-exists >/dev/null 2>&1; sleep 2
kafka-topics.sh --bootstrap-server $BS --create --topic $T --partitions 3 --replication-factor 3 >/dev/null
for u in admin:admin-secret st-producer:st-producer-secret st-consumer:st-consumer-secret; do
  check "Utilisateur SCRAM-SHA-512 ${u%%:*}" kafka-configs.sh --bootstrap-server $BS --alter --entity-type users \
    --entity-name "${u%%:*}" --add-config "SCRAM-SHA-512=[iterations=8192,password=${u#*:}]"
done
# Fichiers clients generes a partir du modele des stagiaires.
for u in st-producer st-consumer; do
  sed -e 's/<protocole du listener SECURE>/SASL_SSL/' -e 's/<mecanisme SASL active sur le listener>/SCRAM-SHA-512/' \
      -e "s/<utilisateur>/$u/" -e "s/<mot de passe>/$u-secret/" -e "s/<identifiant de l'application>/$u/" \
      /opt/lab/clients/secure-template.properties > /tmp/$u.properties
done
check "Connexion admin en SASL_SSL (admin.properties)" \
  kafka-topics.sh --bootstrap-server $BS_SECURE --command-config /opt/lab/clients/admin.properties --list
out="$(kafka-console-producer.sh --bootstrap-server $BS_SECURE --command-config /tmp/st-producer.properties --topic $T \
  --command-property max.block.ms=10000 <<<'refuse' 2>&1)"
check "Production refusee sans ACL (TopicAuthorization)" grep -q 'TopicAuthorization\|Not authorized' <<<"$out"
check "ACL Write sur le topic pour st-producer" kafka-acls.sh --bootstrap-server $BS --add \
  --allow-principal User:st-producer --operation Write --operation Describe --topic $T
check "Production autorisee avec ACL" bash -c "! seq 1 20 | kafka-console-producer.sh --bootstrap-server $BS_SECURE \
  --command-config /tmp/st-producer.properties --topic $T 2>&1 | grep -q ErrorLoggingCallback"
check "ACL Read sur le topic et le groupe pour st-consumer" kafka-acls.sh --bootstrap-server $BS --add \
  --allow-principal User:st-consumer --operation Read --operation Describe --topic $T --group $G
check "Consommation autorisee (20 messages)" bash -c "test \$(kafka-console-consumer.sh --bootstrap-server $BS_SECURE \
  --command-config /tmp/st-consumer.properties --topic $T --group $G --from-beginning --max-messages 20 --timeout-ms 20000 2>/dev/null | wc -l) -eq 20"
out="$(kafka-console-consumer.sh --bootstrap-server $BS_SECURE --command-config /tmp/st-consumer.properties \
  --topic $T --group other-group --from-beginning --max-messages 1 --timeout-ms 10000 2>&1)"
check "Consommation refusee avec un groupe non autorise (GroupAuthorization)" grep -q 'GroupAuthorization\|Not authorized to access group' <<<"$out"
check "ACL listees" bash -c "kafka-acls.sh --bootstrap-server $BS --list --topic $T | grep -q st-producer"
check "Quota producer_byte_rate=102400 pour st-producer" kafka-configs.sh --bootstrap-server $BS --alter \
  --entity-type users --entity-name st-producer --add-config producer_byte_rate=102400
out="$(kafka-producer-perf-test.sh --bootstrap-server $BS_SECURE --topic $T --num-records 8000 --record-size 1000 \
  --throughput -1 --command-config /tmp/st-producer.properties --print-metrics 2>/dev/null)"
throttle="$(grep -o 'producer-metrics:produce-throttle-time-avg:{client-id=st-producer}[^0-9]*[0-9.]*' <<<"$out" | grep -o '[0-9.]*$')"
note "Debit bride : $(grep -o '[0-9.]* MB/sec' <<<"$out" | tail -1), throttle moyen ${throttle:-?} ms"
check "Throttling observe (produce-throttle-time-avg > 0)" awk -v t="${throttle:-0}" 'BEGIN {exit !(t > 0)}'
EOF
  hcheck "Refus journalise dans kafka-authorizer.log" bash -c \
    "cd '$LAB_DIR' && docker compose exec -T kafka-1 grep -q 'User:st-producer.*Denied' /opt/kafka/logs/kafka-authorizer.log \
     || docker compose exec -T kafka-2 grep -q 'User:st-producer.*Denied' /opt/kafka/logs/kafka-authorizer.log \
     || docker compose exec -T kafka-3 grep -q 'User:st-producer.*Denied' /opt/kafka/logs/kafka-authorizer.log"
  in_tools <<'EOF'
T=selftest-secure; G=selftest-billing
kafka-acls.sh --bootstrap-server $BS --remove --force --topic $T >/dev/null 2>&1
kafka-acls.sh --bootstrap-server $BS --remove --force --group $G >/dev/null 2>&1
kafka-configs.sh --bootstrap-server $BS --alter --entity-type users --entity-name st-producer --delete-config producer_byte_rate >/dev/null 2>&1
for u in st-producer st-consumer; do
  kafka-configs.sh --bootstrap-server $BS --alter --entity-type users --entity-name $u --delete-config SCRAM-SHA-512 >/dev/null 2>&1
done
[[ "$(cat /tmp/selftest-had-admin)" == "0" ]] && kafka-configs.sh --bootstrap-server $BS --alter --entity-type users \
  --entity-name admin --delete-config SCRAM-SHA-512 >/dev/null 2>&1
kafka-consumer-groups.sh --bootstrap-server $BS --delete --group $G >/dev/null 2>&1
kafka-topics.sh --bootstrap-server $BS --delete --topic $T >/dev/null 2>&1
check "Nettoyage : plus aucun utilisateur st-*" bash -c "! kafka-configs.sh --bootstrap-server $BS --describe --entity-type users | grep -q 'st-'"
EOF
}

# ------------------------------------------------------------------
# TP6 : traces et metriques (necessite le mode extended)
# ------------------------------------------------------------------
tp6() {
  section "TP6 Traces et metriques"
  in_tools <<'EOF'
L=org.apache.kafka.controller
check "Logger $L passe en DEBUG a chaud sur le noeud 1" kafka-configs.sh --bootstrap-server $BS --alter \
  --entity-type broker-loggers --entity-name 1 --add-config $L=DEBUG
out="$(kafka-configs.sh --bootstrap-server $BS --describe --entity-type broker-loggers --entity-name 1 2>/dev/null)"
check "Describe broker-loggers : $L=DEBUG" grep -q "^  $L=DEBUG" <<<"$out"
check "Retour au niveau par defaut (--delete-config)" kafka-configs.sh --bootstrap-server $BS --alter \
  --entity-type broker-loggers --entity-name 1 --delete-config $L
out="$(wget -qO- http://kafka-1:9404/metrics 2>/dev/null)"
check "Endpoint /metrics de l'agent JMX (kafka-1:9404)" grep -q '^kafka_server_replicamanager_underreplicatedpartitions' <<<"$out"
EOF
  if ! running prometheus; then
    warn "Prometheus non demarre : verifications Prometheus et lag ignorees (mode extended requis)."
    return
  fi
  in_tools <<'EOF'
check "PromQL : 1 controleur actif" grep -q '"1"\]' <<<"$(prom 'sum(kafka_controller_kafkacontroller_activecontrollercount)')"
check "PromQL : 3 series URP" test "$(prom 'kafka_server_replicamanager_underreplicatedpartitions' | grep -o '"instance"' | wc -l)" -eq 3
rate="$(prom 'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{topic="loadgen"}[1m]))' | grep -o '"[0-9.]*"\]' | tr -d '"]')"
check "PromQL : debit entrant du topic loadgen = ${rate:-?} msg/s" awk -v r="${rate:-0}" 'BEGIN {exit !(r > 0)}'
check "PromQL : lag du groupe loadgen-consumers (kafka-exporter)" grep -q '"value"' <<<"$(prom 'sum(kafka_consumergroup_lag{consumergroup="loadgen-consumers"})')"
check "PromQL : metriques client du loadgen-producer" grep -q loadgen-producer <<<"$(prom 'kafka_producer_producer_metrics_record_send_rate')"
check "Regles d'alerte chargees (9)" test "$(wget -qO- http://prometheus:9090/api/v1/rules | grep -o '"type":"alerting"' | wc -l)" -eq 9
EOF
}

# ------------------------------------------------------------------
# TP7 : exploitation (kafka-4, reassignation, log dir, rolling restart)
# ------------------------------------------------------------------
tp7() {
  section "TP7 Exploitation"
  in_tools <<'EOF'
T=selftest-reassign
kafka-topics.sh --bootstrap-server $BS --delete --topic $T --if-exists >/dev/null 2>&1; sleep 2
kafka-topics.sh --bootstrap-server $BS --create --topic $T --partitions 4 --replication-factor 3 >/dev/null
kafka-producer-perf-test.sh --bootstrap-server $BS --topic $T --num-records 20000 --record-size 500 --throughput -1 >/dev/null 2>&1
check "Topic $T alimente (20000 messages)" test "$(end_offsets $T)" -eq 20000
EOF
  info "Demarrage de kafka-4 (profil scale)"
  dc --profile scale up -d --wait --wait-timeout 120 kafka-4 >/dev/null 2>&1
  hcheck "kafka-4 sain" healthy kafka-4
  in_tools <<'EOF'
T=selftest-reassign
start=$SECONDS
until (( $(kafka-broker-api-versions.sh --bootstrap-server $BS 2>/dev/null | grep -c '^kafka-[0-9]*:19092') == 4 )) || (( SECONDS - start > 60 )); do sleep 2; done
check "4 brokers enregistres" test "$(kafka-broker-api-versions.sh --bootstrap-server $BS 2>/dev/null | grep -c '^kafka-[0-9]*:19092')" -eq 4
check "kafka-4 observateur du quorum (pas votant)" bash -c \
  "kafka-metadata-quorum.sh --bootstrap-server $BS describe --replication | grep -q '^4 .*Observer'"
echo "{\"version\":1,\"topics\":[{\"topic\":\"$T\"}]}" > /tmp/st-topics.json
kafka-reassign-partitions.sh --bootstrap-server $BS --topics-to-move-json-file /tmp/st-topics.json \
  --broker-list 1,2,3,4 --generate 2>/dev/null | sed -n '/Proposed partition reassignment/{n;p;}' > /tmp/st-plan.json
check "Plan genere (--generate) incluant le broker 4" grep -q '[[,]4[],]' /tmp/st-plan.json
check "Reassignation lancee avec throttle (--execute --throttle)" kafka-reassign-partitions.sh --bootstrap-server $BS \
  --reassignment-json-file /tmp/st-plan.json --execute --throttle 50000000
start=$SECONDS
until kafka-reassign-partitions.sh --bootstrap-server $BS --reassignment-json-file /tmp/st-plan.json --verify 2>/dev/null \
  | grep -q 'Clearing topic-level throttles' || (( SECONDS - start > 120 )); do sleep 3; done
check "Reassignation terminee (--verify) et throttles retires" test $(( SECONDS - start )) -le 120
check "Le broker 4 heberge des repliques" bash -c "kafka-topics.sh --bootstrap-server $BS --describe --topic $T | grep -q 'Replicas: .*4'"
EOF
  # Rolling restart avec un producteur acks=all en continu ; controleur actif en dernier.
  info "Rolling restart de kafka-1..3 sous charge (acks=all)"
  dc exec -d tools bash -c 'kafka-producer-perf-test.sh --bootstrap-server $BS --topic selftest-reassign \
    --num-records 30000 --record-size 200 --throughput 300 \
    --command-property acks=all --command-property enable.idempotence=true > /tmp/st-rolling.log 2>&1'
  local leader order=() n
  leader="$(kt bash -c 'kafka-metadata-quorum.sh --bootstrap-server $BS describe --status 2>/dev/null | awk "/^LeaderId:/ {print \$2}"')"
  for n in 1 2 3; do [[ "$n" != "$leader" ]] && order+=("$n"); done
  order+=("$leader")
  info "Ordre : ${order[*]} (controleur actif $leader en dernier)"
  local rr_start=$SECONDS
  for n in "${order[@]}"; do
    dc restart "kafka-$n" >/dev/null 2>&1
    hcheck "kafka-$n redemarre et sain" wait_healthy "kafka-$n"
    hcheck "URP = 0 avant le noeud suivant" wait_for 120 urp_zero
  done
  info "Rolling restart termine en $(( SECONDS - rr_start )) s"
  in_tools <<'EOF'
start=$SECONDS
until grep -q '30000 records sent' /tmp/st-rolling.log || (( SECONDS - start > 150 )); do sleep 5; done
check "Producteur acks=all : 30000 envois confirmes pendant le rolling restart" grep -q '^30000 records sent' /tmp/st-rolling.log
check "Aucun envoi en echec (pas d'exception remontee au callback)" bash -c "! grep -v WARN /tmp/st-rolling.log | grep -q 'Exception'"
note "Avertissements retriables : $(grep -c WARN /tmp/st-rolling.log)"
EOF
  # Ajout d'un log dir sur kafka-1, deplacement d'une replique, retour arriere.
  info "Ajout du log dir /var/lib/kafka/data2 sur kafka-1"
  local k1="$LAB_DIR/config/kafka/kafka-1/server.properties"
  sed -i.selftest 's|^log.dirs=/var/lib/kafka/data$|log.dirs=/var/lib/kafka/data,/var/lib/kafka/data2|' "$k1" && rm -f "$k1.selftest"
  dc restart kafka-1 >/dev/null 2>&1
  hcheck "kafka-1 sain avec 2 log dirs" wait_healthy kafka-1
  hcheck "URP = 0" wait_for 120 urp_zero
  in_tools <<'EOF'
T=selftest-reassign
check "kafka-log-dirs : 2 repertoires sur le broker 1" test "$(kafka-log-dirs.sh --bootstrap-server $BS --describe --broker-list 1 2>/dev/null | tail -1 | grep -o '"logDir"' | wc -l)" -eq 2
# Premiere partition dont le broker 1 heberge une replique : deplacement vers data2.
line="$(kafka-topics.sh --bootstrap-server $BS --describe --topic $T | awk '$7 == "Replicas:" && ("," $8 ",") ~ /,1,/ {print $4, $8; exit}')"
p="${line%% *}"; reps="${line#* }"
if [[ -n "$line" ]]; then
  dirs="$(awk -F, '{for (i = 1; i <= NF; i++) printf "%s\"%s\"", (i > 1 ? "," : ""), ($i == 1 ? "/var/lib/kafka/data2" : "any")}' <<<"$reps")"
  echo "{\"version\":1,\"partitions\":[{\"topic\":\"$T\",\"partition\":$p,\"replicas\":[$reps],\"log_dirs\":[$dirs]}]}" > /tmp/st-move.json
  kafka-reassign-partitions.sh --bootstrap-server $BS --reassignment-json-file /tmp/st-move.json --execute >/dev/null 2>&1
  start=$SECONDS
  until kafka-log-dirs.sh --bootstrap-server $BS --describe --broker-list 1 --topic-list $T 2>/dev/null | tail -1 \
    | grep -q "\"partition\":\"$T-$p\",\"size\":[0-9]*,\"offsetLag\":0,\"isFuture\":false}[^]]*\],\"error\":null,\"logDir\":\"/var/lib/kafka/data2\"" \
    || (( SECONDS - start > 60 )); do sleep 3; done
  check "Replique $T-$p du broker 1 deplacee vers data2" test $(( SECONDS - start )) -le 60
  sed 's#/var/lib/kafka/data2#/var/lib/kafka/data#' /tmp/st-move.json > /tmp/st-move-back.json
  kafka-reassign-partitions.sh --bootstrap-server $BS --reassignment-json-file /tmp/st-move-back.json --execute >/dev/null 2>&1
  start=$SECONDS
  until kafka-reassign-partitions.sh --bootstrap-server $BS --reassignment-json-file /tmp/st-move-back.json --verify 2>/dev/null \
    | grep -q "replica $T-$p-1 completed" || (( SECONDS - start > 60 )); do sleep 3; done
  check "Replique ramenee dans data" test $(( SECONDS - start )) -le 60
else
  echo "CHECK|ko|Aucune replique de $T sur le broker 1 : deplacement entre log dirs impossible"
fi
EOF
  cp "$LAB_DIR/config/kafka/pristine/kafka-1.server.properties" "$k1"
  dc restart kafka-1 >/dev/null 2>&1
  hcheck "kafka-1 revenu a 1 log dir et sain" wait_healthy kafka-1
  hcheck "URP = 0" wait_for 120 urp_zero
  # Decommissionnement de kafka-4.
  in_tools <<'EOF'
T=selftest-reassign
kafka-reassign-partitions.sh --bootstrap-server $BS --topics-to-move-json-file /tmp/st-topics.json \
  --broker-list 1,2,3 --generate 2>/dev/null | sed -n '/Proposed partition reassignment/{n;p;}' > /tmp/st-plan-back.json
kafka-reassign-partitions.sh --bootstrap-server $BS --reassignment-json-file /tmp/st-plan-back.json --execute >/dev/null 2>&1
start=$SECONDS
until kafka-reassign-partitions.sh --bootstrap-server $BS --reassignment-json-file /tmp/st-plan-back.json --verify 2>/dev/null \
  | grep -q 'Clearing topic-level throttles' || (( SECONDS - start > 120 )); do sleep 3; done
check "Plus aucune replique sur le broker 4" bash -c \
  "test \$(kafka-log-dirs.sh --bootstrap-server $BS --describe --broker-list 4 2>/dev/null | tail -1 | grep -o '\"partition\":' | wc -l) -eq 0"
EOF
  dc --profile scale stop kafka-4 >/dev/null 2>&1
  in_tools <<'EOF'
check "kafka-cluster unregister --id 4" kafka-cluster.sh unregister --bootstrap-server $BS --id 4
start=$SECONDS
until (( $(kafka-broker-api-versions.sh --bootstrap-server $BS 2>/dev/null | grep -c '^kafka-[0-9]*:19092') == 3 )) || (( SECONDS - start > 30 )); do sleep 2; done
check "3 brokers enregistres apres decommissionnement" test "$(kafka-broker-api-versions.sh --bootstrap-server $BS 2>/dev/null | grep -c '^kafka-[0-9]*:19092')" -eq 3
kafka-topics.sh --bootstrap-server $BS --delete --topic selftest-reassign >/dev/null 2>&1
EOF
}

# ------------------------------------------------------------------
# TP9 : MirrorMaker 2 (cluster cloud, ACL, replication, bascule d'un groupe)
# ------------------------------------------------------------------
tp9() {
  section "TP9 MirrorMaker 2"
  hcheck "Certificat kafka-cloud present" test -f "$LAB_DIR/security/certs/kafka-cloud.keystore.p12"
  dc --profile mirror up -d --wait --wait-timeout 120 kafka-cloud >/dev/null 2>&1
  hcheck "kafka-cloud sain" healthy kafka-cloud
  in_tools <<'EOF2'
C=kafka-cloud:19092
rm -f /tmp/st-mm2-created
for t in orders payments; do
  if ! kafka-topics.sh --bootstrap-server $BS --list 2>/dev/null | grep -qx $t; then
    kafka-topics.sh --bootstrap-server $BS --create --topic $t --partitions 3 --replication-factor 3 >/dev/null
    echo $t >> /tmp/st-mm2-created
  fi
done
kafka-producer-perf-test.sh --bootstrap-server $BS --topic orders --num-records 1000 --record-size 200 --throughput -1 >/dev/null 2>&1
kafka-acls.sh --bootstrap-server $C --add --allow-principal User:mm2 --operation All --topic onprem. --resource-pattern-type prefixed >/dev/null 2>&1
kafka-acls.sh --bootstrap-server $C --add --allow-principal User:mm2 --operation All --topic mm2- --resource-pattern-type prefixed >/dev/null 2>&1
kafka-acls.sh --bootstrap-server $C --add --allow-principal User:mm2 --operation All --topic heartbeats >/dev/null 2>&1
kafka-acls.sh --bootstrap-server $C --add --allow-principal User:mm2 --operation All --group '*' >/dev/null 2>&1
check "ACL du compte mm2 sur le cluster cloud (4 regles)" test "$(kafka-acls.sh --bootstrap-server $C --list --principal User:mm2 2>/dev/null | grep -c 'principal=User:mm2')" -eq 4
EOF2
  dc --profile mirror up -d mirrormaker >/dev/null 2>&1
  in_tools <<'EOF2'
C=kafka-cloud:19092
start=$SECONDS
until (( $(end_offsets orders) > 0 )) && kafka-get-offsets.sh --bootstrap-server $C --topic onprem.orders >/dev/null 2>&1 \
  && (( $(BS=$C end_offsets onprem.orders) == $(end_offsets orders) )) || (( SECONDS - start > 120 )); do sleep 3; done
check "orders recopie dans onprem.orders ($(BS=$C end_offsets onprem.orders)/$(end_offsets orders))" test "$(BS=$C end_offsets onprem.orders)" -eq "$(end_offsets orders)"
note "Replication initiale en $(( SECONDS - start )) s"
check "Rien d'ecrit sur la source (ni heartbeats ni topics mm2-*)" bash -c "! kafka-topics.sh --bootstrap-server $BS --list | grep -Eq '^(heartbeats|mm2-)'"
# Bascule d'un groupe : 300 messages lus sur la source, reprise sur le cloud.
kafka-console-consumer.sh --bootstrap-server $BS --topic orders --group selftest-mm2 --from-beginning \
  --max-messages 300 --timeout-ms 20000 >/dev/null 2>&1
total=$(end_offsets orders)
start=$SECONDS
until kafka-consumer-groups.sh --bootstrap-server $C --describe --group selftest-mm2 2>/dev/null | grep -q onprem.orders \
  || (( SECONDS - start > 90 )); do sleep 3; done
check "Offsets du groupe selftest-mm2 traduits sur le cloud" bash -c "kafka-consumer-groups.sh --bootstrap-server $C --describe --group selftest-mm2 2>/dev/null | grep -q onprem.orders"
reread=$(kafka-console-consumer.sh --bootstrap-server $C --topic onprem.orders --group selftest-mm2 --timeout-ms 10000 2>/dev/null | wc -l | tr -d ' ')
check "Reprise sur le cloud sans perte ($reread lus, au moins $(( total - 300 )) attendus)" test "$reread" -ge $(( total - 300 ))
note "Messages relus en double apres bascule : $(( reread - (total - 300) ))"
# Recopie d'une configuration de topic.
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name orders --add-config retention.ms=86400000 >/dev/null
start=$SECONDS
until kafka-configs.sh --bootstrap-server $C --describe --entity-type topics --entity-name onprem.orders 2>/dev/null | grep -q 'retention.ms=86400000' \
  || (( SECONDS - start > 60 )); do sleep 3; done
check "retention.ms recopie sur onprem.orders" bash -c "kafka-configs.sh --bootstrap-server $C --describe --entity-type topics --entity-name onprem.orders | grep -q 'retention.ms=86400000'"
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name orders --delete-config retention.ms >/dev/null
EOF2
  # Coupure de MirrorMaker 2 : le retard s'accumule, puis se resorbe.
  dc --profile mirror stop mirrormaker >/dev/null 2>&1
  in_tools <<'EOF2'
C=kafka-cloud:19092
kafka-producer-perf-test.sh --bootstrap-server $BS --topic orders --num-records 2000 --record-size 200 --throughput -1 >/dev/null 2>&1
check "MM2 arrete : retard de replication ($(( $(end_offsets orders) - $(BS=$C end_offsets onprem.orders) )) messages)" \
  test "$(( $(end_offsets orders) - $(BS=$C end_offsets onprem.orders) ))" -eq 2000
EOF2
  dc --profile mirror start mirrormaker >/dev/null 2>&1
  in_tools <<'EOF2'
C=kafka-cloud:19092
start=$SECONDS
until (( $(BS=$C end_offsets onprem.orders) == $(end_offsets orders) )) || (( SECONDS - start > 120 )); do sleep 2; done
check "Retard resorbe apres redemarrage de MM2" test "$(BS=$C end_offsets onprem.orders)" -eq "$(end_offsets orders)"
note "Rattrapage en $(( SECONDS - start )) s (demarrage de la JVM compris)"
kafka-consumer-groups.sh --bootstrap-server $BS --delete --group selftest-mm2 >/dev/null 2>&1
for t in $(cat /tmp/st-mm2-created 2>/dev/null); do kafka-topics.sh --bootstrap-server $BS --delete --topic $t >/dev/null 2>&1; done
EOF2
  info "Suppression du cluster cloud et de MirrorMaker 2"
  dc --profile mirror rm -sfv kafka-cloud mirrormaker >/dev/null 2>&1
  docker volume rm kafka-lab_kafka-cloud-data >/dev/null 2>&1
  hcheck "Cluster cloud supprime" bash -c "! docker volume inspect kafka-lab_kafka-cloud-data"
}

for tp in "${TPS[@]}"; do "$tp"; done

echo
if (( ERRORS == 0 )); then
  ok "Selftest reussi (${TPS[*]}) en $(( SECONDS - T0 )) s."
else
  fail "$ERRORS verification(s) en echec en $(( SECONDS - T0 )) s. Relancer avec --verbose pour le detail."
fi
exit "$ERRORS"
