#!/usr/bin/env bash
# ------------------------------------------------------------------
# Injection et reparation des incidents du TP8 (et du runbook formateur).
#
# Usage : scripts/incident.sh list
#         scripts/incident.sh <numero> inject|status|repair
#
# Les incidents 2, 3 et 6 supposent le mode extended (charge + monitoring).
# Tous sont sans danger dans le lab et entierement reversibles.
# NE PAS lire ce script avant d'avoir diagnostique l'incident.
# ------------------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/lib.sh"

usage() {
  echo "Usage : $0 list | <1-6> inject|status|repair"
  exit 1
}

list() {
  cat <<'EOF'
 1  Un broker devient indisponible
 2  Un consumer group accumule du retard
 3  Des partitions deviennent sous-repliquees
 4  Un producteur n'arrive pas a se connecter
 5  Un probleme de listeners empeche l'acces au cluster
 6  La consommation du stockage s'emballe
EOF
}

require_running() {
  for c in "$@"; do
    [[ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" == "true" ]] \
      || die "Le conteneur $c doit etre demarre (mode extended : scripts/start.sh extended)."
  done
}

wait_healthy() {
  local c="$1" start=$SECONDS
  until [[ "$(docker inspect -f '{{.State.Health.Status}}' "$c" 2>/dev/null)" == "healthy" ]]; do
    (( SECONDS - start > 120 )) && die "$c n'est pas redevenu sain en 2 min."
    sleep 2
  done
}

urp() { kt bash -c 'kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions 2>/dev/null | grep -c Partition' || true; }

# ------------------------------------------------------------------
# 1. Broker indisponible : arret brutal (SIGKILL) de kafka-2.
# ------------------------------------------------------------------
i1_inject() { dc kill kafka-2 >/dev/null 2>&1; ok "Incident 1 injecte."; }
i1_status() { docker ps -a --filter name=kafka- --format '{{.Names}}  {{.Status}}'; echo "URP : $(urp)"; }
i1_repair() { dc start kafka-2 >/dev/null 2>&1; wait_healthy kafka-2; ok "kafka-2 redemarre. URP : $(urp)"; }

# ------------------------------------------------------------------
# 2. Lag : quota de consommation tres bas sur le client loadgen-consumer.
#    Le consommateur tourne toujours, mais il est bride par les brokers.
# ------------------------------------------------------------------
i2_inject() {
  require_running loadgen-producer loadgen-consumer
  kt bash -c 'kafka-configs.sh --bootstrap-server $BS --alter --entity-type clients \
    --entity-name loadgen-consumer --add-config consumer_byte_rate=1024' >/dev/null
  ok "Incident 2 injecte."
}
i2_status() {
  kt bash -c 'kafka-consumer-groups.sh --bootstrap-server $BS --describe --group loadgen-consumers 2>/dev/null \
    | awk "NF && !/^Consumer/ {printf \"%-10s %-10s %-15s %-15s %s\n\", \$2, \$3, \$4, \$5, \$6}"
    kafka-configs.sh --bootstrap-server $BS --describe --entity-type clients --entity-name loadgen-consumer'
}
i2_repair() {
  kt bash -c 'kafka-configs.sh --bootstrap-server $BS --alter --entity-type clients \
    --entity-name loadgen-consumer --delete-config consumer_byte_rate' >/dev/null
  ok "Quota supprime : le lag doit se resorber en 1 a 2 minutes."
}

# ------------------------------------------------------------------
# 3. Partition reseau : kafka-3 est coupe du reseau Docker. Son conteneur
#    reste "healthy" (le healthcheck est local) mais le cluster le perd :
#    ses repliques sortent des ISR, le controleur l'isole (fenced).
# ------------------------------------------------------------------
i3_inject() {
  docker network disconnect kafka-lab kafka-3
  ok "Incident 3 injecte (effet visible apres 10 a 30 s)."
}
i3_status() {
  docker ps --filter name=kafka-3 --format '{{.Names}}  {{.Status}}  reseaux={{.Networks}}'
  echo "URP : $(urp)"
}
i3_repair() {
  docker network connect kafka-lab kafka-3 2>/dev/null || warn "kafka-3 deja connecte au reseau kafka-lab."
  ok "kafka-3 reconnecte : il rejoint le quorum et rattrape ses repliques (URP -> 0 sous 30 s)."
}

# ------------------------------------------------------------------
# 4. Producteur "shipping-service" mal configure (protocole et mecanisme).
#    Fichiers deposes dans le conteneur tools : /tmp/incident4/
# ------------------------------------------------------------------
i4_inject() {
  kt bash -s <<'EOF' >/dev/null 2>&1
kafka-topics.sh --bootstrap-server $BS --create --if-not-exists --topic shipments --partitions 3 --replication-factor 3
kafka-configs.sh --bootstrap-server $BS --alter --entity-type users --entity-name shipping \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=shipping-secret]'
kafka-acls.sh --bootstrap-server $BS --add --allow-principal User:shipping \
  --operation Write --operation Describe --topic shipments
mkdir -p /tmp/incident4
cat > /tmp/incident4/shipping.properties <<'PROPS'
# Configuration livree par l'equipe Logistique pour shipping-service
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="shipping" password="shipping-secret";
ssl.truststore.type=PKCS12
ssl.truststore.location=/opt/lab/secrets/truststore.p12
ssl.truststore.password=novashop-lab
client.id=shipping-service
PROPS
cat > /tmp/incident4/send-shipment.sh <<'SCRIPT'
#!/usr/bin/env bash
# Envoi d'un evenement d'expedition, comme le fait shipping-service.
echo "SHIP-$(date +%s)|{\"order_id\":\"O-1001\",\"carrier\":\"colissimo\"}" | \
kafka-console-producer.sh --bootstrap-server kafka-1:19094,kafka-2:19094 \
  --topic shipments --command-config /tmp/incident4/shipping.properties \
  --reader-property parse.key=true --reader-property 'key.separator=|' \
  --command-property max.block.ms=15000 --command-property delivery.timeout.ms=20000 \
  --command-property request.timeout.ms=10000
SCRIPT
chmod +x /tmp/incident4/send-shipment.sh
EOF
  ok "Incident 4 injecte : dans tools, lancer /tmp/incident4/send-shipment.sh"
}
i4_status() { kt bash -c 'cat /tmp/incident4/shipping.properties'; }
i4_repair() {
  kt bash -c "sed -i -e 's/^security.protocol=.*/security.protocol=SASL_SSL/' \
    -e 's/^sasl.mechanism=.*/sasl.mechanism=SCRAM-SHA-512/' /tmp/incident4/shipping.properties"
  kt bash /tmp/incident4/send-shipment.sh && ok "Message envoye : configuration corrigee."
}

# ------------------------------------------------------------------
# 5. advertised.listeners errone sur kafka-3 (faute de frappe kafka3).
# ------------------------------------------------------------------
K3="$LAB_DIR/config/kafka/kafka-3/server.properties"
K3_BACKUP="$LAB_DIR/config/kafka/kafka-3/server.properties.before-incident5"
i5_inject() {
  [[ -f "$K3_BACKUP" ]] && die "Incident 5 deja injecte (sauvegarde presente)."
  cp "$K3" "$K3_BACKUP"
  sed -i.tmp 's#INTERNAL://kafka-3:19092#INTERNAL://kafka3:19092#' "$K3" && rm -f "$K3.tmp"
  dc restart kafka-3 >/dev/null
  wait_healthy kafka-3
  # Attente du retour de kafka-3 dans les ISR, puis election preferee :
  # kafka-3 redevient leader de ses partitions (comme apres le reequilibrage
  # automatique qui a lieu au plus tard 5 min apres son retour).
  sleep 15
  kt bash -c 'kafka-leader-election.sh --bootstrap-server $BS --election-type preferred --all-topic-partitions' >/dev/null 2>&1 || true
  ok "Incident 5 injecte (symptomes visibles apres 30 s)."
}
i5_status() { grep '^advertised.listeners' "$K3"; echo "URP : $(urp)"; }
i5_repair() {
  if [[ -f "$K3_BACKUP" ]]; then mv "$K3_BACKUP" "$K3"; else cp "$LAB_DIR/config/kafka/pristine/kafka-3.server.properties" "$K3"; fi
  dc restart kafka-3 >/dev/null
  wait_healthy kafka-3
  ok "kafka-3 redemarre avec la bonne configuration. URP dans 30 s : verifier avec status."
}

# ------------------------------------------------------------------
# 6. Topic clickstream sans limite de retention + rafale de production.
# ------------------------------------------------------------------
i6_inject() {
  require_running prometheus
  kt bash -s <<'EOF' >/dev/null 2>&1
kafka-topics.sh --bootstrap-server $BS --create --if-not-exists --topic clickstream \
  --partitions 3 --replication-factor 3 \
  --config retention.ms=-1 --config retention.bytes=-1 --config segment.bytes=1048576
# Le topic existe deja si le TP2 a ete fait : on force la meme configuration fautive.
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name clickstream \
  --add-config retention.ms=-1,retention.bytes=-1,segment.bytes=1048576
EOF
  dc exec -d tools bash -c 'kafka-producer-perf-test.sh --bootstrap-server $BS --topic clickstream \
    --num-records 300000 --record-size 1000 --throughput 6000 \
    --command-property acks=1 client.id=clickstream-collector > /tmp/incident6-producer.log 2>&1'
  ok "Incident 6 injecte (production d'environ 300 Mo par broker pendant 50 s)."
}
i6_status() {
  kt bash -c 'kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name clickstream
    kafka-log-dirs.sh --bootstrap-server $BS --describe --topic-list clickstream 2>/dev/null | tail -1 \
      | grep -o "\"size\":[0-9]*" | cut -d: -f2 | awk "{s+=\$1} END {printf \"clickstream : %.0f Mo (toutes repliques)\n\", s/1048576}"'
}
i6_repair() {
  kt bash -c 'kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name clickstream \
    --add-config retention.ms=3600000,retention.bytes=5242880' >/dev/null
  ok "Retention bornee (1 h, 5 Mo par partition). Suppression des segments sous 30 a 60 s."
}

# ------------------------------------------------------------------
[[ $# -ge 1 ]] || usage
if [[ "$1" == "list" ]]; then list; exit 0; fi
[[ $# -eq 2 && "$1" =~ ^[1-6]$ ]] || usage
case "$2" in
  inject|status|repair) "i$1_$2" ;;
  *) usage ;;
esac
