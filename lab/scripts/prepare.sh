#!/usr/bin/env bash
# Preparation du poste : images preconstruites puis certificats locaux.
# Usage : bash scripts/prepare.sh [--no-pull]
# --no-pull : aucun acces reseau, toutes les images doivent etre chargees.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

NO_PULL=0
case "${1:-}" in
  '') ;;
  --no-pull) NO_PULL=1 ;;
  *) die "Usage : bash scripts/prepare.sh [--no-pull]" ;;
esac
(( $# <= 1 )) || die "Trop d'arguments."

info "1/3 Prerequis"
command -v docker >/dev/null || die "Docker n'est pas installe."
docker info >/dev/null 2>&1 || die "Le daemon Docker ne repond pas."
docker compose version >/dev/null || die "Docker Compose v2 est requis."
mem_bytes="$(docker info --format '{{.MemTotal}}')"
mem_gb=$(( mem_bytes / 1024 / 1024 / 1024 ))
if (( mem_gb < 6 )); then
  warn "Docker : ${mem_gb} Go. Prevoir 6 Go pour extended, 8 Go pour full."
else
  ok "Docker : ${mem_gb} Go alloues"
fi

info "2/3 Images Docker preconstruites"
if (( ! NO_PULL )); then
  dc $ALL_PROFILES pull --quiet || die "Images indisponibles. Charger le lot hors ligne ou consulter images/README.md."
fi
image_list="$(dc $ALL_PROFILES config --images | sort -u)"
while IFS= read -r img; do
  docker image inspect "$img" >/dev/null 2>&1 || die "Image absente : $img. Charger le lot hors ligne ou lancer prepare.sh avec Internet."
done <<<"$image_list"
# Lire les references resolues par Compose, y compris un eventuel .env.
kafka_image="$(compose_image tools)"
jupyter_image="$(compose_image jupyter)"
docker run --rm --pull never --network none --entrypoint sh "$kafka_image" -c \
  "test -f /opt/lab/common/start-node.sh && test -f /opt/lab/loadgen/producer.sh && echo '0ad843261c567d1924a017d542e14c40f2afff523d940c25ee000742fde4b3a9  /opt/jmx-exporter/jmx_prometheus_javaagent-1.7.0.jar' | sha256sum -c -" \
  || die "Image Kafka incompatible avec le lab."
docker run --rm --pull never --network none --entrypoint python "$jupyter_image" \
  -c 'import jupyterlab, confluent_kafka, requests' || die "Image JupyterLab incomplete."
ok "Images presentes, aucune construction sur ce poste"

info "3/3 Certificats TLS du lab"
# generate-certs.sh garde la CA existante et ne cree que les certificats
# manquants (kafka-cloud sur un lab prepare avant le TP9, par exemple).
docker run --rm --pull never --network none --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$LAB_DIR/security:/work" --entrypoint bash "$kafka_image" \
  /work/generate-certs.sh >/dev/null
ok "Certificats presents sur ce poste dans security/certs/"
ok "Poste pret. Etape suivante : bash scripts/start.sh extended"
