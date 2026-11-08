#!/usr/bin/env bash
# ------------------------------------------------------------------
# Demarrage du lab.
#
# Usage : scripts/start.sh [core|extended|full]
#   core      kafka-1..3, kafka-ui, tools, jupyter             (1,5 Go RAM mesures sans jupyter)
#   extended  core + prometheus, grafana, kafka-exporter,
#             loadgen-producer, loadgen-consumer               (4,1 Go RAM mesures)
#   full      extended + schema-registry, connect              (4,9 Go RAM mesures)
#   Mesures : macOS arm64, Docker Desktop, apres 1 min de fonctionnement
#   (voir TESTS.md). Memoire a allouer a Docker : 4, 6 et 8 Go.
#
# Le broker kafka-4 (TP7) se demarre a part :
#   docker compose --profile scale up -d kafka-4
# Le cluster cloud et MirrorMaker 2 (TP9) aussi, dans cet ordre :
#   docker compose --profile mirror up -d kafka-cloud
#   docker compose --profile mirror up -d mirrormaker
# ------------------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/lib.sh"

MODE="${1:-core}"
PROFILES="$(profiles_for_mode "$MODE")" || die "Mode inconnu : $MODE (core, extended ou full)"

# Prerequis produits par prepare.sh
[[ -f "$LAB_DIR/security/certs/truststore.p12" ]] || die "Certificats absents : lancer d'abord scripts/prepare.sh"

info "Demarrage du lab en mode $MODE"
# --wait : rend la main quand les services sont demarres et, s'ils ont un
# healthcheck, declares sains (timeout 4 min).
# shellcheck disable=SC2086
dc $PROFILES up -d --no-build --pull never --wait --wait-timeout 240 || {
  fail "Certains services ne sont pas sains. Diagnostic : docker compose ps ; docker compose logs <service>"
  exit 1
}

echo
ok "Lab demarre (mode $MODE)"
echo "  Kafka UI       http://localhost:${KAFKA_UI_PORT}"
echo "  JupyterLab     http://localhost:${JUPYTER_PORT}${JUPYTER_TOKEN:+/lab?token=$JUPYTER_TOKEN}   (notebooks des TP et demos)"
if [[ "$MODE" != "core" ]]; then
  echo "  Prometheus     http://localhost:${PROMETHEUS_PORT}"
  echo "  Grafana        http://localhost:${GRAFANA_PORT}   (admin / kafka-lab, ou consultation anonyme)"
fi
if [[ "$MODE" == "full" ]]; then
  echo "  Schema Registry http://localhost:${SCHEMA_REGISTRY_PORT}"
  echo "  Kafka Connect   http://localhost:${CONNECT_PORT}"
fi
echo "  Shell d'administration : docker compose exec tools bash"
echo
echo "Verification : scripts/validate.sh"
