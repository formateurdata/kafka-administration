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

# Ports publies par les services du mode, valeurs de .env comprises :
# une ligne "service port" par port.
published_ports() {
  # shellcheck disable=SC2086
  dc $PROFILES config | awk '
    /^[^ ]/                         { in_services = ($0 == "services:") }
    in_services && /^  [^ ]+:$/     { service = $1; sub(/:$/, "", service) }
    in_services && /^ +published:/  { port = $2; gsub(/"/, "", port); print service, port }
  '
}

# Variable de .env qui fixe le port d'un service (vide : port fixe).
port_variable() {
  case "$1" in
    kafka-ui)        echo KAFKA_UI_PORT ;;
    jupyter)         echo JUPYTER_PORT ;;
    prometheus)      echo PROMETHEUS_PORT ;;
    grafana)         echo GRAFANA_PORT ;;
    schema-registry) echo SCHEMA_REGISTRY_PORT ;;
    connect)         echo CONNECT_PORT ;;
    *)               echo "" ;;
  esac
}

# Refuse de demarrer si un port du lab est pris par un conteneur d'un autre
# projet ou par un processus de l'hote. Les conteneurs du lab deja demarres
# sont ignores (start.sh peut etre relance). Sans cette verification, Compose
# cree le conteneur et echoue a le demarrer ; un nouvel essai, port libere,
# peut alors le demarrer sans reseau (observe avec Docker Desktop 29.9.3 :
# kafka-ui sans adresse IP ni port publie).
check_ports() {
  local service port holder lab_owned name project var conflicts=0
  while read -r service port; do
    [[ -n "$port" ]] || continue
    holder=""; lab_owned=0
    while IFS='|' read -r name project; do
      [[ -n "$name" ]] || continue
      if [[ "$project" == "kafka-lab" ]]; then lab_owned=1
      else holder="le conteneur $name${project:+ (projet Compose $project)}"; fi
    done < <(docker ps --filter "publish=$port" --format '{{.Names}}|{{.Label "com.docker.compose.project"}}')
    if [[ -z "$holder" && $lab_owned -eq 0 ]] && (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      holder="un processus de l'hote"
      if command -v lsof >/dev/null 2>&1; then
        name="$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | awk 'NR == 2 {print $1 " (pid " $2 ")"}')"
        [[ -z "$name" ]] || holder="le processus $name"
      fi
    fi
    [[ -n "$holder" ]] || continue
    conflicts=$((conflicts + 1))
    var="$(port_variable "$service")"
    if [[ -n "$var" ]]; then
      fail "Port $port ($service) deja utilise par $holder. Liberer le port ou changer $var dans .env (modele : env.template)."
    else
      fail "Port $port ($service) deja utilise par $holder. Port fixe (advertised.listeners) : le liberer."
    fi
  done < <(published_ports)
  (( conflicts == 0 )) || die "$conflicts port(s) deja utilise(s) : lab non demarre."
}
check_ports

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
