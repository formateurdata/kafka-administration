#!/usr/bin/env bash
# ------------------------------------------------------------------
# Fonctions communes aux scripts du lab. A sourcer :
#     source "$(dirname "$0")/lib.sh"
# ------------------------------------------------------------------

# Racine du lab (dossier contenant docker-compose.yml).
LAB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export LAB_DIR
# Racine de la formation, montee au meme chemin dans le conteneur jupyter
# (voir docker-compose.yml).
FORMATION_DIR="$(cd "$LAB_DIR/.." && pwd)"
export FORMATION_DIR

# Valeurs par defaut, identiques a docker-compose.yml.
# Un fichier .env eventuel est lu par Docker Compose ; les scripts
# utilisent ses valeurs si elles sont exportees dans l'environnement.
KAFKA_IMAGE="${KAFKA_IMAGE:-m0uaad/kafka-lab-kafka:4.3.2-1}"
JMX_EXPORTER_VERSION="${JMX_EXPORTER_VERSION:-1.7.0}"
KAFKA_UI_PORT="${KAFKA_UI_PORT:-8080}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-9090}"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"
SCHEMA_REGISTRY_PORT="${SCHEMA_REGISTRY_PORT:-8081}"
CONNECT_PORT="${CONNECT_PORT:-8083}"
JUPYTER_PORT="${JUPYTER_PORT:-8888}"
BS_INTERNAL="kafka-1:19092,kafka-2:19092,kafka-3:19092"
JUPYTER_IMAGE="${JUPYTER_IMAGE:-m0uaad/kafka-lab-jupyter:1}"

if [[ -t 1 ]]; then
  C_OK=$'\033[32m'; C_KO=$'\033[31m'; C_WARN=$'\033[33m'; C_INFO=$'\033[36m'; C_RST=$'\033[0m'
else
  C_OK=""; C_KO=""; C_WARN=""; C_INFO=""; C_RST=""
fi

info() { printf '%s[INFO]%s %s\n' "$C_INFO" "$C_RST" "$*"; }
ok()   { printf '%s[ OK ]%s %s\n' "$C_OK" "$C_RST" "$*"; }
warn() { printf '%s[WARN]%s %s\n' "$C_WARN" "$C_RST" "$*"; }
fail() { printf '%s[FAIL]%s %s\n' "$C_KO" "$C_RST" "$*"; }
die()  { fail "$*"; exit 1; }

# docker compose execute depuis la racine du lab.
dc() { (cd "$LAB_DIR" && docker compose "$@"); }

# Commande Kafka dans le conteneur tools (sans TTY, pour les scripts).
kt() { dc exec -T tools "$@"; }

# Attend qu'une commande reussisse (timeout en secondes).
wait_for() {
  local timeout="$1"; shift
  local start=$SECONDS
  until "$@" >/dev/null 2>&1; do
    if (( SECONDS - start >= timeout )); then return 1; fi
    sleep 2
  done
}

# Profils Compose correspondant a un mode.
profiles_for_mode() {
  case "$1" in
    core)     echo "" ;;
    extended) echo "--profile monitoring --profile load" ;;
    full)     echo "--profile monitoring --profile load --profile ecosystem" ;;
    *)        return 1 ;;
  esac
}

# Tous les profils (pour stop/reset/status).
ALL_PROFILES="--profile monitoring --profile load --profile ecosystem --profile scale --profile mirror"

# Empreintes des artefacts, GNU/Linux et macOS.
sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# config inclut aussi les dependances : selectionner le service explicitement.
compose_image() {
  dc config "$1" | awk -v service="$1" '
    $0 == "  " service ":" {selected=1; next}
    selected && /^    image:/ {print $2; selected=0}
  '
}
