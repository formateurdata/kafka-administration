#!/usr/bin/env bash
# Construction reservee au formateur. Aucun build dans prepare.sh/start.sh.
# Usage : bash scripts/build-images.sh [--push] [--platform linux/amd64,linux/arm64]
# References : KAFKA_IMAGE et JUPYTER_IMAGE (ou valeurs de Compose).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

push=0
platform=""
while (( $# )); do
  case "$1" in
    --push) push=1; shift ;;
    --platform) platform="${2:-}"; [[ -n "$platform" ]] || die "Plateforme manquante."; shift 2 ;;
    *) die "Usage : bash scripts/build-images.sh [--push] [--platform linux/amd64,linux/arm64]" ;;
  esac
done
[[ -n "$platform" ]] || platform="linux/$(docker version --format '{{.Server.Arch}}')"
case "$platform" in
  linux/amd64|linux/arm64|linux/amd64,linux/arm64|linux/arm64,linux/amd64) ;;
  *) die "Plateformes prises en charge : linux/amd64 et linux/arm64." ;;
esac
kafka_image="$(compose_image tools)"
jupyter_image="$(compose_image jupyter)"
output=(--load)
if (( push )); then
  [[ "$kafka_image" != kafka-lab/* && "$jupyter_image" != kafka-lab/* ]] \
    || die "Exporter KAFKA_IMAGE et JUPYTER_IMAGE avec les references du registre avant --push."
  output=(--push)
fi

jar="$LAB_DIR/monitoring/jmx-exporter/jmx_prometheus_javaagent-1.7.0.jar"
expected=0ad843261c567d1924a017d542e14c40f2afff523d940c25ee000742fde4b3a9
if [[ ! -f "$jar" ]]; then
  trap 'rm -f "$jar.part"' EXIT
  curl -fsSL -o "$jar.part" https://github.com/prometheus/jmx_exporter/releases/download/1.7.0/jmx_prometheus_javaagent-1.7.0.jar
  [[ "$(sha256_of "$jar.part")" == "$expected" ]] || die "Empreinte JMX inattendue."
  mv "$jar.part" "$jar"
fi
[[ "$(sha256_of "$jar")" == "$expected" ]] || die "Empreinte JMX inattendue."

# JupyterLab est construit/publie en premier.
info "JupyterLab : $jupyter_image ($platform)"
docker buildx build --platform "$platform" "${output[@]}" --tag "$jupyter_image" "$LAB_DIR/jupyter"
info "Kafka et agent JMX : $kafka_image ($platform)"
docker buildx build --platform "$platform" "${output[@]}" --tag "$kafka_image" -f "$LAB_DIR/images/Dockerfile" "$LAB_DIR"
if (( push )); then
  docker buildx imagetools inspect "$jupyter_image"
  docker buildx imagetools inspect "$kafka_image"
fi
ok "Images construites. Verification : prepare.sh --no-pull, puis cycle de tests de images/README.md."
