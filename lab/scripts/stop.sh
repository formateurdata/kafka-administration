#!/usr/bin/env bash
# ------------------------------------------------------------------
# Arret du lab SANS perte de donnees.
#
# Usage : scripts/stop.sh          arrete les conteneurs (redemarrage rapide)
#         scripts/stop.sh --down   supprime aussi les conteneurs et le reseau
#                                  (les volumes de donnees sont conserves)
#
# Les noeuds Kafka disposent de 60 s pour un arret controle
# (controlled shutdown : transfert des leaders avant l'arret).
# ------------------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/lib.sh"

if [[ "${1:-}" == "--down" ]]; then
  info "Suppression des conteneurs (volumes conserves)"
  # shellcheck disable=SC2086
  dc $ALL_PROFILES down --remove-orphans
else
  info "Arret des conteneurs"
  # shellcheck disable=SC2086
  dc $ALL_PROFILES stop
fi
ok "Lab arrete. Redemarrage : scripts/start.sh <mode>"
